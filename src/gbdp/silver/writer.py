from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from gbdp.utils.io import ensure_dir, storage_format, spark_path, path_exists, has_files_with_suffix


def write_parquet(rows: Iterable[Dict[str, Any]], path: Path, force: bool = False) -> Path:
    """
    Parquet mode: writes to the file `path`.
    Delta mode: ignores the filename and writes a UC-friendly delta table at the dataset root,
               partitioned by dt (inferred from parent folder dt=YYYY-MM-DD if present).
    """
    ensure_dir(path.parent)

    data: List[Dict[str, Any]] = list(rows)
    if not data:
        data = [{"empty": True}]

    fmt = storage_format()
    if fmt == "parquet":
        if path_exists(path) and not force:
            return path
        table = pa.Table.from_pylist(data)
        pq.write_table(table, path, use_dictionary=False)
        return path

    if fmt == "delta":
        _write_delta(data, path.parent, force=force)
        return path

    raise ValueError(f"Unsupported storage format: {fmt}")


def _infer_type(values: List[Any]) -> str:
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            return "boolean"
        if isinstance(v, int) and not isinstance(v, bool):
            return "long"
        if isinstance(v, float):
            return "double"
        if isinstance(v, (bytes, bytearray)):
            return "binary"
        return "string"
    return "string"


def _rows_to_df(spark, rows: List[Dict[str, Any]]):
    """
    Deterministic DF creation:
      - dict/list -> JSON string (prevents STRUCT/MAP surprises like status)
      - all-null cols -> STRING (prevents CANNOT_DETERMINE_TYPE)
    """
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        LongType,
        DoubleType,
        BooleanType,
        BinaryType,
    )

    def dtype(name: str):
        return {
            "string": StringType(),
            "long": LongType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "binary": BinaryType(),
        }.get(name, StringType())

    if not rows:
        schema = StructType([StructField("empty", BooleanType(), True)])
        return spark.createDataFrame([], schema=schema)

    norm: List[Dict[str, Any]] = []
    for r in rows:
        nr: Dict[str, Any] = {}
        for k, v in r.items():
            if isinstance(v, (dict, list)):
                nr[k] = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
            else:
                nr[k] = v
        norm.append(nr)

    keys = sorted({k for r in norm for k in r.keys()})
    fields = []
    for k in keys:
        vals = [r.get(k) for r in norm]
        fields.append(StructField(k, dtype(_infer_type(vals)), True))

    schema = StructType(fields)
    return spark.createDataFrame(norm, schema=schema)


def _delta_exists(spark, table_root: str) -> bool:
    try:
        spark.read.format("delta").load(table_root).limit(1).collect()
        return True
    except Exception:
        return False


def _add_missing_columns(spark, table_root: str, df) -> None:
    existing = {f.name for f in spark.read.format("delta").load(table_root).schema.fields}
    new_fields = [f for f in df.schema.fields if f.name not in existing]
    if not new_fields:
        return
    ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in new_fields)
    spark.sql(f"ALTER TABLE delta.`{table_root}` ADD COLUMNS ({ddl})")


def _table_root_and_dt(out_dir: Path) -> tuple[Path, Optional[str]]:
    """
    If out_dir is .../<dataset>/dt=YYYY-MM-DD -> treat <dataset> as table root and dt as partition key.
    Otherwise treat out_dir itself as table root (no dt partition overwrite).
    """
    name = out_dir.name
    if name.startswith("dt="):
        return out_dir.parent, name.split("=", 1)[1]
    return out_dir, None


def _write_delta(rows: List[Dict[str, Any]], out_dir: Path, force: bool = False) -> None:
    """
    UC-friendly delta write:
      - one delta table per dataset at table_root/
      - partitioned by dt
      - overwrite only dt partition (replaceWhere) when dt=... folder is used
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    table_root_dir, dt_value = _table_root_and_dt(out_dir)
    ensure_dir(table_root_dir)

    if not rows:
        return

    # If a parquet dataset already exists (no delta log), keep using parquet.
    delta_log = table_root_dir / "_delta_log"
    if not path_exists(delta_log) and has_files_with_suffix(table_root_dir, ".parquet"):
        df = _rows_to_df(spark, rows)
        df.write.mode("overwrite").parquet(spark_path(out_dir))
        return

    # Enforce dt partition consistency when writing a dt folder
    if dt_value is not None:
        for r in rows:
            r["dt"] = dt_value
    else:
        # Ensure dt exists to keep partitioning consistent
        for r in rows:
            r.setdefault("dt", "")

    df = _rows_to_df(spark, rows)
    table_root = spark_path(table_root_dir)

    # First write creates table schema
    if not _delta_exists(spark, table_root):
        try:
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("dt")
                .save(table_root)
            )
            return
        except Exception as exc:
            msg = str(exc)
            if "DELTA_MISSING_TRANSACTION_LOG" in msg or "Incompatible format detected" in msg:
                df.write.mode("overwrite").parquet(spark_path(out_dir))
                return
            raise

    # Partition overwrite
    w = (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
    )
    if dt_value is not None:
        w = w.option("replaceWhere", f"dt = '{dt_value}'")

    try:
        w.save(table_root)
    except Exception as exc:
        msg = str(exc)
        if "DELTA_MISSING_TRANSACTION_LOG" in msg or "Incompatible format detected" in msg:
            df.write.mode("overwrite").parquet(spark_path(out_dir))
            return
        if "DELTA_FAILED_TO_MERGE_FIELDS" in msg or "delta_failed_to_merge_fields" in msg.lower():
            # Widen the table's schema additively (ADD COLUMNS never touches existing
            # rows or other partitions), then retry the same partition-scoped write.
            # Do NOT fall back to mode("overwrite") without replaceWhere here -- that
            # replaces the ENTIRE table with just this partition's rows, silently
            # destroying every other date ever written to it.
            _add_missing_columns(spark, table_root, df)
            w.save(table_root)
            return
        raise RuntimeError(
            f"Delta write failed (likely schema/type conflict). "
            f"If you recently changed how a field is represented (e.g., status struct -> string), "
            f"delete the existing delta table at {table_root} and rerun. Original error: {exc}"
        ) from exc
