from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq

from gbdp.utils.io import ensure_dir, storage_format, spark_path, path_exists


def write_parquet(rows: Iterable[Dict[str, Any]], path: Path, force: bool = False) -> Path:
    data: List[Dict[str, Any]] = list(rows)
    if not data:
        data = [{"empty": True}]

    fmt = storage_format()

    if fmt == "parquet":
        ensure_dir(path.parent)
        if path_exists(path) and not force:
            return path
        table = pa.Table.from_pylist(data)
        pq.write_table(table, path, use_dictionary=False)
        return path

    if fmt == "delta":
        # We write a delta table to a DIRECTORY, not a single file path.
        out_dir = path if path.suffix == "" else path.parent
        ensure_dir(out_dir)

        delta_log = out_dir / "_delta_log"
        if path_exists(delta_log) and not force:
            return out_dir

        _write_delta(data, out_dir)
        return out_dir

    raise ValueError(f"Unsupported storage format: {fmt}")


def _write_delta(rows: List[Dict[str, Any]], out_dir: Path) -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import (
            StructType, StructField,
            StringType, LongType, DoubleType, BooleanType, BinaryType
        )
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta writes") from exc

    # Normalize values so schema inference is stable (dict/list -> JSON, others unchanged)
    norm_rows: List[Dict[str, Any]] = []
    for r in rows:
        nr: Dict[str, Any] = {}
        for k, v in r.items():
            if isinstance(v, (dict, list)):
                nr[k] = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
            else:
                nr[k] = v
        norm_rows.append(nr)

    # Infer schema: if a column has no non-null values, default to STRING (avoids NullType)
    keys = sorted({k for r in norm_rows for k in r.keys()})

    def infer_type(values: List[Any]):
        for v in values:
            if v is None:
                continue
            if isinstance(v, bool):
                return BooleanType()
            if isinstance(v, int) and not isinstance(v, bool):
                return LongType()
            if isinstance(v, float):
                return DoubleType()
            if isinstance(v, (bytes, bytearray)):
                return BinaryType()
            return StringType()
        return StringType()

    fields = []
    for k in keys:
        col_vals = [r.get(k) for r in norm_rows]
        fields.append(StructField(k, infer_type(col_vals), nullable=True))

    schema = StructType(fields)

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(norm_rows, schema=schema)

    df.write.format("delta").mode("overwrite").save(spark_path(out_dir))