from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from gbdp.utils.io import (
    ensure_dir,
    gold_root,
    list_dir,
    path_exists,
    spark_path,
    storage_format,
    write_parquet_table,
)


def write_run_audit(run_id: str, dt: str, status: str, force: bool = False) -> Path:
    root = gold_root()
    metrics = _collect_row_counts(root, dt)
    total_rows = sum(metrics.values()) if metrics else 0
    metrics_out = metrics if metrics else None
    row = {
        "run_id": run_id,
        "dt": dt,
        "start_utc": datetime.now(timezone.utc).isoformat(),
        "end_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "row_counts_by_table": metrics_out,
        "total_rows": total_rows,
        "anomalies": None,
    }
    out_dir = root / "audit_pipeline_runs" / f"dt={dt}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if path_exists(out_path) and not force:
        return out_path
    _write_parquet([row], out_path)
    return out_path


def _collect_row_counts(root: Path, dt: str) -> Dict[str, int]:
    fmt = storage_format()
    con = None
    if fmt == "parquet":
        try:
            import duckdb
        except Exception as exc:
            raise RuntimeError("duckdb is required for parquet metrics") from exc
        con = duckdb.connect()
    counts: Dict[str, int] = {}
    gold_dir = root
    if not path_exists(gold_dir):
        return counts
    for table_dir in list_dir(gold_dir, dirs_only=True):
        part = table_dir / f"dt={dt}"
        if not path_exists(part):
            continue
        if fmt == "parquet":
            con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{part}/*.parquet')")
            counts[table_dir.name] = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        else:
            counts[table_dir.name] = _spark_count(part)
    return counts


def _spark_count(path: Path) -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # path is a dt= partition folder — read from the table root and filter
    if path.name.startswith("dt="):
        table_root = path.parent
        dt_value = path.name.split("=", 1)[1]
        try:
            return (
                spark.read.format("delta")
                .load(spark_path(table_root))
                .where(f"dt = '{dt_value}'")
                .count()
            )
        except Exception:
            pass

    # Fallback: partition folder is itself a delta table (legacy layout)
    try:
        return spark.read.format("delta").load(spark_path(path)).count()
    except Exception:
        pass

    try:
        return spark.read.format("parquet").load(spark_path(path)).count()
    except Exception:
        return 0


def _write_parquet(rows: List[Dict], path: Path) -> None:
    if not rows:
        rows = [{"empty": True}]
    import pyarrow as pa
    table = pa.Table.from_pylist(rows)
    write_parquet_table(table, path, force=True)
