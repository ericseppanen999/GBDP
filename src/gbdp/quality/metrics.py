from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from gbdp.utils.io import storage_format, gold_root

from gbdp.utils.io import data_root, ensure_dir


def write_run_audit(run_id: str, dt: str, status: str, force: bool = False) -> Path:
    root = gold_root()
    metrics = _collect_row_counts(root, dt)
    total_rows = sum(metrics.values()) if metrics else 0
    row = {
        "run_id": run_id,
        "dt": dt,
        "start_utc": datetime.now(timezone.utc).isoformat(),
        "end_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "row_counts_by_table": metrics,
        "total_rows": total_rows,
        "anomalies": None,
    }
    out_dir = root / "gold" / "audit_pipeline_runs" / f"dt={dt}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if out_path.exists() and not force:
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
    if not gold_dir.exists():
        return counts
    for table_dir in gold_dir.iterdir():
        if not table_dir.is_dir():
            continue
        part = table_dir / f"dt={dt}"
        if not part.exists():
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
    return spark.read.format("delta").load(str(path)).count()


def _write_parquet(rows: List[Dict], path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not rows:
        rows = [{"empty": True}]
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, use_dictionary=False)
