from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq

from gbdp.utils.io import ensure_dir, storage_format


def write_parquet(rows: Iterable[Dict[str, Any]], path: Path, force: bool = False) -> Path:
    ensure_dir(path.parent)
    if path.exists() and not force:
        return path
    data: List[Dict[str, Any]] = list(rows)
    if not data:
        data = [{"empty": True}]
    fmt = storage_format()
    if fmt == "parquet":
        table = pa.Table.from_pylist(data)
        pq.write_table(table, path, use_dictionary=False)
    elif fmt == "delta":
        _write_delta(data, path.parent)
    else:
        raise ValueError(f"Unsupported storage format: {fmt}")
    return path


def _write_delta(rows: List[Dict[str, Any]], out_dir: Path) -> None:
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta writes") from exc
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("overwrite").save(str(out_dir))
