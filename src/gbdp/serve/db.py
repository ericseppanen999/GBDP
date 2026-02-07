from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import time

import duckdb

from gbdp.utils.io import gold_root, storage_format, path_exists, spark_path


_CACHE: Dict[Tuple[str, str | None, str | None, int], Tuple[float, List[Dict[str, Any]]]] = {}
_CACHE_TTL_S = 24 * 60 * 60


def query(table: str, dt: str | None = None, where: str | None = None, limit: int = 1000) -> List[Dict[str, Any]]:
    key = (table, dt, where, int(limit))
    now = time.time()
    cached = _CACHE.get(key)
    if cached and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]
    root = gold_root()
    path = root / table
    if dt:
        path = path / f"dt={dt}"
    if not path_exists(path):
        return []
    if storage_format() == "parquet":
        con = duckdb.connect()
        con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{path}/*.parquet')")
        sql = "SELECT * FROM t"
        if where:
            sql += f" WHERE {where}"
        sql += f" LIMIT {int(limit)}"
        rows = [dict(zip([c[0] for c in con.description], row)) for row in con.execute(sql).fetchall()]
        _CACHE[key] = (now, rows)
        return rows
    # delta
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta reads") from exc
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("delta").load(spark_path(path))
    if where:
        df = df.where(where)
    rows = df.limit(int(limit)).collect()
    out = [row.asDict() for row in rows]
    _CACHE[key] = (now, out)
    return out
