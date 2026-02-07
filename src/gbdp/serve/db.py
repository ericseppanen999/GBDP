from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import duckdb

from gbdp.utils.io import gold_root, storage_format


def query(table: str, dt: str | None = None, where: str | None = None, limit: int = 1000) -> List[Dict[str, Any]]:
    root = gold_root()
    path = root / table
    if dt:
        path = path / f"dt={dt}"
    if not path.exists():
        return []
    if storage_format() == "parquet":
        con = duckdb.connect()
        con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{path}/*.parquet')")
        sql = "SELECT * FROM t"
        if where:
            sql += f" WHERE {where}"
        sql += f" LIMIT {int(limit)}"
        return [dict(zip([c[0] for c in con.description], row)) for row in con.execute(sql).fetchall()]
    # delta
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta reads") from exc
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("delta").load(str(path))
    if where:
        df = df.where(where)
    rows = df.limit(int(limit)).collect()
    return [row.asDict() for row in rows]
