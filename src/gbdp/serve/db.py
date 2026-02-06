from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import duckdb

from gbdp.utils.io import data_root


def query(table: str, dt: str | None = None, where: str | None = None, limit: int = 1000) -> List[Dict[str, Any]]:
    root = data_root()
    path = root / "gold" / table
    if dt:
        path = path / f"dt={dt}"
    if not path.exists():
        return []
    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{path}/*.parquet')")
    sql = "SELECT * FROM t"
    if where:
        sql += f" WHERE {where}"
    sql += f" LIMIT {int(limit)}"
    return [dict(zip([c[0] for c in con.description], row)) for row in con.execute(sql).fetchall()]
