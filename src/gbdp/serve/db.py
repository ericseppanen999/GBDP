from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import time

import duckdb
import yaml

from gbdp.utils.io import gold_root, storage_format, path_exists, spark_path


_CACHE: Dict[Tuple[str, str | None, str | None, int], Tuple[float, List[Dict[str, Any]]]] = {}
_CACHE_TTL_S = 24 * 60 * 60

_SEMANTIC_LAYER_PATH = Path("configs/semantic_layer.yaml")
_semantic_layer_cache: Dict[str, Any] | None = None


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


def load_semantic_layer() -> Dict[str, Any]:
    """Load configs/semantic_layer.yaml (cached in-process)."""
    global _semantic_layer_cache
    if _semantic_layer_cache is not None:
        return _semantic_layer_cache
    if not _SEMANTIC_LAYER_PATH.exists():
        _semantic_layer_cache = {"tables": {}, "audit_tables": {}, "derived_metrics": {}}
        return _semantic_layer_cache
    with _SEMANTIC_LAYER_PATH.open("r", encoding="utf-8") as f:
        _semantic_layer_cache = yaml.safe_load(f) or {}
    return _semantic_layer_cache


def known_table_names() -> List[str]:
    layer = load_semantic_layer()
    names = list(layer.get("tables", {}).keys()) + list(layer.get("audit_tables", {}).keys())
    return names


# Whole-word match against SQL keywords that mutate data/schema or otherwise
# escape a plain read. This is a pragmatic guard for a trusted client (an
# LLM or a known caller), not a hardened defense against a malicious one --
# no string-based check can fully replace a real SQL parser. It exists so an
# accidental or hallucinated DROP/DELETE can't slip through a "just query
# my data" endpoint.
_WRITE_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|REPLACE|GRANT|REVOKE|"
    r"COPY|VACUUM|OPTIMIZE|CALL|EXEC|EXECUTE|SET|USE|ATTACH|DETACH|PRAGMA)\b",
    re.IGNORECASE,
)
_LEADING_KEYWORD = re.compile(r"^\s*(--.*\n|\s)*\b(SELECT|WITH)\b", re.IGNORECASE)


class UnsafeSqlError(ValueError):
    pass


def assert_read_only_sql(sql: str) -> None:
    stripped = sql.strip().rstrip(";")
    if ";" in stripped:
        raise UnsafeSqlError("multiple statements are not allowed")
    if not _LEADING_KEYWORD.match(stripped):
        raise UnsafeSqlError("query must start with SELECT or WITH")
    if _WRITE_KEYWORDS.search(stripped):
        raise UnsafeSqlError("query must be read-only (no INSERT/UPDATE/DELETE/DDL/etc.)")


def run_sql(sql: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """Execute a read-only SQL query against the gold layer.

    Every known gold/audit table (per configs/semantic_layer.yaml) that
    currently has data on disk is registered under its bare name (e.g.
    `fact_game`, not a file path), identically in both parquet and delta
    mode, so the same query text works regardless of backend.
    """
    assert_read_only_sql(sql)
    stripped = sql.strip().rstrip(";")
    limited = f"SELECT * FROM ({stripped}) AS _gbdp_query_result LIMIT {int(limit)}"

    root = gold_root()
    fmt = storage_format()

    if fmt == "parquet":
        con = duckdb.connect()
        for table in known_table_names():
            base = root / table
            if not path_exists(base):
                continue
            glob = str(base / "**" / "*.parquet")
            try:
                con.execute(
                    f"CREATE OR REPLACE VIEW \"{table}\" AS "
                    f"SELECT * FROM read_parquet('{glob}', union_by_name=true)"
                )
            except Exception:
                continue
        cur = con.execute(limited)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # delta
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta reads") from exc
    spark = SparkSession.builder.getOrCreate()
    for table in known_table_names():
        base = root / table
        if not path_exists(base):
            continue
        try:
            spark.read.format("delta").load(spark_path(base)).createOrReplaceTempView(table)
        except Exception:
            continue
    rows = spark.sql(limited).collect()
    return [row.asDict() for row in rows]
