from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import time

import duckdb
import yaml

from gbdp.utils.io import gold_root, list_dir, storage_format, path_exists, spark_path

# (column, "eq" or "contains", value)
Filter = Tuple[str, str, Any]


_CACHE: Dict[tuple, Tuple[float, List[Dict[str, Any]]]] = {}
_CACHE_TTL_S = 24 * 60 * 60

_SEMANTIC_LAYER_PATH = Path("configs/semantic_layer.yaml")
_semantic_layer_cache: Dict[str, Any] | None = None


def latest_dt(table: str) -> str | None:
    base = gold_root() / table
    if not path_exists(base):
        return None
    dts = [p.name.split("=", 1)[1] for p in list_dir(base, dirs_only=True) if p.name.startswith("dt=")]
    return max(dts) if dts else None


def query(
    table: str,
    dt: str | None = None,
    filters: Sequence[Filter] = (),
    limit: int = 1000,
    all_dts: bool = False,
) -> List[Dict[str, Any]]:
    """
    Read one gold table. Without dt this reads the latest snapshot, or every
    snapshot with all_dts. Filter values are bound, never pasted into SQL.
    """
    root = gold_root()
    dt = dt or (None if all_dts else latest_dt(table))
    if not path_exists(root / table) or not (dt or all_dts):
        return []
    key = (str(root), table, dt, tuple(filters), int(limit))
    now = time.time()
    cached = _CACHE.get(key)
    if cached and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]
    if storage_format() == "parquet":
        rows = _query_parquet(root / table, dt, filters, limit)
    else:
        rows = _query_delta(root / table, dt, filters, limit)
    _CACHE[key] = (now, rows)
    return rows


def _query_parquet(base: Path, dt: str | None, filters: Sequence[Filter], limit: int) -> List[Dict[str, Any]]:
    if dt and not path_exists(base / f"dt={dt}"):
        return []
    glob = str(base / f"dt={dt or '*'}" / "*.parquet")
    conditions = []
    params: List[Any] = [glob]
    for column, op, value in filters:
        if op == "contains":
            conditions.append(f'contains(lower("{column}"), ?)')
            params.append(str(value).lower())
        else:
            conditions.append(f'"{column}" = ?')
            params.append(value)
    sql = "SELECT * FROM read_parquet(?, union_by_name=true, hive_partitioning=false)"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += f" LIMIT {int(limit)}"
    cur = duckdb.connect().execute(sql, params)
    columns = [c[0] for c in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _query_delta(base: Path, dt: str | None, filters: Sequence[Filter], limit: int) -> List[Dict[str, Any]]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta reads") from exc
    df = SparkSession.builder.getOrCreate().read.format("delta").load(spark_path(base))
    if dt:
        df = df.where(F.col("dt") == dt)
    for column, op, value in filters:
        if op == "contains":
            df = df.where(F.lower(F.col(column)).contains(str(value).lower()))
        else:
            df = df.where(F.col(column) == value)
    return [row.asDict() for row in df.limit(int(limit)).collect()]


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
# my data" endpoint. replace(...) the string function is fine; only the keyword
# is blocked.
_WRITE_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|REPLACE(?!\s*\()|GRANT|REVOKE|"
    r"COPY|VACUUM|OPTIMIZE|CALL|EXEC|EXECUTE|SET|USE|ATTACH|DETACH|PRAGMA)\b",
    re.IGNORECASE,
)
# Reading files directly, e.g. read_text('secrets.env') or parquet.`/some/path`.
# Queries only ever need the registered tables. DuckDB enforces this for real
# in run_sql; this also covers the Spark path.
_FILE_ACCESS = re.compile(r"\b(read_\w+|glob|sniff_csv|parquet_\w+)\s*\(|`[^`]*[/\\][^`]*`", re.IGNORECASE)
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
    if _FILE_ACCESS.search(stripped):
        raise UnsafeSqlError("query can only read the registered gold tables, not files")


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
        gold_dir = os.path.abspath(str(root))
        for table in known_table_names():
            base = root / table
            if not path_exists(base):
                continue
            glob = os.path.join(gold_dir, table, "**", "*.parquet")
            try:
                con.execute(
                    f"CREATE OR REPLACE VIEW \"{table}\" AS "
                    f"SELECT * FROM read_parquet('{glob}', union_by_name=true)"
                )
            except Exception:
                continue
        # Confine the connection to gold, so a query that slips past the text
        # guard still can't read the rest of the machine.
        con.execute("SET allowed_directories = [?]", [gold_dir])
        con.execute("SET enable_external_access = false")
        con.execute("SET lock_configuration = true")
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
