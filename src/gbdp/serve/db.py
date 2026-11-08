from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Sequence, Tuple

import time

import duckdb

from gbdp.utils.io import gold_root, list_dir, storage_format, path_exists, spark_path

# (column, "eq" or "contains", value)
Filter = Tuple[str, str, Any]


_CACHE: Dict[tuple, Tuple[float, List[Dict[str, Any]]]] = {}
_CACHE_TTL_S = 24 * 60 * 60



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


def semantic_layer():
    """The validated semantic layer (gbdp.semantic.layer.SemanticLayer)."""
    from gbdp.semantic import layer

    return layer.load()


def load_semantic_layer() -> Dict[str, Any]:
    """configs/semantic_layer.yaml as plain data, validated (cached until it changes)."""
    return semantic_layer().raw


def known_table_names() -> List[str]:
    sl = semantic_layer()
    return [t for t in sl.tables if not sl.tables[t].placeholder] + list(sl.audit_tables)


def known_view_names() -> List[str]:
    return list(semantic_layer().views)


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

    if storage_format() == "parquet":
        cur = _duckdb_connection().execute(limited)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    rows = _spark_session_with_views().sql(limited).collect()
    return [row.asDict() for row in rows]


def _gold_tables() -> List[str]:
    """Every described gold/audit table that has data on disk."""
    root = gold_root()
    return [t for t in known_table_names() if path_exists(root / t)]


def _duckdb_connection():
    """A DuckDB connection with every gold table and semantic-layer view
    registered by bare name, then confined to the gold directory.

    A view whose tables aren't on disk yet (no Statcast data loaded, say) is
    skipped; a query that needs it fails with DuckDB's "does not exist".
    """
    from gbdp.semantic.render import duckdb_target, render

    root = gold_root()
    con = duckdb.connect()
    gold_dir = os.path.abspath(str(root))
    for table in _gold_tables():
        glob = os.path.join(gold_dir, table, "**", "*.parquet")
        # hive_types_autocast=false keeps dt a string, as it is in the files and
        # in Unity Catalog. Autocast makes it a DATE, which DuckDB 1.5 then
        # mis-binds (an internal error) in YEAR(CAST(dt AS DATE)) and windows.
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW \"{table}\" AS "
                f"SELECT * FROM read_parquet('{glob}', union_by_name=true, hive_types_autocast=false)"
            )
        except Exception:
            continue
    sl = semantic_layer()
    target = duckdb_target()
    for name, view in sl.views.items():
        try:
            con.execute(f"CREATE OR REPLACE VIEW \"{name}\" AS {render(view.sql, sl, target)}")
        except Exception:
            continue
    # Confine the connection to gold, so a query that slips past the text
    # guard still can't read the rest of the machine.
    con.execute("SET allowed_directories = [?]", [gold_dir])
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    return con


def _spark_session_with_views():
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta reads") from exc
    from gbdp.semantic.render import render, spark_local_target

    spark = SparkSession.builder.getOrCreate()
    root = gold_root()
    for table in _gold_tables():
        try:
            spark.read.format("delta").load(spark_path(root / table)).createOrReplaceTempView(table)
        except Exception:
            continue
    sl = semantic_layer()
    target = spark_local_target()
    for name, view in sl.views.items():
        try:
            spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {render(view.sql, sl, target)}")
        except Exception:
            continue
    return spark


def compute_metrics(request) -> Dict[str, Any]:
    """Compute semantic-layer metrics for a gbdp.semantic.compile.MetricRequest.

    The SQL is compiled from configs/semantic_layer.yaml and filter values are
    bound parameters, never pasted into it. Returns the rows and the SQL.
    """
    from gbdp.semantic.compile import compile_request
    from gbdp.semantic.render import duckdb_target, spark_local_target

    sl = semantic_layer()
    if request.limit is None:
        request.limit = 1000
    if storage_format() == "parquet":
        compiled = compile_request(sl, request, duckdb_target(), param_style="qmark")
        cur = _duckdb_connection().execute(compiled.sql, compiled.params)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    else:
        compiled = compile_request(sl, request, spark_local_target(), param_style="named")
        args = {f"p{i}": v for i, v in enumerate(compiled.params)}
        rows = [r.asDict() for r in _spark_session_with_views().sql(compiled.sql, args=args).collect()]
    return {"metric_view": compiled.metric_view, "columns": compiled.columns, "rows": rows, "sql": compiled.sql}
