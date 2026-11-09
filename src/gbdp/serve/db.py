from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Sequence, Tuple

import time

import duckdb

from gbdp.serve import warehouse as wh

from gbdp.utils.io import gold_root, list_dir, storage_format, path_exists, spark_path

# (column, "eq" or "contains", value)
Filter = Tuple[str, str, Any]


_CACHE: Dict[tuple, Tuple[float, List[Dict[str, Any]]]] = {}
_CACHE_TTL_S = 24 * 60 * 60



def backend() -> str:
    """GBDP_SERVE_BACKEND: duckdb (default; local gold) or databricks (SQL warehouse over Unity Catalog)."""
    value = os.getenv("GBDP_SERVE_BACKEND", "duckdb").strip().lower() or "duckdb"
    if value not in ("duckdb", "databricks"):
        raise wh.WarehouseConfigError(f"GBDP_SERVE_BACKEND={value!r}; use duckdb or databricks")
    return value


_WAREHOUSE: wh.Warehouse | None = None  # tests and scripts can install a client with set_warehouse


def set_warehouse(client: wh.Warehouse | None) -> None:
    global _WAREHOUSE
    _WAREHOUSE = client


def _warehouse() -> wh.Warehouse:
    return _WAREHOUSE or wh.Warehouse(wh.WarehouseSettings.from_env())


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
    columns: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
    """
    Read one gold table. Without dt this reads the latest snapshot, or every
    snapshot with all_dts. Filter values are bound, never pasted into SQL.
    columns picks columns by name (default: all), so an endpoint keeps its
    shape when a table gains columns.
    """
    if backend() == "databricks":
        return _cached(
            ("databricks", _cloud_scope(), table, dt, tuple(filters), int(limit), all_dts, tuple(columns or ())),
            lambda: _query_databricks(table, dt, filters, limit, all_dts, columns),
        )
    root = gold_root()
    dt = dt or (None if all_dts else latest_dt(table))
    if not path_exists(root / table) or not (dt or all_dts):
        return []
    key = (str(root), table, dt, tuple(filters), int(limit), tuple(columns or ()))
    if storage_format() == "parquet":
        return _cached(key, lambda: _query_parquet(root / table, dt, filters, limit, columns))
    return _cached(key, lambda: _query_delta(root / table, dt, filters, limit, columns))


def _cached(key: tuple, load) -> List[Dict[str, Any]]:
    """The 24 h cache: one entry per table read (the URL's table, filters and as_of)."""
    now = time.time()
    cached = _CACHE.get(key)
    if cached and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]
    rows = load()
    _CACHE[key] = (now, rows)
    return rows


def _cloud_scope() -> tuple:
    s = wh.WarehouseSettings.from_env() if _WAREHOUSE is None else _WAREHOUSE.settings
    return (s.host, s.catalog, s.schema)


def _query_databricks(
    table: str, dt: str | None, filters: Sequence[Filter], limit: int, all_dts: bool,
    columns: Sequence[str] | None,
) -> List[Dict[str, Any]]:
    """The same read as _query_parquet, as one parameterized SELECT on the warehouse.

    Latest-snapshot reads resolve the dt inside the statement
    (dt = (SELECT max(dt) ...)), so one round trip serves them. A table that
    doesn't exist yet reads as empty, as it does locally.
    """
    client = _warehouse()
    s = client.settings
    colname = _uc_column if s.uc_mode in ("managed", "serverless") else (lambda c: c)
    tbl = s.qualified(table)
    select = ", ".join(f"`{colname(c)}`" for c in columns) if columns else "*"
    where: List[str] = []
    params: List[Any] = []

    def bind(value: Any) -> str:
        params.append(value)
        return f":p{len(params) - 1}"

    if dt:
        where.append(f"`dt` = {bind(dt)}")
    elif not all_dts:
        where.append(f"`dt` = (SELECT max(`dt`) FROM {tbl})")
    for column, op, value in filters:
        col = f"`{colname(column)}`"
        if op == "contains":
            where.append(f"locate({bind(str(value).lower())}, lower({col})) > 0")
        else:
            where.append(f"{col} = {bind(value)}")
    sql = f"SELECT {select} FROM {tbl}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" LIMIT {int(limit)}"
    try:
        _, rows = client.execute(sql, wh.named_params(params), limit=limit)
    except wh.WarehouseError as exc:
        if exc.table_missing:
            return []
        raise
    return _restore_columns(table, rows, s.uc_mode)


def _uc_column(name: str) -> str:
    from gbdp.semantic.render import uc_managed_column_name

    return uc_managed_column_name(name)


def _restore_columns(table: str, rows: List[Dict[str, Any]], uc_mode: str) -> List[Dict[str, Any]]:
    """Give managed-UC columns back their gold names ("c_2b" -> "2b"), so the
    response matches the local one. Only columns the semantic layer lists are renamed."""
    if uc_mode not in ("managed", "serverless") or not rows:
        return rows
    t = semantic_layer().tables.get(table)
    back = {_uc_column(c): c for c in (t.columns if t else ()) if _uc_column(c) != c}
    if not back:
        return rows
    return [{back.get(k, k): v for k, v in r.items()} for r in rows]


def _query_parquet(
    base: Path, dt: str | None, filters: Sequence[Filter], limit: int, columns: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
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
    select = ", ".join('"' + c.replace('"', '""') + '"' for c in columns) if columns else "*"
    sql = f"SELECT {select} FROM read_parquet(?, union_by_name=true, hive_partitioning=false)"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += f" LIMIT {int(limit)}"
    cur = duckdb.connect().execute(sql, params)
    columns = [c[0] for c in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _query_delta(
    base: Path, dt: str | None, filters: Sequence[Filter], limit: int, columns: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
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
    if columns:
        df = df.select(*columns)
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
    if backend() == "databricks":
        return _run_sql_databricks(stripped, limit)
    limited = f"SELECT * FROM ({stripped}) AS _gbdp_query_result LIMIT {int(limit)}"

    if storage_format() == "parquet":
        cur = _duckdb_connection().execute(limited)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    rows = _spark_session_with_views().sql(limited).collect()
    return [row.asDict() for row in rows]


# A table named with a catalog, or information_schema, reaches past the gold schema.
_QUALIFIED_NAME = re.compile(r"\b([A-Za-z_]\w*)\s*\.\s*([A-Za-z_]\w*)\s*\.\s*[A-Za-z_]\w*")


def assert_gold_only_sql(sql: str, catalog: str, schema: str) -> None:
    """Cloud mode only: the warehouse token may see more than gold, so /query
    can name gold tables and views bare (or as catalog.schema.table), nothing else."""
    # String literals are checked too: a false alarm on 'a.b.c' beats a bypass via escapes.
    bare = sql.replace("`", "")
    if re.search(r"\binformation_schema\b", bare, re.IGNORECASE):
        raise UnsafeSqlError("query can only read the gold tables")
    for m in _QUALIFIED_NAME.finditer(bare):
        if (m.group(1).lower(), m.group(2).lower()) != (catalog.lower(), schema.lower()):
            raise UnsafeSqlError(f"query can only read the gold tables in {catalog}.{schema}")


def _inline_view_ctes(sql: str, target) -> str:
    """WITH-clause text defining every semantic-layer view the SQL names, and the
    views those read. The v_* views exist in Unity Catalog only once the semantic
    layer has been applied; inlining them as CTEs makes them work either way."""
    from gbdp.semantic.render import render

    sl = semantic_layer()
    named = [v for v in sl.views if re.search(rf"\b{re.escape(v)}\b", sql, re.IGNORECASE)]
    if not named:
        return ""
    needed = sl.view_dependencies(named)
    return ",\n".join(f"{v} AS (\n{render(sl.views[v].sql, sl, target)}\n)" for v in needed)


def _databricks_target(settings):
    from gbdp.semantic.render import databricks_target

    return databricks_target(settings.catalog, settings.schema, settings.uc_mode, inline_views=True)


def _run_sql_databricks(stripped: str, limit: int) -> List[Dict[str, Any]]:
    """/query on the warehouse. Read-only three ways: the statement guard, the
    SELECT-over-subquery wrapper (an INSERT/DDL can't parse inside it), and a
    row_limit on the request. Grant the token SELECT only for defence in depth."""
    client = _warehouse()
    s = client.settings
    assert_gold_only_sql(stripped, s.catalog, s.schema)
    limit = max(1, int(limit))
    ctes = _inline_view_ctes(stripped, _databricks_target(s))
    prefix = f"WITH {ctes}\n" if ctes else ""
    # The newline keeps a trailing -- comment from swallowing the closing paren.
    statement = f"{prefix}SELECT * FROM (\n{stripped}\n) AS _gbdp_query_result LIMIT {limit}"
    return client.execute(statement, limit=limit)[1]


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
    sl = semantic_layer()
    for table in _gold_tables():
        glob = os.path.join(gold_dir, table, "**", "*.parquet")
        # dt stays the string the files hold, as in Unity Catalog. Taking it
        # from the dt=... folder instead (hive partitioning) trips DuckDB 1.5
        # internal errors ("Failed to bind column reference dt") once a view
        # filters or windows on it. Every table gold/publish.py and identity
        # write carries dt in the file (test_semantic_layer_schema checks);
        # the audit tables may not, so they keep the folder's.
        hive = "false" if table in sl.tables else "true"
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW \"{table}\" AS SELECT * FROM read_parquet('{glob}', "
                f"union_by_name=true, hive_partitioning={hive}, hive_types_autocast=false)"
            )
        except Exception:
            continue
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
    if backend() == "databricks":
        client = _warehouse()
        compiled = compile_request(sl, request, _databricks_target(client.settings), param_style="named")
        _, rows = client.execute(
            compiled.sql, wh.named_params(compiled.params), limit=request.limit, use_schema=False,
        )
    elif storage_format() == "parquet":
        compiled = compile_request(sl, request, duckdb_target(), param_style="qmark")
        cur = _duckdb_connection().execute(compiled.sql, compiled.params)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    else:
        compiled = compile_request(sl, request, spark_local_target(), param_style="named")
        args = {f"p{i}": v for i, v in enumerate(compiled.params)}
        rows = [r.asDict() for r in _spark_session_with_views().sql(compiled.sql, args=args).collect()]
    return {"metric_view": compiled.metric_view, "columns": compiled.columns, "rows": rows, "sql": compiled.sql}
