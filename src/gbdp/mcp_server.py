"""
MCP server exposing the GBDP gold layer to any MCP-compatible LLM client
(e.g. Claude Code, Claude Desktop) as tools -- schema introspection, the
governed metric definitions from configs/semantic_layer.yaml, and ad-hoc
read-only SQL, so an agent can compute standard stats the same way the API
and dashboard do, or explore the data with its own queries.

Run standalone (stdio transport, for registering as an MCP server):
    python -m gbdp.mcp_server

Register in Claude Code with (adjust the path):
    claude mcp add gbdp -- python -m gbdp.mcp_server
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from gbdp.semantic.compile import Filter, MetricRequest
from gbdp.semantic.layer import SemanticLayerError
from gbdp.serve.db import (
    UnsafeSqlError,
    compute_metrics,
    known_table_names,
    known_view_names,
    load_semantic_layer,
    run_sql,
    semantic_layer,
)

mcp = FastMCP("gbdp")


def _one_line(text: Any) -> str:
    return " ".join(str(text or "").split())


@mcp.tool()
def list_tables() -> List[Dict[str, str]]:
    """List every queryable table and view in the GBDP gold layer with a
    one-line description. Call this first to see what's available."""
    layer = load_semantic_layer()
    out: List[Dict[str, str]] = []
    for name, meta in layer.get("tables", {}).items():
        out.append({"table": name, "description": _one_line((meta or {}).get("description"))})
    for name, desc in layer.get("audit_tables", {}).items():
        out.append({"table": name, "description": f"[audit/operational] {_one_line(desc)}"})
    for name, meta in layer.get("views", {}).items():
        out.append({"table": name, "description": f"[view] {_one_line((meta or {}).get('description'))}"})
    return out


@mcp.tool()
def describe_table(table: str) -> Dict[str, Any]:
    """Get full detail for one table or view: description, grain, keys,
    column meanings, and known caveats/limitations. Call this before writing
    a query against an unfamiliar table -- several tables have non-obvious
    gotchas (e.g. fact_boxscore_pitching.ip is a string like "6.1", not a
    decimal; fact_plate_appearance has TRUNCATED rows that are not PAs)."""
    layer = load_semantic_layer()
    if table in layer.get("tables", {}):
        return {"table": table, **layer["tables"][table], "conventions": layer.get("conventions", {})}
    if table in layer.get("audit_tables", {}):
        return {"table": table, "description": layer["audit_tables"][table], "category": "audit"}
    if table in layer.get("views", {}):
        return {"table": table, "category": "view", **layer["views"][table]}
    return {
        "error": f"unknown table '{table}'",
        "known_tables": known_table_names() + known_view_names(),
    }


@mcp.tool()
def vocabularies() -> Dict[str, Any]:
    """Code lists used in gold: event_type (and which codes count as a PA,
    an at-bat, a hit, ...), base_state bitmask labels, source -> league,
    final game statuses, Statcast pitch results and pitch types."""
    return load_semantic_layer().get("vocabularies", {})


@mcp.tool()
def list_metrics() -> List[Dict[str, str]]:
    """Every governed baseball metric (AVG, OBP, SLG, OPS, PA, HR, BB%, K%,
    RE24, IP, ERA, WHIP, K/9, BB/9, team W/L and run differential, pitch
    velocity and whiff rate, run expectancy, ...). Prefer compute_metric to
    writing your own formula: these handle the traps (IP strings like "6.1",
    TRUNCATED rows that are not plate appearances, games listed twice)."""
    sl = semantic_layer()
    return [{"name": m.name, "description": m.description, "metric_view": m.metric_view}
            for m in sl.metrics.values()]


@mcp.tool()
def describe_metric(name: str) -> Dict[str, Any]:
    """One metric's definition: SQL expression, base tables, why that source,
    caveats, and the dimensions it can be grouped and filtered by."""
    sl = semantic_layer()
    try:
        return sl.metric_summary(name)
    except SemanticLayerError as exc:
        return {"error": str(exc), "known_metrics": list(sl.metrics)}


@mcp.tool()
def compute_metric(
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    filters: Optional[List[Dict[str, Any]]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    order_by: Optional[List[Dict[str, str]]] = None,
    limit: int = 1000,
) -> Dict[str, Any]:
    """Compute metrics grouped by dimensions, using the semantic layer's SQL.

    metrics: names from list_metrics, all from one metric view
      (e.g. ["avg", "obp", "slg", "pa"]).
    dimensions: names from describe_metric (e.g. ["batter_name"]).
    filters: [{"dimension": "league_code", "op": "eq", "value": "MLB"}];
      op is eq, ne, gt, gte, lt, lte, in, not_in, is_null or not_null.
    start / end: inclusive YYYY-MM-DD on the metric view's game date.
    order_by: [{"column": "avg", "direction": "desc"}].
    Returns {"rows": [...], "columns": [...], "sql": "..."}.
    """
    try:
        request = MetricRequest(
            metrics=metrics,
            dimensions=dimensions or [],
            filters=[Filter(f["dimension"], f.get("op", "eq"), f.get("value")) for f in filters or []],
            start=start,
            end=end,
            order_by=[(o["column"], o.get("direction", "desc")) for o in order_by or []],
            limit=limit,
        )
        return compute_metrics(request)
    except SemanticLayerError as exc:
        return {"error": str(exc)}
    except (KeyError, TypeError, AttributeError) as exc:
        return {"error": f"malformed request: {exc}"}
    except Exception as exc:
        return {"error": f"metric query failed: {exc}"}


@mcp.tool()
def query(sql: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """Run a read-only SQL query (SELECT or WITH...SELECT only) against the
    GBDP gold layer. Tables are addressable by their bare name from
    list_tables() (e.g. `SELECT * FROM fact_game WHERE dt = '2026-09-08'`)
    regardless of whether the backend is local parquet/DuckDB or Databricks
    Delta/Spark -- the same query text works either way. The semantic
    layer's views (v_plate_appearance, v_team_game, ...) are queryable too.
    Call list_tables() and describe_table() first if you're not already
    familiar with the schema, and prefer compute_metric for standard stats.
    Results are capped at `limit` rows (default 1000)."""
    try:
        return run_sql(sql, limit=limit)
    except UnsafeSqlError as exc:
        return [{"error": str(exc)}]
    except Exception as exc:
        return [{"error": f"query failed: {exc}"}]


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
