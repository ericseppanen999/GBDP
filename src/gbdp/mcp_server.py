"""
MCP server exposing the GBDP gold layer to any MCP-compatible LLM client
(e.g. Claude Code, Claude Desktop) as tools -- schema introspection plus
ad-hoc read-only SQL, so an agent can explore the data and run its own
queries instead of being boxed into a fixed set of REST endpoints.

Run standalone (stdio transport, for registering as an MCP server):
    python -m gbdp.mcp_server

Register in Claude Code with (adjust the path):
    claude mcp add gbdp -- python -m gbdp.mcp_server
"""
from __future__ import annotations

from typing import Any, Dict, List

from mcp.server.fastmcp import FastMCP

from gbdp.serve.db import UnsafeSqlError, known_table_names, load_semantic_layer, run_sql

mcp = FastMCP("gbdp")


@mcp.tool()
def list_tables() -> List[Dict[str, str]]:
    """List every queryable table in the GBDP gold layer with a one-line
    description. Call this first to see what's available."""
    layer = load_semantic_layer()
    out: List[Dict[str, str]] = []
    for name, meta in layer.get("tables", {}).items():
        out.append({"table": name, "description": (meta or {}).get("description", "").strip()})
    for name, desc in layer.get("audit_tables", {}).items():
        out.append({"table": name, "description": f"[audit/operational] {desc}"})
    return out


@mcp.tool()
def describe_table(table: str) -> Dict[str, Any]:
    """Get full detail for one table: description, grain, column meanings,
    and known caveats/limitations. Call this before writing a query against
    an unfamiliar table -- several tables have non-obvious gotchas (e.g.
    fact_boxscore_pitching.ip is a string like "6.1", not a decimal)."""
    layer = load_semantic_layer()
    if table in layer.get("tables", {}):
        return {"table": table, **layer["tables"][table]}
    if table in layer.get("audit_tables", {}):
        return {"table": table, "description": layer["audit_tables"][table], "category": "audit"}
    return {
        "error": f"unknown table '{table}'",
        "known_tables": known_table_names(),
    }


@mcp.tool()
def derived_metrics() -> Dict[str, str]:
    """SQL formulas for common baseball metrics (batting average, OBP, SLG,
    ERA, etc.) that are NOT materialized as columns anywhere in gold --
    compute them yourself using these formulas in your query."""
    layer = load_semantic_layer()
    return layer.get("derived_metrics", {})


@mcp.tool()
def query(sql: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """Run a read-only SQL query (SELECT or WITH...SELECT only) against the
    GBDP gold layer. Tables are addressable by their bare name from
    list_tables() (e.g. `SELECT * FROM fact_game WHERE dt = '2026-09-08'`)
    regardless of whether the backend is local parquet/DuckDB or Databricks
    Delta/Spark -- the same query text works either way. Call list_tables()
    and describe_table() first if you're not already familiar with the
    schema. Results are capped at `limit` rows (default 1000)."""
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
