"""
Tests for the MCP server tools (mcp_server.py). These call the tool
functions directly rather than going through the stdio MCP protocol --
full protocol round-tripping was verified manually (real stdio_client
session: initialize, list_tools, call_tool all confirmed working against
real local data) and is slow/flaky to run as part of a unit test suite.
"""
from __future__ import annotations

from gbdp.mcp_server import derived_metrics, describe_table, list_tables, query


def test_list_tables_returns_known_tables_with_descriptions():
    tables = list_tables()
    names = {t["table"] for t in tables}
    assert "fact_game" in names
    assert "dim_player" in names
    for t in tables:
        assert t["description"], f"{t['table']} has an empty description"


def test_describe_table_returns_detail_for_known_table():
    detail = describe_table("fact_boxscore_pitching")
    assert detail["table"] == "fact_boxscore_pitching"
    assert "columns" in detail
    assert "ip" in detail["columns"]


def test_describe_table_reports_unknown_table_instead_of_crashing():
    detail = describe_table("not_a_real_table")
    assert "error" in detail
    assert "known_tables" in detail


def test_derived_metrics_includes_batting_average_formula():
    metrics = derived_metrics()
    assert "batting_average" in metrics
    assert "ab" in metrics["batting_average"].lower()


def test_query_tool_rejects_unsafe_sql_without_raising():
    result = query("DROP TABLE fact_game")
    assert len(result) == 1
    assert "error" in result[0]


def test_query_tool_executes_safe_sql():
    result = query("SELECT count(*) AS n FROM fact_game")
    assert len(result) == 1
    assert "n" in result[0]
