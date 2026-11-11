"""
Tests for the MCP server tools (mcp_server.py). These call the tool
functions directly rather than going through the stdio MCP protocol --
full protocol round-tripping was verified manually (real stdio_client
session: initialize, list_tools, call_tool all confirmed working against
real local data) and is slow/flaky to run as part of a unit test suite.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from gbdp.mcp_server import describe_metric, describe_table, list_metrics, list_tables, query


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


def test_list_metrics_includes_batting_average():
    metrics = {m["name"]: m for m in list_metrics()}
    assert "avg" in metrics and metrics["avg"]["metric_view"] == "mv_batting"
    detail = describe_metric("avg")
    assert "is_ab" in detail["expr"] and "fact_plate_appearance" in detail["base_tables"]
    assert {"player", "team", "league", "season", "date"} <= {d["role"] for d in detail["dimensions"]}


def test_describe_metric_reports_unknown_metric_instead_of_crashing():
    detail = describe_metric("batting_average")
    assert "error" in detail and "avg" in detail["known_metrics"]


def test_query_tool_rejects_unsafe_sql_without_raising():
    result = query("DROP TABLE fact_game")
    assert len(result) == 1
    assert "error" in result[0]


def test_query_tool_executes_safe_sql(tmp_path, monkeypatch):
    # Builds its own gold table: data/ is gitignored, so reading the real one
    # passes on a dev machine and fails on every clean CI checkout.
    part = tmp_path / "fact_game" / "dt=2025-04-01"
    part.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{"game_id": "a"}, {"game_id": "b"}]), part / "part-00001.parquet")
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(tmp_path))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")

    result = query("SELECT count(*) AS n FROM fact_game")
    assert result == [{"n": 2}]


def test_insight_tools_find_explain_and_verify(semantic_gold):
    from gbdp import mcp_server as m

    found = m.find_insights(limit=50)
    assert found and "error" not in found[0], found[:1]
    assert {"insight_id", "detector", "value", "score", "facts"} <= set(found[0])
    assert "evidence_sql" not in found[0]
    streaks = m.find_insights(detector="hitting_streak")
    assert streaks and all(r["detector"] == "hitting_streak" for r in streaks)

    for row in found:
        detail = m.explain_insight(row["insight_id"])
        assert "error" not in detail, detail
        assert detail["evidence_rows"], row["detector"]
        ok = m.verify_claim(row["insight_id"], value=row["value"], baseline=row.get("baseline"))
        assert ok["verified"], (row["detector"], ok)
    wrong = m.verify_claim(found[0]["insight_id"], value=(found[0]["value"] or 0) + 1)
    assert wrong["verified"] is False
    assert m.verify_claim("no-such-id", value=1)["verified"] is False
