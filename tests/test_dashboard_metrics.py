"""The dashboard's metric datasets come from the semantic layer's definitions."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


def _builder():
    spec = importlib.util.spec_from_file_location("build_dashboard", Path("scripts/build_dashboard.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sql(dataset: str) -> str:
    on_disk = json.loads(Path("dashboards/gbdp_season.lvdash.json").read_text(encoding="utf-8"))
    return "".join(next(d for d in on_disk["datasets"] if d["name"] == dataset)["queryLines"])


def test_hitters_use_the_governed_pa_and_at_bat_flags():
    # fixes CR-007: PA and AVG from is_pa / is_ab, not an inline event list
    # that counted TRUNCATED, INT and UNKNOWN as at-bats.
    sql = _sql("hitters")
    assert "SUM(source.is_pa) AS pa" in sql
    assert "SUM(source.is_hit) / NULLIF(SUM(source.is_ab), 0) AS avg" in sql
    assert "NOT IN ('BB', 'HBP', 'SAC')" not in sql and "NOT IN ('BB', 'HBP', 'SF', 'SH')" not in sql
    assert "source.data_source = 'mlb_statcast'" in sql and f"source.season_year = ({_builder().SEASON_SQL})" in sql


def test_standings_are_regular_season_only():
    # fixes CR-008: MLB records from regular-season games.
    sql = _sql("standings")
    assert "source.season_phase = 'regular'" in sql
    assert "WHEN source = 'mlb_statsapi' AND (game_type IN ('R')) THEN 'regular'" in sql


def test_widget_columns_are_dataset_columns():
    mod = _builder()
    datasets = mod.all_datasets()
    for name, cols in {"hitters": ["batter", "pa", "hr", "hits", "bb", "k", "avg"],
                       "standings": ["team", "w", "l", "pct", "run_diff"],
                       "pitch_mix": ["pitch", "avg_mph"]}.items():
        sql = datasets[name][1]
        for c in cols:
            assert f" AS {c}" in sql, (name, c)


def test_dashboard_metric_sql_gives_the_api_answer(semantic_gold):
    # Run the dashboard's compiled hitters SQL (Databricks names swapped for
    # the local tables) and compare with the API's answer for the same metric.
    from gbdp.serve import db

    sql = _builder().all_datasets()["hitters"][1].replace("gbdp.gold_gbdp.", "")
    sql = sql.replace("HAVING SUM(source.is_pa) >= 20", "HAVING SUM(source.is_pa) >= 1")
    con = db._duckdb_connection()
    rows = {r[1]: r for r in con.execute(sql).fetchall()}
    assert rows["Aaron Batter"][2] == 6  # pa
    assert rows["Aaron Batter"][-1] == pytest.approx(0.6)  # avg


def _local(sql: str) -> str:
    return sql.replace("gbdp.gold_gbdp.", "")


def test_totals_count_only_the_season(semantic_gold):
    # fixes SL-016: a test-day partition from another year (gold holds
    # 2004-05-18 and 2024-05-18) doesn't count on the 2026 season page.
    from gbdp.serve import db

    mod = _builder()
    con = db._duckdb_connection()
    before = con.execute(_local(mod.all_datasets()["totals"][1])).fetchone()
    for table in ("fact_game", "fact_plate_appearance", "fact_pitch"):
        con.execute(f"CREATE TABLE {table}_copy AS SELECT * FROM {table}")
        con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM {table}_copy UNION ALL BY NAME "
                    f"SELECT * REPLACE ('2024-05-18' AS dt) FROM {table}_copy")
    after = con.execute(_local(mod.all_datasets()["totals"][1])).fetchone()
    assert after == before
    assert before[0] == "2026" and before[1] > 0 and before[-1].startswith("2026-")
    for dataset in ("daily_coverage", "quality"):
        assert f"YEAR(CAST(dt AS DATE)) = ({mod.SEASON_SQL})" in _sql(dataset)


def test_the_season_rolls_over_with_the_first_regular_season_game(semantic_gold):
    # The dashboard shows the latest year with an MLB regular-season game:
    # next year's spring training doesn't move it, its first R game does.
    from gbdp.serve import db

    mod = _builder()
    totals = _local(mod.all_datasets()["totals"][1])
    con = db._duckdb_connection()
    con.execute("CREATE TABLE fact_game_copy AS SELECT * FROM fact_game")
    for game_type, season in (("S", "2026"), ("R", "2027")):
        con.execute(f"CREATE OR REPLACE VIEW fact_game AS SELECT * FROM fact_game_copy UNION ALL BY NAME "
                    f"SELECT * REPLACE ('2027-03-01' AS dt, '{game_type}' AS game_type) FROM fact_game_copy "
                    f"WHERE source = 'mlb_statsapi'")
        assert con.execute(totals).fetchone()[0] == season, game_type


def test_pitch_mix_drops_empty_pitch_types(semantic_gold):
    # fixes SL-016: an empty pitch_type is not its own bar.
    from gbdp.serve import db

    con = db._duckdb_connection()
    con.execute("CREATE TABLE fact_pitch_copy AS SELECT * FROM fact_pitch")
    con.execute("CREATE OR REPLACE VIEW fact_pitch AS SELECT * FROM fact_pitch_copy UNION ALL BY NAME "
                "SELECT * REPLACE ('' AS pitch_type) FROM fact_pitch_copy")
    sql = _local(_builder().all_datasets()["pitch_mix"][1])
    sql = sql.replace("HAVING COUNT(*) >= 50", "HAVING COUNT(*) >= 1")
    pitches = [r[0] for r in con.execute(sql).fetchall()]
    assert pitches and "" not in pitches and None not in pitches


def test_generated_files_are_written_with_lf(tmp_path, monkeypatch):
    # fixes SL-018: a Windows regen doesn't rewrite every line as CRLF.
    mod = _builder()
    monkeypatch.setattr(mod, "OUT", tmp_path / "d.json")
    monkeypatch.setattr("sys.argv", ["build_dashboard.py"])
    assert mod.main() == 0
    assert b"\r\n" not in (tmp_path / "d.json").read_bytes()

    spec = importlib.util.spec_from_file_location("build_semantic_layer", Path("scripts/build_semantic_layer.py"))
    sl_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sl_mod)
    monkeypatch.setattr("sys.argv", ["build_semantic_layer.py", "--out", str(tmp_path / "uc.sql")])
    assert sl_mod.main() == 0
    assert b"\r\n" not in (tmp_path / "uc.sql").read_bytes()


def test_a_sql_filter_value_needs_the_literal_style():
    # SqlExpr is for trusted generator code only; a parameterised (API) query refuses it.
    from gbdp.semantic import layer as semantic
    from gbdp.semantic.compile import Filter, MetricRequest, SqlExpr, compile_request
    from gbdp.semantic.layer import SemanticLayerError
    from gbdp.semantic.render import duckdb_target

    sl = semantic.load()
    req = MetricRequest(metrics=["pa"], filters=[Filter("season_year", "eq", SqlExpr("SELECT 2026"))])
    with pytest.raises(SemanticLayerError):
        compile_request(sl, req, duckdb_target(), param_style="qmark")
    assert "season_year = (SELECT 2026)" in compile_request(sl, req, duckdb_target(), param_style="literal").sql
