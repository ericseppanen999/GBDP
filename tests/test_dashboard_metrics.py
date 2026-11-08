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
    assert "NOT IN ('BB', 'HBP', 'SAC')" not in sql
    assert "source.data_source = 'mlb_statcast'" in sql and "source.season_year = 2026" in sql


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
