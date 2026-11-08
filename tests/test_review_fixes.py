"""Fixes from code review CR-002, CR-004, CR-006, CR-008."""
from __future__ import annotations

from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gbdp.gold import publish
from gbdp.utils.io import read_parquet_rows


@pytest.fixture
def env(tmp_path, monkeypatch):
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    gold.mkdir()
    monkeypatch.setenv("GBDP_SILVER_ROOT", str(silver))
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(gold))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    return silver, gold


def _silver(root, source, entity, day, rows):
    part = root / source / entity / f"dt={day}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), part / "part-00001.parquet")


def test_dims_do_not_carry_a_previous_seasons_snapshot(env):
    silver, gold = env
    _silver(silver, "mlb_statsapi", "rosters", "2024-05-18", [{"team_id": "1", "player_id": "old", "player_name": "Old Guy"}])
    publish._write_dim_player(gold, date(2024, 5, 18), {}, True)
    _silver(silver, "mlb_statsapi", "rosters", "2026-05-13", [{"team_id": "1", "player_id": "new", "player_name": "New Guy"}])
    publish._write_dim_player(gold, date(2026, 5, 13), {}, True)

    current = {r["primary_name"] for r in read_parquet_rows(gold / "dim_player" / "dt=2026-05-13") if r["is_current"]}
    assert current == {"New Guy"}


def test_a_day_without_the_team_list_keeps_abbreviation_and_city(env):
    silver, gold = env
    game = [{"game_id": "1", "home_team_id": "147", "home_team_name": "New York Yankees",
             "away_team_id": "114", "away_team_name": "Cleveland Guardians"}]
    _silver(silver, "mlb_statsapi", "games", "2026-06-10", game)
    _silver(silver, "mlb_statsapi", "teams", "2026-06-10",
            [{"team_id": "147", "team_name": "New York Yankees", "team_abbrev": "NYY", "home_city": "Bronx"}])
    publish._write_dim_team(gold, date(2026, 6, 10), {}, True)
    _silver(silver, "mlb_statsapi", "games", "2026-06-11", game)  # no team list that day
    publish._write_dim_team(gold, date(2026, 6, 11), {}, True)

    rows = read_parquet_rows(gold / "dim_team" / "dt=2026-06-11")
    nyy = [r for r in rows if r["team_name"] == "New York Yankees"]
    assert [(r["team_abbrev"], r["home_city"], r["is_current"]) for r in nyy] == [("NYY", "Bronx", True)]


def test_rebuilding_an_earlier_date_refreshes_later_run_expectancy(env):
    _, gold = env

    def pas(day, runs):
        row = {"source": "mlb_statcast", "game_id": "g", "inning": 1, "is_top_inning": True,
               "runs_scored_on_play": 0, "outs_after": 1, "base_state_before": 0, "outs_before": 0,
               "runs_to_end_of_inning": runs}
        publish._write_gold_table(gold, "fact_plate_appearance", day, [row], True)

    d1, d2 = date(2026, 5, 1), date(2026, 5, 2)
    pas(d1, 1)
    publish._write_run_expectancy(gold, d1, True)
    pas(d2, 1)
    publish._write_run_expectancy(gold, d2, True)
    # d1 is rebuilt with new data after d2 exists: d2's season-to-date must follow.
    pas(d1, 3)
    publish._write_run_expectancy(gold, d1, True)
    publish._refresh_run_expectancy_after(gold, d1, True)

    cell = [r for r in read_parquet_rows(gold / "run_expectancy" / "dt=2026-05-02")
            if (r["league_code"], r["base_state"], r["outs"]) == ("MLB", 0, 0)][0]
    assert (cell["pa_count"], cell["runs"], cell["exp_runs"]) == (2, 4, 2.0)


def test_bronze_uc_publish_keeps_only_the_newest_fetch():
    from pathlib import Path

    text = Path("src/gbdp/catalog/managed_publish.py").read_text(encoding="utf-8")
    body = text[text.index("def _publish_bronze_parsed"):text.index("def _latest_fetch")]
    assert "_latest_fetch(spark.read" in body


def test_latest_fetch_filter_with_spark():
    pyspark = pytest.importorskip("pyspark.sql")
    from gbdp.catalog.managed_publish import _latest_fetch

    spark = pyspark.SparkSession.builder.master("local[1]").getOrCreate()
    df = spark.createDataFrame(
        [("old", "2026-05-22T04:00:00", 1), ("old", "2026-05-22T04:00:00", 2), ("new", "2026-11-07T19:00:00", 3)],
        ["response_checksum", "ingested_at_utc", "v"])
    assert [r["v"] for r in _latest_fetch(df).collect()] == [3]


def test_fact_game_carries_the_game_type(env):
    silver, gold = env
    _silver(silver, "mlb_statsapi", "games", "2026-07-14",
            [{"game_id": "1", "home_team_id": "159", "away_team_id": "160", "game_type": "A", "status": "Final"}])
    publish._write_fact_game(gold, date(2026, 7, 14), {}, True)
    assert read_parquet_rows(gold / "fact_game" / "dt=2026-07-14")[0]["game_type"] == "A"
