"""Run expectancy (RE24): runs to the end of the inning, then season-to-date averages."""
from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient

from gbdp.gold import publish
from gbdp.utils.io import read_parquet_rows


def _pa(inning, top, runs, outs_after, base=0, outs=0, source="mlb_statcast", game="g1"):
    return {"source": source, "game_id": game, "inning": inning, "is_top_inning": top,
            "runs_scored_on_play": runs, "outs_after": outs_after,
            "base_state_before": base, "outs_before": outs}


def test_runs_to_end_of_inning_sums_the_rest_of_the_half_inning():
    rows = [_pa(1, True, 0, 1), _pa(1, True, 2, 1), _pa(1, True, 0, 3), _pa(1, False, 1, 3)]
    publish._fill_runs_to_end_of_inning(rows)
    assert [r["runs_to_end_of_inning"] for r in rows] == [2, 2, 0, 1]


def test_an_unfinished_half_inning_is_left_out():
    # Walk-off: the inning ends on the winning run, short of 3 outs.
    rows = [_pa(9, False, 0, 1), _pa(9, False, 1, 1)]
    publish._fill_runs_to_end_of_inning(rows)
    assert [r["runs_to_end_of_inning"] for r in rows] == [None, None]


def test_unknown_runs_blank_that_pa_and_everything_before_it():
    rows = [_pa(1, True, 0, 1), _pa(1, True, None, 2), _pa(1, True, 1, 3)]
    publish._fill_runs_to_end_of_inning(rows)
    assert [r["runs_to_end_of_inning"] for r in rows] == [None, None, 1]


@pytest.fixture
def gold(tmp_path, monkeypatch):
    root = tmp_path / "gold"
    root.mkdir()
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(root))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    return root


def _write_pas(root, day, rows):
    for r in rows:
        r["runs_to_end_of_inning"] = r.pop("rest")
    publish._write_gold_table(root, "fact_plate_appearance", day, rows, True)


def _re(root, day):
    return {(r["league_code"], r["base_state"], r["outs"]): r for r in read_parquet_rows(root / "run_expectancy" / f"dt={day}")}


def test_run_expectancy_is_season_to_date_and_per_league(gold):
    d1, d2 = date(2026, 5, 1), date(2026, 5, 2)
    _write_pas(gold, d1, [{**_pa(1, True, 0, 1), "rest": 1}, {**_pa(1, True, 0, 1, source="npb_spaia"), "rest": 5}])
    publish._write_run_expectancy(gold, d1, True)
    _write_pas(gold, d2, [{**_pa(1, True, 0, 1), "rest": 0}])
    publish._write_run_expectancy(gold, d2, True)

    day1, day2 = _re(gold, d1), _re(gold, d2)
    assert day1[("MLB", 0, 0)]["exp_runs"] == 1.0
    assert day1[("NPB", 0, 0)]["exp_runs"] == 5.0
    # (1 + 0) runs over 2 PAs, and NPB isn't mixed in.
    assert (day2[("MLB", 0, 0)]["pa_count"], day2[("MLB", 0, 0)]["exp_runs"]) == (2, 0.5)
    assert day2[("NPB", 0, 0)]["exp_runs"] == 5.0


def test_a_new_season_starts_from_zero(gold):
    _write_pas(gold, date(2025, 9, 30), [{**_pa(1, True, 0, 1), "rest": 4}])
    publish._write_run_expectancy(gold, date(2025, 9, 30), True)
    _write_pas(gold, date(2026, 4, 1), [{**_pa(1, True, 0, 1), "rest": 0}])
    publish._write_run_expectancy(gold, date(2026, 4, 1), True)

    assert _re(gold, date(2026, 4, 1))[("MLB", 0, 0)]["pa_count"] == 1


def test_rebuilding_a_date_does_not_double_count(gold):
    d = date(2026, 5, 1)
    _write_pas(gold, d, [{**_pa(1, True, 0, 1), "rest": 2}])
    publish._write_run_expectancy(gold, d, True)
    publish._write_run_expectancy(gold, d, True)
    assert _re(gold, d)[("MLB", 0, 0)]["pa_count"] == 1


def test_api_filters_by_league_and_returns_a_seasons_latest_table(gold):
    from gbdp.serve import db
    from gbdp.serve.api import app

    db._CACHE.clear()
    for day, runs in ((date(2026, 5, 1), 1), (date(2026, 5, 2), 0)):
        _write_pas(gold, day, [{**_pa(1, True, 0, 1), "rest": runs}, {**_pa(1, True, 0, 1, source="npb_spaia"), "rest": 3}])
        publish._write_run_expectancy(gold, day, True)

    client = TestClient(app)
    rows = client.get("/run-expectancy", params={"league": "MLB", "season": 2026}).json()
    assert [(r["dt"], r["pa_count"], r["exp_runs"]) for r in rows] == [("2026-05-02", 2, 0.5)]
    assert {r["league_code"] for r in client.get("/run-expectancy").json()} == {"MLB", "NPB"}
