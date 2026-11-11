"""Semantic-layer views on small hand-written gold, for cases the fixture can't show."""
from __future__ import annotations

from datetime import date

import pytest

from gbdp.gold import publish
from gbdp.semantic.compile import Filter, MetricRequest
from gbdp.serve import db

D1, D2 = date(2026, 5, 1), date(2026, 5, 2)


@pytest.fixture
def gold(tmp_path, monkeypatch):
    root = tmp_path / "gold"
    root.mkdir()
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(root))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    db._CACHE.clear()
    return root


def _write(root, table, day, rows):
    publish._write_gold_table(root, table, day, [dict(r) for r in rows], True)


def _player(pid, name, valid_from="2026-05-01"):
    return {"player_id": pid, "primary_name": name, "bats": "R", "throws": "R", "primary_position": "P",
            "league_id": "L", "valid_from_dt": valid_from, "valid_to_dt": None, "is_current": True}


def test_current_players_come_from_the_newest_snapshot_only(gold):
    # A coach in the older snapshot and gone from the newest (a from-scratch
    # rebuild that dropped staff, or an old partition) is not current.
    _write(gold, "dim_player", D1, [_player("p1", "Player"), _player("c1", "Coach")])
    _write(gold, "dim_player", D2, [_player("p1", "Player Renamed", "2026-05-02"),
                                    {**_player("p1", "Player"), "is_current": False, "valid_to_dt": "2026-05-01"}])
    rows = db.run_sql("SELECT player_id, primary_name FROM v_player_current")
    assert rows == [{"player_id": "p1", "primary_name": "Player Renamed"}]


def test_current_teams_come_from_the_newest_snapshot_only(gold):
    team = {"team_name": "T", "team_abbrev": None, "home_city": None, "league_id": "L",
            "valid_from_dt": "2026-05-01", "valid_to_dt": None, "is_current": True}
    _write(gold, "dim_team", D1, [{**team, "team_id": "t1"}, {**team, "team_id": "gone"}])
    _write(gold, "dim_team", D2, [{**team, "team_id": "t1", "team_abbrev": "TT"}])
    assert db.run_sql("SELECT team_id, team_abbrev FROM v_team_current") == [{"team_id": "t1", "team_abbrev": "TT"}]


def _pa(pa_id, event, base, outs, base_after, outs_after, runs):
    return {"pa_id": pa_id, "game_id": "g", "inning": 1, "is_top_inning": True, "batting_team_id": "t1",
            "fielding_team_id": "t2", "batter_id": "p1", "pitcher_id": "p2", "event_type": event, "rbi": None,
            "runs_scored_on_play": runs, "outs_on_play": outs_after - outs, "base_state_before": base,
            "outs_before": outs, "base_state_after": base_after, "outs_after": outs_after,
            "runs_to_end_of_inning": None, "source": "mlb_statcast"}


def _re(base, outs, n, runs, exp=None):
    row = {"league_code": "MLB", "league_id": "L", "season_year": 2026, "base_state": base, "outs": outs,
           "pa_count": n, "runs": runs}
    if exp is not None:
        row["exp_runs"] = exp
    return row


def test_run_value_ignores_a_stale_run_expectancy(gold):
    # D2's run_expectancy is stale: D1 was rebuilt after it was published, so
    # it disagrees with the daily rows. Run values sum run_expectancy_daily.
    _write(gold, "fact_plate_appearance", D1, [_pa("a", "1B", 0, 0, 1, 0, 0)])
    _write(gold, "run_expectancy_daily", D1, [_re(0, 0, 2, 1), _re(1, 0, 1, 1)])  # (0,0)=0.5, (1,0)=1.0
    _write(gold, "run_expectancy_daily", D2, [_re(0, 0, 2, 1)])                    # (0,0) -> 2/4 = 0.5
    _write(gold, "run_expectancy", D2, [_re(0, 0, 9, 90, 10.0), _re(1, 0, 9, 0, 0.0)])  # wrong on purpose
    rows = db.run_sql("SELECT re_before, re_after, run_value FROM v_plate_appearance")
    assert rows == [{"re_before": pytest.approx(0.5), "re_after": pytest.approx(1.0), "run_value": pytest.approx(0.5)}]
    out = db.compute_metrics(MetricRequest(metrics=["re24"], filters=[Filter("batter_id", "eq", "p1")]))
    assert out["rows"] == [{"re24": pytest.approx(0.5)}]


def test_obp_counts_sac_flies_not_sac_bunts(gold):
    # Official OBP: (H + BB + HBP) / (AB + BB + HBP + SF). Neither SF nor SH
    # is an at-bat.
    _write(gold, "fact_plate_appearance", D1, [
        _pa("a", "1B", 0, 0, 1, 0, 0), _pa("b", "OUT", 1, 0, 1, 1, 0), _pa("c", "SF", 4, 1, 0, 2, 1),
        _pa("d", "SH", 1, 1, 2, 2, 0), _pa("e", "BB", 2, 2, 3, 2, 0)])
    _write(gold, "run_expectancy_daily", D1, [_re(0, 0, 1, 0)])  # v_plate_appearance reads it
    out = db.compute_metrics(MetricRequest(
        metrics=["pa", "ab", "sac_flies", "sac_bunts", "avg", "obp"], filters=[Filter("batter_id", "eq", "p1")]))
    row = out["rows"][0]
    assert (row["pa"], row["ab"], row["sac_flies"], row["sac_bunts"]) == (5, 2, 1, 1)
    assert row["avg"] == pytest.approx(1 / 2)
    assert row["obp"] == pytest.approx(2 / (2 + 1 + 1))  # (H + BB) / (AB + BB + SF); SH is in neither
