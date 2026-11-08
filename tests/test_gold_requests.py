"""Gold-layer requests from the semantic layer (SF/SH, dim_team/dim_player for
kbo_api, bridge dedupe, fact_game.game_type) and review items CR-018/CR-019."""
from __future__ import annotations

import json
from datetime import date

import pytest

from gbdp.gold import publish


def _pitch(event, outs, ab=1, n=1, inning=9):
    return {"game_pk": 7, "at_bat_number": ab, "pitch_number": n, "inning": inning, "inning_topbot": "Top",
            "outs_when_up": outs, "events": event, "batter_id": 5, "pitcher_id": 6,
            "raw_json": json.dumps({"bat_score": 0, "post_bat_score": 0})}


# CR-019: a game-ending sacrifice double play is two outs, not one.
@pytest.mark.parametrize("event,outs_before,expected_type,expected_after", [
    ("sac_fly_double_play", 1, "SF", 3),
    ("sac_bunt_double_play", 1, "SH", 3),
    ("strikeout_double_play", 1, "K", 3),
    ("sac_fly", 2, "SF", 3),
    ("grounded_into_double_play", 1, "DP", 3),
    ("triple_play", 0, "TP", 3),
])
def test_final_pa_sacrifice_double_play_ends_the_game(event, outs_before, expected_type, expected_after):
    pa, = publish._statcast_plate_appearances([_pitch(event, outs_before)], date(2026, 6, 10), {})
    assert pa["event_type"] == expected_type
    assert pa["outs_after"] == expected_after
    assert pa["base_state_after"] == 0


def test_final_pa_walk_off_sacrifice_fly_stays_unknown_bases():
    # A walk-off sac fly with 0 out leaves 1 out; the bases after are unknown.
    pa, = publish._statcast_plate_appearances([_pitch("sac_fly", 0)], date(2026, 6, 10), {})
    assert pa["outs_after"] == 1
    assert pa["base_state_after"] is None


# CR-018: the --fresh-dims-before guard checks the run's RESOLVED start.
def _cli(monkeypatch, argv):
    import sys

    from gbdp import cli

    monkeypatch.setenv("GBDP_FRESH_DIMS_BEFORE", "")  # restored afterwards
    ran = []
    monkeypatch.setattr(cli, "run_pipeline", lambda *a, **k: ran.append(a))
    monkeypatch.setattr(sys, "argv", ["gbdp", *argv])
    cli.main()
    return ran


def test_fresh_dims_guard_uses_the_backfill_env_start(monkeypatch):
    monkeypatch.setenv("GBDP_BACKFILL_START", "2026-06-01")
    monkeypatch.setenv("GBDP_BACKFILL_END", "2026-06-30")
    with pytest.raises(SystemExit) as e:
        _cli(monkeypatch, ["backfill", "--fresh-dims-before", "2026-05-13", "--stages", "gold_publish"])
    assert e.value.code == 2
    monkeypatch.setenv("GBDP_BACKFILL_START", "2026-05-13")
    assert _cli(monkeypatch, ["backfill", "--fresh-dims-before", "2026-05-13", "--stages", "gold_publish"])


def test_fresh_dims_guard_uses_the_window_start(monkeypatch):
    # --window replaces --start; a flag matching the ignored --start is refused.
    with pytest.raises(SystemExit) as e:
        _cli(monkeypatch, ["run", "--start", "2026-05-13", "--end", "2026-05-13", "--window", "yesterday",
                           "--fresh-dims-before", "2026-05-13", "--stages", "gold_publish"])
    assert e.value.code == 2


def test_fresh_dims_empty_is_a_no_op(monkeypatch):
    import os

    assert _cli(monkeypatch, ["backfill", "--start", "2026-06-01", "--end", "2026-06-01",
                              "--fresh-dims-before", "", "--stages", "gold_publish"])
    assert os.environ["GBDP_FRESH_DIMS_BEFORE"] == ""


# ---- dim_team / dim_player / bridge for kbo_api, and fact_game.game_type ----

@pytest.fixture
def lake(tmp_path, monkeypatch):
    bronze, silver, gold = tmp_path / "bronze", tmp_path / "silver", tmp_path / "gold"
    gold.mkdir()
    monkeypatch.setenv("GBDP_BRONZE_ROOT", str(bronze))
    monkeypatch.setenv("GBDP_SILVER_ROOT", str(silver))
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(gold))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    monkeypatch.delenv("GBDP_FRESH_DIMS_BEFORE", raising=False)
    return bronze, silver, gold


def _parquet(root, source, entity, day, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    part = root / source / entity / f"dt={day}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), part / "part-00001.parquet")


DAY = "2026-06-10"


def _kbo_silver(silver):
    # A doubleheader: both teams appear in two games; a player both bats and pitches.
    _parquet(silver, "kbo_api", "games", DAY, [
        {"game_id": "20260610LGOB0", "home_team_id": "OB", "home_team_name": "Doosan Bears",
         "away_team_id": "LG", "away_team_name": "LG Twins", "home_score": 3, "away_score": 2,
         "status": "Final", "sr_id": "0"},
        {"game_id": "20260610LGOB2", "home_team_id": "OB", "home_team_name": "Doosan Bears",
         "away_team_id": "LG", "away_team_name": "LG Twins", "home_score": 1, "away_score": 4,
         "status": "Final", "sr_id": "7"},
    ])
    _parquet(silver, "kbo_api", "boxscore_batting", DAY, [
        {"game_id": "20260610LGOB0", "team_id": "OB", "player_name": "김재환", "ab": 4, "h": 2},
        {"game_id": "20260610LGOB2", "team_id": "OB", "player_name": "김재환", "ab": 3, "h": 1},
        {"game_id": "20260610LGOB0", "team_id": "LG", "player_name": "오지환", "ab": 4, "h": 0},
    ])
    _parquet(silver, "kbo_api", "boxscore_pitching", DAY, [
        {"game_id": "20260610LGOB0", "team_id": "LG", "player_name": "오지환", "ip": "1", "h": 0},
    ])


def _resolve(gold):
    from gbdp.identity import resolver
    from gbdp.utils.io import read_parquet_rows

    resolver._resolve_for_date(gold, date.fromisoformat(DAY), {}, True)
    return read_parquet_rows(gold / "bridge_source_ids" / f"dt={DAY}")


def test_bridge_has_one_row_per_source_id_per_day(lake):
    _, silver, gold = lake
    _kbo_silver(silver)
    rows = _resolve(gold)
    keys = [(r["entity_type"], r["source"], r["source_id"], r["dt"]) for r in rows]
    assert len(keys) == len(set(keys))
    assert {k[2] for k in keys if k[0] == "team"} == {"OB", "LG"}
    assert {k[2] for k in keys if k[0] == "player"} == {"김재환", "오지환"}


def test_kbo_api_players_and_teams_get_dimension_rows_matching_the_facts(lake):
    from gbdp.utils.io import read_parquet_rows

    _, silver, gold = lake
    _kbo_silver(silver)
    _resolve(gold)
    d = date.fromisoformat(DAY)
    bridge = publish._read_gold_bridge(gold, d)
    publish._write_dim_player(gold, d, bridge, True)
    publish._write_dim_team(gold, d, bridge, True)
    publish._write_fact_boxscore_batting(gold, d, bridge, True)
    publish._write_fact_boxscore_pitching(gold, d, bridge, True)

    players = {r["player_id"]: r for r in read_parquet_rows(gold / "dim_player" / f"dt={DAY}") if r["source"] == "kbo_api"}
    assert {r["primary_name"] for r in players.values()} == {"김재환", "오지환"}
    assert all(r["is_current"] and r["valid_from_dt"] == DAY for r in players.values())
    teams = {r["team_id"] for r in read_parquet_rows(gold / "dim_team" / f"dt={DAY}")}
    for table in ("fact_boxscore_batting", "fact_boxscore_pitching"):
        facts = [r for r in read_parquet_rows(gold / table / f"dt={DAY}") if r["source"] == "kbo_api"]
        assert facts and all(r["player_id"] in players for r in facts), table
        assert all(r["team_id"] in teams for r in facts), table


def test_fact_game_type_for_kbo_api_retrosheet_and_local_sources(lake):
    from gbdp.silver import retrosheet
    from gbdp.utils.io import read_parquet_rows

    bronze, silver, gold = lake
    _kbo_silver(silver)
    # Retrosheet: silver maps gameinfo.gametype to statsapi codes.
    _parquet(bronze / "parsed", "retrosheet_local", "gameinfo", DAY, [
        {"gid": "NYA202606100", "date": "20260610", "hometeam": "NYA", "visteam": "BOS",
         "hruns": "5", "vruns": "3", "site": "NYC21", "gametype": "regular"},
        {"gid": "NYA202606101", "date": "20260610", "hometeam": "NYA", "visteam": "BOS",
         "hruns": "2", "vruns": "1", "site": "NYC21", "gametype": "worldseries"},
    ])
    retrosheet._normalize_entity(silver, "gameinfo", date.fromisoformat(DAY), True)
    # A local source passes game_type through when its file carries one.
    _parquet(silver, "lmb_local", "games", DAY, [
        {"game_id": "L1", "home_team_id": "MTY", "away_team_id": "TIJ", "game_type": "R"},
        {"game_id": "L2", "home_team_id": "MEX", "away_team_id": "OAX"},
    ])
    publish._write_fact_game(gold, date.fromisoformat(DAY), {}, True)
    got = {(r["source"], r["source_game_id"]): r["game_type"]
           for r in read_parquet_rows(gold / "fact_game" / f"dt={DAY}")}
    assert got[("kbo_api", "20260610LGOB0")] == "R"
    assert got[("kbo_api", "20260610LGOB2")] == "P"   # SR_ID 7: Korean Series
    assert got[("retrosheet_local", "NYA202606100")] == "R"
    assert got[("retrosheet_local", "NYA202606101")] == "W"
    assert got[("lmb_local", "L1")] == "R"
    assert got[("lmb_local", "L2")] is None


@pytest.mark.parametrize("raw,code", [
    ("regular", "R"), ("Regular", "R"), ("playoff", "R"), ("exhibition", "E"), ("allstar", "A"),
    ("wildcard", "F"), ("divisionseries", "D"), ("lcs", "L"), ("worldseries", "W"),
    ("championship", "P"), ("", None), (None, None),
])
def test_retrosheet_game_types(raw, code):
    from gbdp.silver.retrosheet import retrosheet_game_type

    assert retrosheet_game_type(raw) == code
