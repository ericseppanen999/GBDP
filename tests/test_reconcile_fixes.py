"""Fixes found by reconciling 2026-06-10 against MLB's own boxscores (scripts/reconcile_day.py)."""
from __future__ import annotations

import json
from datetime import date

from gbdp.gold import publish
from gbdp.utils.ids import ulid_from_key


def test_a_bridge_miss_still_gets_the_deterministic_canonical_id():
    assert publish._canonical({}, "player", "mlb_statsapi", 665862) == ulid_from_key("player:source:mlb_statsapi:665862")
    assert publish._canonical({("player", "mlb_statsapi", "1"): "OVERRIDE"}, "player", "mlb_statsapi", 1) == "OVERRIDE"
    assert publish._canonical({}, "player", "mlb_statsapi", None) is None
    assert publish._canonical({}, "player", "mlb_statsapi", "") is None


def _pitch(n, bat, post, event=None, ab=1):
    return {"game_pk": 1, "at_bat_number": ab, "pitch_number": n, "inning": 1, "inning_topbot": "Top",
            "outs_when_up": 0, "events": event, "batter_id": 5, "pitcher_id": 6,
            "raw_json": json.dumps({"bat_score": bat, "post_bat_score": post})}


def test_statcast_runs_count_across_the_whole_plate_appearance():
    # A run scores on a wild pitch (pitch 1), then the PA ends in a strikeout.
    pa, = publish._statcast_plate_appearances([_pitch(1, 0, 1), _pitch(2, 1, 1, "strikeout")], date(2026, 6, 10), {})
    assert pa["runs_scored_on_play"] == 1


def test_season_list_players_get_identity_and_a_dimension_row(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from gbdp.identity import resolver

    silver, gold = tmp_path / "silver", tmp_path / "gold"
    gold.mkdir()
    monkeypatch.setenv("GBDP_SILVER_ROOT", str(silver))
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(gold))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    day = "2026-06-10"

    def write(source, entity, rows):
        part = silver / source / entity / f"dt={day}"
        part.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rows), part / "part-00001.parquet")

    write("mlb_statsapi", "rosters", [{"team_id": "147", "player_id": "1", "player_name": "On Roster"}])
    write("mlb_statsapi", "player_bios", [
        {"player_id": "1", "player_name": "On Roster", "dob": "1990-01-01"},
        {"player_id": "665862", "player_name": "Jazz Chisholm Jr.", "dob": "1998-02-01", "team_id": "147"},
    ])

    sources = resolver._collect_player_sources(gold, date.fromisoformat(day))
    assert sorted(r["source_id"] for r in sources if r["source"] == "mlb_statsapi") == ["1", "665862"]

    publish._write_dim_player(gold, date.fromisoformat(day), {}, True)
    from gbdp.utils.io import read_parquet_rows

    names = {r["primary_name"] for r in read_parquet_rows(gold / "dim_player" / f"dt={day}")}
    assert names == {"On Roster", "Jazz Chisholm Jr."}
