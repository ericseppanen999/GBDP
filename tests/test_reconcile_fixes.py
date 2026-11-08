"""Fixes found by reconciling 2026-06-10 against MLB's own boxscores (scripts/reconcile_day.py)."""
from __future__ import annotations

import json
from datetime import date

from gbdp.gold import publish
from gbdp.utils.ids import ulid_from_key


def test_mlb_rosters_are_fetched_as_of_the_partition_date():
    from gbdp.bronze.writer import RawPayload
    from gbdp.connectors.base import Partition
    from gbdp.connectors.mlb_statsapi import MlbStatsApiConnector

    calls = []

    class Fake(MlbStatsApiConnector):
        def http_get(self, url, params):
            calls.append((url, dict(params)))
            body = json.dumps({"teams": [{"id": 147}]}) if url.endswith("/teams") else json.dumps({"roster": []})
            return RawPayload(source="mlb_statsapi", entity="x", dt="2026-06-10", url=url, params=params,
                              status_code=200, fetched_at_utc="2026-11-07T00:00:00Z", checksum="c",
                              content_type="application/json", body_text=body)

    payload = Fake(None, None, "https://statsapi.mlb.com/api/v1").fetch_partition(
        Partition(dt="2026-06-10", entity="rosters", keys={}))
    roster_calls = [p for u, p in calls if u.endswith("/roster")]
    assert roster_calls and all(p["date"] == "2026-06-10" for p in roster_calls)
    assert payload.params["date"] == "2026-06-10"


def test_combined_payload_checksum_covers_every_team_not_just_the_last():
    from gbdp.bronze.writer import RawPayload
    from gbdp.connectors.base import Partition
    from gbdp.connectors.mlb_statsapi import MlbStatsApiConnector

    def connector(first_team_roster):
        class Fake(MlbStatsApiConnector):
            def http_get(self, url, params):
                if url.endswith("/teams"):
                    body = json.dumps({"teams": [{"id": 1}, {"id": 2}]})
                elif "/teams/1/" in url:
                    body = json.dumps({"roster": first_team_roster})
                else:
                    body = json.dumps({"roster": ["same"]})
                return RawPayload(source="mlb_statsapi", entity="x", dt="2026-06-10", url=url, params=params,
                                  status_code=200, fetched_at_utc="t", checksum=f"sub-{hash(body)}",
                                  content_type="application/json", body_text=body)
        return Fake(None, None, "https://statsapi.mlb.com/api/v1")

    part = Partition(dt="2026-06-10", entity="rosters", keys={})
    a = connector(["Judge"]).fetch_partition(part)
    b = connector(["Judge", "Soto"]).fetch_partition(part)
    # Only team 1 (not the last) changed; the two payloads must not share a filename.
    assert a.checksum != b.checksum


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


def test_dim_team_takes_abbreviation_and_city_from_the_team_list(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from gbdp.utils.io import read_parquet_rows

    silver, gold = tmp_path / "silver", tmp_path / "gold"
    gold.mkdir()
    monkeypatch.setenv("GBDP_SILVER_ROOT", str(silver))
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(gold))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    day = "2026-06-10"
    for entity, rows in {
        "games": [{"game_id": "1", "home_team_id": "147", "home_team_name": "New York Yankees",
                   "away_team_id": "114", "away_team_name": "Cleveland Guardians"}],
        "teams": [{"team_id": "147", "team_name": "New York Yankees", "team_abbrev": "NYY", "home_city": "Bronx"},
                  {"team_id": "133", "team_name": "Athletics", "team_abbrev": "ATH", "home_city": "Sacramento"}],
    }.items():
        part = silver / "mlb_statsapi" / entity / f"dt={day}"
        part.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rows), part / "part-00001.parquet")

    publish._write_dim_team(gold, date.fromisoformat(day), {}, True)

    teams = {r["team_name"]: r for r in read_parquet_rows(gold / "dim_team" / f"dt={day}")}
    assert (teams["New York Yankees"]["team_abbrev"], teams["New York Yankees"]["home_city"]) == ("NYY", "Bronx")
    assert "Athletics" in teams  # no game that day, still a team
    assert teams["Cleveland Guardians"]["team_abbrev"] is None  # only seen in a game
    assert len({r["team_id"] for r in teams.values()}) == 3


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
