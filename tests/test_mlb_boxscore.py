"""
Tests for the MLB Stats API boxscore feature (fetch_boxscores stage), which
feeds fact_boxscore_batting/fact_boxscore_pitching from a real MLB source
for the first time -- previously those tables only had data from
indy_local/kbo_local/lmb_local/retrosheet_local, so a pure-MLB day always
produced empty box scores.
"""
from __future__ import annotations

import json
import tempfile
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

import gbdp.silver.mlb as mlb_module


def _fake_boxscore_bronze_row():
    box = {
        "teams": {
            "home": {
                "team": {"id": 113},
                "players": {
                    "ID682829": {
                        "person": {"id": 682829, "fullName": "Real Batter"},
                        "stats": {
                            "batting": {
                                "atBats": 4, "hits": 2, "doubles": 1, "triples": 0,
                                "homeRuns": 1, "baseOnBalls": 0, "strikeOuts": 1,
                                "rbi": 2, "runs": 1,
                            },
                            "pitching": {},
                        },
                    },
                    "ID686730": {
                        "person": {"id": 686730, "fullName": "Real Pitcher"},
                        "stats": {
                            "batting": {},
                            "pitching": {
                                "inningsPitched": "6.0", "hits": 3, "runs": 1,
                                "earnedRuns": 1, "baseOnBalls": 2, "strikeOuts": 5,
                                "homeRuns": 1,
                            },
                        },
                    },
                    "ID999999": {
                        # entered the game (e.g. pinch runner) but never had a PA/pitch
                        "person": {"id": 999999, "fullName": "Never Appeared"},
                        "stats": {"batting": {"atBats": 0, "plateAppearances": 0}, "pitching": {}},
                    },
                },
            },
            "away": {
                "team": {"id": 121},
                "players": {},
            },
        }
    }
    return {
        "games": [
            {"game_pk": 778498, "status_code": 200, "body_text": json.dumps(box)}
        ]
    }


def test_normalize_boxscore_batting_extracts_real_batters(monkeypatch):
    monkeypatch.setattr(mlb_module, "_read_bronze", lambda source, entity, dt, root: [_fake_boxscore_bronze_row()])

    with tempfile.TemporaryDirectory() as tmp:
        out_path = mlb_module._normalize_boxscore_batting(Path(tmp), date(2025, 4, 1), force=True)
        rows = pq.ParquetFile(str(out_path)).read().to_pylist()

    assert len(rows) == 1, "player with 0 AB / 0 PA must be excluded"
    row = rows[0]
    assert row["player_id"] == "682829"
    assert row["team_id"] == "113"
    assert row["game_id"] == "778498"
    assert row["ab"] == 4
    assert row["h"] == 2
    assert row["hr"] == 1
    assert row["rbi"] == 2


def test_normalize_boxscore_pitching_extracts_real_pitchers(monkeypatch):
    monkeypatch.setattr(mlb_module, "_read_bronze", lambda source, entity, dt, root: [_fake_boxscore_bronze_row()])

    with tempfile.TemporaryDirectory() as tmp:
        out_path = mlb_module._normalize_boxscore_pitching(Path(tmp), date(2025, 4, 1), force=True)
        rows = pq.ParquetFile(str(out_path)).read().to_pylist()

    assert len(rows) == 1
    row = rows[0]
    assert row["player_id"] == "686730"
    assert row["team_id"] == "113"
    assert row["ip"] == "6.0"
    assert row["er"] == 1
    assert row["so"] == 5


def test_boxscore_connector_fetches_one_game_per_scheduled_game(monkeypatch):
    from gbdp.connectors.mlb_statsapi import MlbStatsApiConnector
    from gbdp.connectors.base import Partition

    schedule_response = {"dates": [{"games": [{"gamePk": 778498}, {"gamePk": 778499}]}]}
    boxscore_calls = []

    class FakeConnector(MlbStatsApiConnector):
        def http_get(self, url, params=None):
            if url.endswith("/schedule"):
                from gbdp.bronze.writer import RawPayload
                return RawPayload(
                    source=self.source, entity="schedule", dt="2025-04-01", url=url,
                    params=params or {}, status_code=200, fetched_at_utc="2025-04-01T00:00:00Z",
                    checksum="x", content_type="application/json",
                    body_text=json.dumps(schedule_response),
                )
            boxscore_calls.append(url)
            from gbdp.bronze.writer import RawPayload
            return RawPayload(
                source=self.source, entity="boxscore", dt="2025-04-01", url=url,
                params={}, status_code=200, fetched_at_utc="2025-04-01T00:00:00Z",
                checksum="y", content_type="application/json", body_text="{}",
            )

    connector = FakeConnector(writer=None, cache=None, base_url="https://statsapi.mlb.com/api/v1")
    connector.fetch_partition(Partition(dt="2025-04-01", entity="boxscore", keys={}))

    assert len(boxscore_calls) == 2
    assert "778498" in boxscore_calls[0]
    assert "778499" in boxscore_calls[1]
