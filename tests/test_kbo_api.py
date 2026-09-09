"""
Tests for the KBO API connector/normalizer (koreabaseball.com's unofficial
internal .asmx JSON web services -- KBO has no published public API). Fixture
JSON below is trimmed from real captured responses (2025-04-02 games).
"""
from __future__ import annotations

import json
import tempfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import gbdp.silver.kbo_api as kbo_silver
from gbdp.bronze.writer import RawPayload
from gbdp.connectors.base import Partition
from gbdp.connectors.kbo_api import KboApiConnector


def _grid(cells_per_row, headers=None):
    return json.dumps(
        {
            "headers": [{"row": [{"Text": h} for h in headers]}] if headers else [],
            "rows": [{"row": [{"Text": c} for c in row]} for row in cells_per_row],
        }
    )


def _fake_boxscore_json():
    return {
        "code": "100",
        "arrHitter": [
            {  # away
                "table1": _grid([["1", "좌", "테스트타자1"]]),
                "table3": _grid([["4", "1", "2", "1", "0.300"]]),
            },
            {  # home
                "table1": _grid([["1", "중", "테스트타자2"]]),
                "table3": _grid([["3", "0", "1", "0", "0.250"]]),
            },
        ],
        "arrPitcher": [
            {  # away
                "table": _grid(
                    [["테스트투수1", "선발", "패", "0", "1", "0", "5", "20", "70", "18", "5", "0", "2", "4", "3", "3", "5.40"]],
                    headers=["선수명", "등판", "결과", "승", "패", "세", "이닝", "타자", "투구수", "타수", "피안타", "홈런", "4사구", "삼진", "실점", "자책", "평균자책점"],
                )
            },
            {  # home
                "table": _grid(
                    [["테스트투수2", "선발", "승", "1", "0", "0", "9", "30", "100", "28", "3", "0", "1", "8", "0", "0", "0.00"]],
                    headers=["선수명", "등판", "결과", "승", "패", "세", "이닝", "타자", "투구수", "타수", "피안타", "홈런", "4사구", "삼진", "실점", "자책", "평균자책점"],
                )
            },
        ],
    }


def _fake_game_record(**overrides):
    base = {
        "LE_ID": 1, "SR_ID": 0, "SEASON_ID": 2025,
        "G_DT": "20250402", "G_ID": "20250402TEST0",
        "AWAY_ID": "AW", "HOME_ID": "HM",
        "AWAY_NM": "원정팀", "HOME_NM": "홈팀",
        "T_SCORE_CN": "3", "B_SCORE_CN": "5",
        "S_NM": "테스트구장", "GAME_STATE_SC": "3", "CANCEL_SC_ID": "0", "CANCEL_SC_NM": "정상경기",
    }
    base.update(overrides)
    return base


def _write_bronze_row(tmp_root: Path, entity: str, dt_str: str, row: dict) -> None:
    part = tmp_root / "parsed" / "kbo_api" / entity / f"dt={dt_str}"
    part.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([row]), part / "part.parquet")


def test_normalize_games_maps_fields_and_scores_correctly(monkeypatch, tmp_path):
    bronze_root = tmp_path / "bronze"
    _write_bronze_row(bronze_root, "games", "2025-04-02", _fake_game_record())
    monkeypatch.setattr(kbo_silver, "bronze_root", lambda: bronze_root)

    out_path = kbo_silver._normalize_games(tmp_path / "silver", date(2025, 4, 2), force=True)
    rows = pq.ParquetFile(str(out_path)).read().to_pylist()

    assert len(rows) == 1
    row = rows[0]
    assert row["game_id"] == "20250402TEST0"
    assert row["game_date"] == "2025-04-02"
    assert row["home_team_id"] == "HM"
    assert row["away_team_id"] == "AW"
    # T_SCORE_CN (top/away) must map to away_score, B_SCORE_CN (bottom/home)
    # to home_score -- these are named for inning half, NOT team identity,
    # and swapping them silently attributes the wrong score to each team.
    assert row["away_score"] == 3
    assert row["home_score"] == 5
    assert row["status"] == "Final"


def test_normalize_games_marks_cancelled_status(monkeypatch, tmp_path):
    bronze_root = tmp_path / "bronze"
    _write_bronze_row(
        bronze_root, "games", "2025-04-02",
        _fake_game_record(GAME_STATE_SC="4", CANCEL_SC_ID="99", CANCEL_SC_NM="우천취소"),
    )
    monkeypatch.setattr(kbo_silver, "bronze_root", lambda: bronze_root)

    out_path = kbo_silver._normalize_games(tmp_path / "silver", date(2025, 4, 2), force=True)
    rows = pq.ParquetFile(str(out_path)).read().to_pylist()
    assert rows[0]["status"] == "우천취소"


def test_normalize_boxscore_batting_assigns_away_home_correctly(monkeypatch, tmp_path):
    bronze_root = tmp_path / "bronze"
    wrapper = {
        "games": [
            {
                "game_id": "20250402TEST0",
                "away_team_id": "AW",
                "home_team_id": "HM",
                "status_code": 200,
                "body_text": json.dumps(_fake_boxscore_json()),
            }
        ]
    }
    _write_bronze_row(bronze_root, "boxscore", "2025-04-02", wrapper)
    monkeypatch.setattr(kbo_silver, "bronze_root", lambda: bronze_root)

    out_path = kbo_silver._normalize_boxscore_batting(tmp_path / "silver", date(2025, 4, 2), force=True)
    rows = pq.ParquetFile(str(out_path)).read().to_pylist()

    assert len(rows) == 2
    away_row = next(r for r in rows if r["team_id"] == "AW")
    home_row = next(r for r in rows if r["team_id"] == "HM")
    assert away_row["ab"] == 4 and away_row["r"] == 1 and away_row["h"] == 2 and away_row["rbi"] == 1
    assert home_row["ab"] == 3 and home_row["h"] == 1


def test_normalize_boxscore_pitching_maps_by_header_not_position(monkeypatch, tmp_path):
    bronze_root = tmp_path / "bronze"
    wrapper = {
        "games": [
            {
                "game_id": "20250402TEST0",
                "away_team_id": "AW",
                "home_team_id": "HM",
                "status_code": 200,
                "body_text": json.dumps(_fake_boxscore_json()),
            }
        ]
    }
    _write_bronze_row(bronze_root, "boxscore", "2025-04-02", wrapper)
    monkeypatch.setattr(kbo_silver, "bronze_root", lambda: bronze_root)

    out_path = kbo_silver._normalize_boxscore_pitching(tmp_path / "silver", date(2025, 4, 2), force=True)
    rows = pq.ParquetFile(str(out_path)).read().to_pylist()

    assert len(rows) == 2
    home_pitcher = next(r for r in rows if r["team_id"] == "HM")
    assert home_pitcher["ip"] == "9"
    assert home_pitcher["er"] == 0
    assert home_pitcher["so"] == 8


def test_connector_uses_post_with_referer():
    calls = []

    class FakeConnector(KboApiConnector):
        def http_post(self, url, data=None, referer=None):
            calls.append({"url": url, "data": data, "referer": referer})
            return RawPayload(
                source=self.source, entity="unknown", dt="2025-04-02", url=url,
                params=data or {}, status_code=200, fetched_at_utc="2025-04-02T00:00:00Z",
                checksum="x", content_type="application/json",
                body_text=json.dumps({"game": [_fake_game_record()], "code": "100"}),
            )

    connector = FakeConnector(writer=None, cache=None)
    connector.fetch_partition(Partition(dt="2025-04-02", entity="games", keys={}))

    assert len(calls) == 1
    assert calls[0]["referer"], "request must carry a Referer -- koreabaseball.com silently 200s an HTML error page without one"
    assert calls[0]["data"]["date"] == "20250402"
