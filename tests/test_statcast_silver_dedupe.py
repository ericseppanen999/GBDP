from datetime import date

import gbdp.silver.mlb as mlb


def _pitch(ab, n, **extra):
    row = {"game_pk": "778899", "at_bat_number": str(ab), "pitch_number": str(n),
           "pitcher": "1", "batter": "2", "description": "hit_into_play", "events": "field_out"}
    row.update(extra)
    return row


def _run(monkeypatch, tmp_path, bronze):
    captured = {}
    monkeypatch.setattr(mlb, "_read_bronze", lambda *a, **k: bronze)
    monkeypatch.setattr(mlb, "write_parquet", lambda rows, path, force=False: captured.setdefault("rows", rows) and path)
    mlb._normalize_pitches(tmp_path, date(2025, 4, 1), force=True)
    return captured["rows"]


def test_identical_repeated_pitch_rows_are_dropped(monkeypatch, tmp_path, caplog):
    # Savant listed 145 balls in play twice, byte for byte, for 2025-04-01.
    bronze = [_pitch(1, 1), _pitch(1, 2), _pitch(1, 2), _pitch(2, 1, ingested_at_utc="x"), _pitch(2, 1, ingested_at_utc="y")]
    rows = _run(monkeypatch, tmp_path, bronze)
    assert [(r["at_bat_number"], r["pitch_number"]) for r in rows] == [(1, 1), (1, 2), (2, 1)]
    assert "2 identical, 0 conflicting" in caplog.text


def test_conflicting_repeat_keeps_first_and_is_reported(monkeypatch, tmp_path, caplog):
    bronze = [_pitch(1, 1, events="single"), _pitch(1, 1, events="field_out")]
    rows = _run(monkeypatch, tmp_path, bronze)
    assert len(rows) == 1 and rows[0]["events"] == "single"
    assert "0 identical, 1 conflicting" in caplog.text


def test_distinct_pitches_are_all_kept(monkeypatch, tmp_path):
    bronze = [_pitch(1, 1), _pitch(1, 2), _pitch(2, 1)]
    assert len(_run(monkeypatch, tmp_path, bronze)) == 3
