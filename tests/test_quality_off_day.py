"""row_count_min:fact_game hard-failed 2026-07-13, the All-Star break Monday,
when no league played: it now fails only short of what silver scheduled."""
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gbdp.quality import checks

DAY = "2026-07-13"


def _write(root, *parts, rows, schema=None):
    path = root.joinpath(*parts)
    path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema) if rows or schema is None else schema.empty_table()
    pq.write_table(table, path / "part-00001.parquet")


@pytest.fixture
def lake(tmp_path, monkeypatch):
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    monkeypatch.setenv("GBDP_SILVER_ROOT", str(silver))
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(gold))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    monkeypatch.setattr(checks, "_in_season", lambda dt: True)
    # Gold's fact_game for the day: no rows.
    _write(gold, "fact_game", f"dt={DAY}", rows=[], schema=pa.schema([("game_id", pa.string()), ("dt", pa.string())]))
    return silver, gold


def _row_count(gold):
    cfg = {"row_count_min": [{"table": "gbdp_gold.fact_game", "min_count": 1}]}
    try:
        checks._run_for_date(gold, date.fromisoformat(DAY), cfg, True)
        return True
    except RuntimeError as exc:
        assert "row_count_min:fact_game" in str(exc)
        return False


def test_no_game_scheduled_anywhere_passes(lake):
    silver, gold = lake
    # Silver's empty-day placeholder is a sentinel row, not a game.
    for src in ("mlb_statsapi", "npb_spaia", "kbo_api"):
        _write(silver, src, "games", f"dt={DAY}", rows=[{"dt": DAY, "empty": True}])
    assert _row_count(gold)


def test_games_scheduled_but_none_in_gold_still_fails(lake):
    silver, gold = lake
    _write(silver, "npb_spaia", "games", f"dt={DAY}", rows=[{"game_id": "g1", "dt": DAY}])
    assert not _row_count(gold)


def test_a_failing_date_does_not_stop_the_later_ones(monkeypatch, tmp_path):
    seen = []

    def run(root, d, cfg, force):
        seen.append(d.isoformat())
        if d.day == 13:
            raise RuntimeError(f"quality_checks dt={d.isoformat()} failed hard invariant(s): x")
        return tmp_path

    monkeypatch.setattr(checks, "_run_for_date", run)
    monkeypatch.setattr(checks, "_load_quality_config", lambda: {})
    with pytest.raises(RuntimeError, match="dt=2026-07-13"):
        checks.run_quality_checks("2026-07-12", "2026-07-15", root=tmp_path)
    assert seen == ["2026-07-12", "2026-07-13", "2026-07-14", "2026-07-15"]
