"""Insight detectors: each finds its pattern, respects its sample-size and
score thresholds, and its evidence SQL re-derives the headline number."""
from datetime import date, timedelta

import duckdb
import pyarrow as pa
import pytest

from gbdp.insights import detectors as d

AS_OF = date(2026, 8, 31)
CFG = {
    "definition_version": "test",
    "hitting_streak": {"min_games": 5},
    "change_vs_baseline": {"recent_days": 14, "min_recent_pa": 40, "min_baseline_pa": 150,
                           "min_abs_z": 2.5, "woba_sd_per_pa": 0.5},
    "league_month_shift": {"min_month_pa": 1000, "min_abs_z": 3.0},
    "cross_league_gap": {"min_season_pa": 1000, "min_abs_z": 4.0},
    "top_per_detector": 10,
}


def _day(pid, league, dt, pa=4, h=1, hr=0, bb=0, so=1, wv=None, wd=None):
    wd = pa if wd is None else wd
    wv = (0.9 * h if wv is None else wv)
    return {"player_id": pid, "league_code": league, "pa": pa, "ab": pa - bb, "h": h, "doubles": 0, "triples": 0,
            "hr": hr, "bb": bb, "hbp": 0, "so": so, "sf": 0, "sh": 0, "woba_den": wd, "woba_value": float(wv),
            "dt": dt.isoformat()}


@pytest.fixture
def query():
    con = duckdb.connect()

    def make(rows, names=()):
        con.register("fbd_src", pa.Table.from_pylist(rows))
        con.execute("CREATE OR REPLACE VIEW fbd AS SELECT * FROM fbd_src")
        con.register("players", pa.Table.from_pylist(
            [{"player_id": p, "primary_name": n} for p, n in names] or [{"player_id": "x", "primary_name": "x"}]))
        # the evidence SQL names the gold table
        con.execute("CREATE OR REPLACE VIEW feature_batter_daily AS SELECT * FROM fbd_src")

        def q(sql):
            cur = con.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        return q
    return make


def _evidence(q, c):
    return q(c["evidence_sql"].replace("{{table:feature_batter_daily}}", "feature_batter_daily"))


def test_an_active_hitting_streak_is_found_and_a_broken_one_is_not(query):
    rows = [_day("streak", "NPB", AS_OF - timedelta(days=i), h=1) for i in range(7)]
    rows += [_day("broken", "MLB", AS_OF - timedelta(days=i), h=0 if i == 0 else 1) for i in range(9)]
    q = query(rows, [("streak", "田中")])
    found = d.hitting_streaks(q, AS_OF, CFG["hitting_streak"])
    assert [(c["subject_id"], c["value"], c["subject_name"]) for c in found] == [("streak", 7.0, "田中")]
    assert len([r for r in _evidence(q, found[0]) if r["h"] > 0]) == 7


def test_change_vs_baseline_needs_a_real_swing_on_enough_pas(query):
    rows = []
    for i in range(60):  # season baseline: weak
        rows.append(_day("hot", "MLB", AS_OF - timedelta(days=20 + i), pa=4, h=1, wv=1.0))
        rows.append(_day("flat", "MLB", AS_OF - timedelta(days=20 + i), pa=4, h=1, wv=1.3))
    for i in range(12):  # last 14 days
        rows.append(_day("hot", "MLB", AS_OF - timedelta(days=i), pa=4, h=3, wv=2.6))
        rows.append(_day("flat", "MLB", AS_OF - timedelta(days=i), pa=4, h=1, wv=1.3))
    q = query(rows)
    found = d.change_vs_baseline(q, AS_OF, CFG["change_vs_baseline"])
    assert [c["subject_id"] for c in found] == ["hot"]
    c = found[0]
    ev = {r["span"]: r for r in _evidence(q, c)}
    assert ev["recent"]["woba"] == pytest.approx(c["value"], abs=1e-4)
    assert ev["baseline"]["woba"] == pytest.approx(c["baseline"], abs=1e-4)


def test_league_month_shift_and_cross_league_gap(query):
    rows = []
    for i in range(300):  # July: 1 HR every 50 PA; August: 1 every 20
        rows.append(_day(f"m{i}", "MLB", date(2026, 7, 15), pa=5, h=1, hr=1 if i % 10 == 0 else 0, so=1))
        rows.append(_day(f"a{i}", "MLB", date(2026, 8, 15), pa=5, h=1, hr=1 if i % 4 == 0 else 0, so=1))
        rows.append(_day(f"n{i}", "NPB", date(2026, 7, 15), pa=5, h=1, hr=0, so=0))
        rows.append(_day(f"o{i}", "NPB", date(2026, 8, 15), pa=5, h=1, hr=0, so=0))
    q = query(rows)
    shifts = d.league_month_shift(q, AS_OF, CFG["league_month_shift"])
    hr = [c for c in shifts if c["league_code"] == "MLB" and c["metric"] == "hr_rate"]
    assert len(hr) == 1 and hr[0]["value"] > hr[0]["baseline"]
    by_month = {r["month"]: r["hr_rate"] for r in _evidence(q, hr[0])}
    assert by_month["2026-08"] == pytest.approx(hr[0]["value"]) and by_month["2026-07"] == pytest.approx(hr[0]["baseline"])
    gaps = {c["metric"] for c in d.cross_league_gap(q, AS_OF, CFG["cross_league_gap"])}
    assert {"k_rate", "hr_rate"} <= gaps


def test_detect_ranks_caps_and_stamps_the_version(query):
    rows = [_day(f"p{j}", "NPB", AS_OF - timedelta(days=i)) for j in range(15) for i in range(6 + j)]
    cfg = dict(CFG, top_per_detector=3)
    out = [c for c in d.detect(query(rows), AS_OF, cfg) if c["detector"] == "hitting_streak"]
    assert [c["rank"] for c in out] == [1, 2, 3]
    assert out[0]["value"] >= out[1]["value"] >= out[2]["value"]
    assert all(c["definition_version"] == "test" for c in out)
    assert len({c["insight_id"] for c in out}) == 3


def test_publish_insights_writes_gold(tmp_path, monkeypatch):
    import pyarrow.parquet as pq

    from gbdp.insights.publish import publish_insights
    from gbdp.utils.io import read_parquet_rows

    gold = tmp_path / "gold"
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(gold))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    for i in range(6):
        dt = AS_OF - timedelta(days=i)
        part = gold / "feature_batter_daily" / f"dt={dt.isoformat()}"
        part.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist([_day("s", "NPB", dt)]), part / "part-00001.parquet")
    snap = gold / "dim_player" / f"dt={AS_OF.isoformat()}"
    snap.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{"player_id": "s", "primary_name": "鈴木"}]), snap / "part-00001.parquet")
    monkeypatch.setattr("gbdp.insights.publish.insights_config", lambda: dict(CFG, hitting_streak={"min_games": 5}))
    publish_insights(AS_OF.isoformat(), AS_OF.isoformat())
    rows = read_parquet_rows(gold / "insight_candidates" / f"dt={AS_OF.isoformat()}")
    assert [(r["detector"], r["subject_name"], r["value"]) for r in rows] == [("hitting_streak", "鈴木", 6.0)]


def test_a_streak_whose_player_stopped_playing_is_not_active(query):
    # Soderstrom, 2026-08-31: an 11-game streak that ended on his last game, 08-14.
    rows = [_day("idle", "MLB", date(2026, 8, 14) - timedelta(days=i)) for i in range(11)]
    rows += [_day("live", "MLB", AS_OF - timedelta(days=1 + i)) for i in range(11)]
    found = d.hitting_streaks(query(rows), AS_OF, dict(CFG["hitting_streak"], max_days_since_last_game=3))
    assert [c["subject_id"] for c in found] == ["live"]


def test_only_the_latest_month_shift_is_reported(query):
    rows = []
    for i in range(300):  # June -> July jump in HR, then July == August
        rows.append(_day(f"j{i}", "NPB", date(2026, 6, 15), pa=5, hr=0, so=1))
        rows.append(_day(f"k{i}", "NPB", date(2026, 7, 15), pa=5, hr=1 if i % 3 == 0 else 0, so=1))
        rows.append(_day(f"l{i}", "NPB", date(2026, 8, 15), pa=5, hr=1 if i % 3 == 0 else 0, so=1))
    shifts = d.league_month_shift(query(rows), AS_OF, CFG["league_month_shift"])
    assert not [c for c in shifts if c["metric"] == "hr_rate"]  # July's jump is old news by August
