"""Storyline detectors: batted-ball luck, a new pitch, a velocity change.
Each finds its pattern, respects its thresholds, and its evidence SQL
re-derives the headline number (as verify_claim does)."""
import json
from datetime import date, timedelta

import duckdb
import pyarrow as pa
import pytest

from gbdp.insights import detectors as d
from gbdp.insights.statcast_features import daily_rows

AS_OF = date(2026, 8, 31)
SPLIT = AS_OF - timedelta(days=29)  # recent_days = 30
CFG = {
    "luck_gap": {"min_pa": 200, "min_abs_gap": 0.040},
    "new_pitch": {"recent_days": 30, "min_early_pitches": 400, "min_recent_pitches": 250, "min_xwoba_improvement": 0.02,
                  "max_early_share": 0.03, "min_recent_share": 0.12},
    "velocity_change": {"recent_days": 30, "pitch_types": ["FF", "SI"], "min_pitches": 150, "min_abs_mph": 1.5},
}


def _savant(pitch_type, desc, events=None, woba=0.0, xwoba=None, speed=95.0, spin=2300, babip=None):
    raw = {"pitch_type": pitch_type, "release_spin_rate": str(spin)}
    if events:
        raw.update(woba_denom="1", woba_value=str(woba), events=events)
        if xwoba is not None:
            raw.update(estimated_woba_using_speedangle=str(xwoba), bb_type="line_drive")
        if babip is not None:
            raw["babip_value"] = str(babip)
    return {"game_pk": "1", "pitch_type": pitch_type, "description": desc, "events": events,
            "release_speed": speed, "pitcher_id": "P", "batter_id": "B", "raw_json": json.dumps(raw)}


def test_daily_rows_count_whiffs_velocity_and_xwoba():
    pitches = [
        _savant("FF", "swinging_strike", speed=96.0), _savant("FF", "foul", speed=97.0),
        _savant("SL", "hit_into_play", "single", woba=0.88, xwoba=0.30, speed=85.0, babip=1),
        _savant("FF", "ball", "walk", woba=0.69, speed=95.0),
        _savant("SL", "hit_into_play", "home_run", woba=2.03, xwoba=1.8, speed=86.0, babip=0),
    ]
    rows = daily_rows(pitches, lambda s: f"id-{s}", AS_OF)
    ff = next(r for r in rows["feature_pitch_daily"] if r["pitch_type"] == "FF")
    sl = next(r for r in rows["feature_pitch_daily"] if r["pitch_type"] == "SL")
    assert (ff["pitches"], ff["swings"], ff["whiffs"], ff["velo_sum"], ff["velo_n"]) == (3, 2, 1, 288.0, 3)
    # A walk counts its woba_value as xwOBA too (Savant's rule for non-contact PAs).
    assert (ff["woba_den"], ff["woba_value"], ff["xwoba_value"]) == (1, 0.69, 0.69)
    assert (sl["woba_den"], sl["woba_value"], sl["xwoba_value"]) == (2, 2.91, 2.1)
    (b,) = rows["feature_contact_daily"]
    assert b["player_id"] == "id-B" and b["woba_den"] == 3 and b["xwoba_value"] == pytest.approx(2.79)
    assert (b["bip"], b["babip_hits"], b["xwoba_bip_n"]) == (1, 1, 2)  # a homer isn't a BABIP ball in play


@pytest.fixture
def con():
    c = duckdb.connect()
    c.register("players", pa.Table.from_pylist([{"player_id": "P", "primary_name": "Pat Pitcher"},
                                                {"player_id": "L", "primary_name": "Lucky Larry"}]))
    return c


def _q(c):
    def q(sql):
        cur = c.execute(sql)
        cols = [x[0] for x in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    return q


def _views(c, fpd=(), fcd=()):
    for name, rows, table in (("fpd", fpd, "feature_pitch_daily"), ("fcd", fcd, "feature_contact_daily")):
        if rows:
            c.register(f"{name}_src", pa.Table.from_pylist(list(rows)))
            c.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM {name}_src")
            c.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM {name}_src")


def _evidence(q, cand):
    import re
    return q(re.sub(r"\{\{table:(\w+)\}\}", lambda m: m.group(1), cand["evidence_sql"]))


def test_luck_gap_finds_a_lucky_batter_and_ignores_small_samples(con):
    rows = []
    for i in range(60):
        dt = (AS_OF - timedelta(days=i)).isoformat()
        rows.append({"player_id": "L", "woba_value": 1.6, "xwoba_value": 1.2, "woba_den": 4, "xwoba_bip_n": 2,
                     "bip": 2, "babip_hits": 1, "dt": dt})          # .400 wOBA on .300 xwOBA, 240 PA
        if i < 10:
            rows.append({"player_id": "S", "woba_value": 2.0, "xwoba_value": 0.4, "woba_den": 4, "xwoba_bip_n": 2,
                         "bip": 2, "babip_hits": 2, "dt": dt})      # a bigger gap on 40 PA: too few
    _views(con, fcd=rows)
    q = _q(con)
    found = d.luck_gap(q, AS_OF, CFG["luck_gap"])
    assert [(c["subject_name"], c["value"], c["baseline"]) for c in found] == [("Lucky Larry", 0.4, 0.3)]
    assert json.loads(found[0]["facts_json"])["direction"] == "lucky"
    assert d.recompute(found[0], _evidence(q, found[0])) == pytest.approx({"value": 0.4, "baseline": 0.3})


def _pitches_row(pitch_type, dt, pitches, swings=0, whiffs=0, xv=0.0, wd=0, velo=95.0):
    return {"player_id": "P", "pitch_type": pitch_type, "pitches": pitches, "swings": swings, "whiffs": whiffs,
            "velo_sum": velo * pitches, "velo_n": pitches, "spin_sum": 0.0, "spin_n": 0,
            "woba_value": xv, "xwoba_value": xv, "woba_den": wd, "dt": dt.isoformat()}


def test_new_pitch_needs_a_real_change_in_mix_and_better_results(con):
    rows = []
    for i in range(60):  # before SPLIT: fastball / slider, no sweeper; .350 xwOBA against
        dt = SPLIT - timedelta(days=60 - i)
        rows += [_pitches_row("FF", dt, 12, 5, 1, 1.4, 4, 95.0), _pitches_row("SL", dt, 8, 4, 1, 0.0, 0)]
    for i in range(30):  # since SPLIT: a sweeper 25% of the time; more whiffs, .250 xwOBA against
        dt = SPLIT + timedelta(days=i)
        rows += [_pitches_row("FF", dt, 9, 5, 1, 1.0, 4, 95.2), _pitches_row("SL", dt, 6, 4, 2, 0.0, 0),
                 _pitches_row("ST", dt, 5, 4, 2, 0.0, 0)]
    _views(con, fpd=rows)
    q = _q(con)
    (c,) = d.new_pitch(q, AS_OF, CFG["new_pitch"])
    facts = json.loads(c["facts_json"])
    assert (c["metric"], c["value"], c["baseline"]) == ("pitch_share:ST", 0.25, 0.0)
    assert facts["xwoba_against_since"] < facts["xwoba_against_before"] and facts["since"] == SPLIT.isoformat()
    assert d.recompute(c, _evidence(q, c)) == pytest.approx({"value": 0.25, "baseline": 0.0})


def test_a_new_pitch_with_worse_results_is_not_a_story(con):
    rows = [_pitches_row("FF", SPLIT - timedelta(days=60 - i), 20, 10, 3, 1.0, 4) for i in range(60)]
    rows += [_pitches_row("ST", SPLIT + timedelta(days=i), 5, 5, 0, 2.0, 4) for i in range(30)]
    rows += [_pitches_row("FF", SPLIT + timedelta(days=i), 15, 10, 1, 2.0, 4) for i in range(30)]
    _views(con, fpd=rows)
    assert d.new_pitch(_q(con), AS_OF, CFG["new_pitch"]) == []


def test_velocity_change_on_a_fastball(con):
    rows = [_pitches_row("FF", SPLIT - timedelta(days=60 - i), 10, velo=94.0) for i in range(60)]
    rows += [_pitches_row("FF", SPLIT + timedelta(days=i), 10, velo=96.0) for i in range(30)]
    rows += [_pitches_row("SL", SPLIT + timedelta(days=i), 10, velo=80.0) for i in range(30)]  # not a fastball
    _views(con, fpd=rows)
    q = _q(con)
    (c,) = d.velocity_change(q, AS_OF, CFG["velocity_change"])
    assert (c["metric"], c["value"], c["baseline"]) == ("velo:FF", 96.0, 94.0)
    assert d.recompute(c, _evidence(q, c)) == pytest.approx({"value": 96.0, "baseline": 94.0})
