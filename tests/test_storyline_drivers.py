"""The "why" behind storylines (insights/drivers.py): the components that
changed, scored against noise, and attached to the insight's facts."""
import json
from datetime import date, timedelta

import duckdb
import pyarrow as pa
import pytest

from gbdp.insights import detectors as d
from gbdp.insights import drivers as dr

AS_OF = date(2025, 8, 31)
SPLIT = AS_OF - timedelta(days=13)


def _row(pid, group, dt, **counts):
    base = {k: 0 for k in dr.SUMS}
    base.update(counts)
    return {"player_id": pid, "pitch_group": group, "pitch_type": counts.pop("pitch_type", "FF"), "dt": dt.isoformat(),
            **{k: float(v) if k in ("ev_sum", "la_sum", "woba_value", "xwoba_value") else int(v) for k, v in base.items()}}


def _q(rows, view="fbpd"):
    con = duckdb.connect()
    con.register(f"{view}_src", pa.Table.from_pylist(rows))
    con.execute(f"CREATE VIEW {view} AS SELECT * FROM {view}_src")

    def q(sql):
        cur = con.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    return q


def test_a_batter_who_stopped_chasing_and_crushes_breaking_balls():
    rows = []
    for i in range(60):  # before: chases 35%, weak on breaking balls
        dt = SPLIT - timedelta(days=61 - i)
        rows.append(_row("V", "fastball", dt, pitches=10, swings=5, chase_pitches=4, chase_swings=1, bip=2,
                         hard_hit=1, ev_sum=180, ev_n=2, woba_den=1, woba_value=0.3, xwoba_value=0.3))
        rows.append(_row("V", "breaking", dt, pitches=6, swings=3, whiffs=1, chase_pitches=4, chase_swings=2, bip=1,
                         ev_sum=80, ev_n=1, woba_den=1, woba_value=0.2, xwoba_value=0.2))
    for i in range(14):  # since: stopped chasing, barrels breaking balls
        dt = SPLIT + timedelta(days=i)
        rows.append(_row("V", "fastball", dt, pitches=10, swings=5, chase_pitches=4, chase_swings=0, bip=2,
                         hard_hit=1, ev_sum=182, ev_n=2, woba_den=1, woba_value=0.35, xwoba_value=0.35))
        rows.append(_row("V", "breaking", dt, pitches=6, swings=3, whiffs=0, chase_pitches=4, chase_swings=0, bip=1,
                         hard_hit=1, barrels=1, ev_sum=101, ev_n=1, woba_den=1, woba_value=1.2, xwoba_value=1.2))
    found = dr.batter_change(_q(rows), "V", SPLIT, AS_OF)
    metrics = {f["metric"]: f for f in found}
    assert "chase_rate" in metrics and metrics["chase_rate"]["since"] == 0.0 and metrics["chase_rate"]["before"] == pytest.approx(0.375)
    assert metrics["chase_rate"]["z"] < -2
    assert len(found) <= 3 and all(abs(f["z"]) >= 2 for f in found)


def test_noise_is_not_a_driver():
    rows = [_row("N", "fastball", SPLIT - timedelta(days=30 - i), pitches=10, swings=5, chase_pitches=4,
                 chase_swings=1, bip=2, hard_hit=1) for i in range(30)]
    rows += [_row("N", "fastball", SPLIT + timedelta(days=i), pitches=10, swings=5, chase_pitches=4,
                  chase_swings=1, bip=2, hard_hit=1) for i in range(14)]
    assert dr.batter_change(_q(rows), "N", SPLIT, AS_OF) == []


def test_a_luck_story_compares_his_contact_with_the_league():
    rows = []
    for i in range(100):
        dt = AS_OF - timedelta(days=i)
        rows.append(_row("W", "fastball", dt, bip=3, hard_hit=0, ev_sum=250, ev_n=3, woba_den=4))   # soft contact
        for k in range(5):
            rows.append(_row(f"L{k}", "fastball", dt, bip=3, hard_hit=1, barrels=0, ev_sum=270, ev_n=3, woba_den=4))
    found = dr.batter_vs_league(_q(rows), "W", AS_OF)
    hh = next(f for f in found if f["metric"] == "hard_hit_rate")
    assert hh["since"] == 0.0 and hh["vs"] == "league" and hh["before"] > 0.25


def test_drivers_attach_to_mlb_player_insights_only(monkeypatch):
    monkeypatch.setattr(dr, "batter_change", lambda q, pid, split, as_of: [{"metric": "chase_rate", "z": -3.0}])
    mlb = {"detector": "change_vs_baseline", "subject_type": "player", "league_code": "MLB", "subject_id": "V",
           "facts_json": json.dumps({"recent_days": 14})}
    npb = dict(mlb, league_code="NPB")
    d._attach_drivers(None, AS_OF, mlb)
    d._attach_drivers(None, AS_OF, npb)
    assert json.loads(mlb["facts_json"])["drivers"] == [{"metric": "chase_rate", "z": -3.0}]
    assert "drivers" not in json.loads(npb["facts_json"])


def test_a_failing_driver_query_keeps_the_insight():
    c = {"detector": "luck_gap", "subject_type": "player", "league_code": "MLB", "subject_id": "W",
         "facts_json": json.dumps({"direction": "lucky"})}
    d._attach_drivers(lambda sql: (_ for _ in ()).throw(RuntimeError("no fbpd")), AS_OF, c)
    assert json.loads(c["facts_json"]) == {"direction": "lucky"}
