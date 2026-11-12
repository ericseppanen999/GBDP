"""Statcast features for the storyline detectors, from silver's raw Savant rows.

Three small gold tables per dt (MLB only; NPB has no Statcast):
  feature_pitch_daily    one row per pitcher x pitch type: pitches, swings,
                         whiffs, velocity and spin sums, and the wOBA / xwOBA
                         of the plate appearances that ended on that pitch type
  feature_batter_pitch_daily  one row per batter x pitch group (fastball,
                         breaking, offspeed): the "why" counters - chase and
                         zone swings, whiffs, hard-hit balls, barrels, exit
                         velocity and launch angle, batted-ball types, pull
                         rate - and wOBA / xwOBA
  feature_contact_daily  one row per batter: wOBA and xwOBA sums over his PAs,
                         balls in play and their BABIP hits

xwOBA follows Savant: a ball in play counts its estimated_woba_using_speedangle,
any other PA-ending event (walk, HBP, strikeout) its woba_value. Only rows
whose woba_denom is 1 (the last pitch of a PA that counts for wOBA) add to
wOBA, xwOBA and their denominator.
"""
from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from gbdp.utils.logging import get_logger
from gbdp.utils.time import daterange, parse_date

logger = get_logger("gbdp.insights")

SWINGS = {"swinging_strike", "swinging_strike_blocked", "foul", "foul_tip", "foul_bunt", "hit_into_play",
          "missed_bunt", "bunt_foul_tip"}
WHIFFS = {"swinging_strike", "swinging_strike_blocked", "missed_bunt"}


def _f(v: Any) -> Optional[float]:
    if v in (None, "", "null"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# Pitch groups for the batter "why" table: what he does against fastballs,
# breaking balls and offspeed pitches.
PITCH_GROUPS = {
    "fastball": {"FF", "SI", "FC", "FA"},
    "breaking": {"SL", "ST", "CU", "KC", "SV", "CS"},
    "offspeed": {"CH", "FS", "FO", "SC"},
}

# The counters both pitch-level tables keep (sums, so any span adds up).
COMPONENTS = ("pitches", "swings", "whiffs", "zone_pitches", "zone_swings", "zone_whiffs", "chase_pitches",
              "chase_swings", "bip", "hard_hit", "barrels", "ev_sum", "ev_n", "la_sum", "la_n", "gb", "ld", "fb",
              "pu", "pulled", "pulled_air", "woba_value", "xwoba_value", "woba_den")


def pitch_group(pitch_type: str) -> str:
    return next((g for g, types in PITCH_GROUPS.items() if pitch_type in types), "other")


def _event(p: Dict[str, Any], raw: Dict[str, Any]) -> Dict[str, float]:
    """One pitch as additive counts over COMPONENTS."""
    desc = p.get("description") or raw.get("description") or ""
    zone = _f(raw.get("zone"))
    in_zone = zone is not None and 1 <= zone <= 9
    out_zone = zone is not None and zone >= 11
    swing = desc in SWINGS
    whiff = desc in WHIFFS
    denom = _f(raw.get("woba_denom")) or 0.0
    woba = _f(raw.get("woba_value")) or 0.0
    xw = _f(raw.get("estimated_woba_using_speedangle"))
    bb_type = raw.get("bb_type") or ""
    in_play = desc == "hit_into_play" or bool(bb_type)
    ev = _f(raw.get("launch_speed")) if in_play else None
    la = _f(raw.get("launch_angle")) if in_play else None
    pulled = False
    hx, hy = _f(raw.get("hc_x")), _f(raw.get("hc_y"))
    if in_play and hx is not None and hy is not None:
        # Spray angle from Statcast's hit coordinates (home plate at 125.42, 198.27):
        # negative is toward left field, where a right-handed batter pulls.
        angle = math.degrees(math.atan2(hx - 125.42, 198.27 - hy))
        pulled = angle < -15 if (raw.get("stand") or "R") == "R" else angle > 15
    return {
        "pitches": 1, "swings": int(swing), "whiffs": int(whiff),
        "zone_pitches": int(in_zone), "zone_swings": int(in_zone and swing), "zone_whiffs": int(in_zone and whiff),
        "chase_pitches": int(out_zone), "chase_swings": int(out_zone and swing),
        "bip": int(in_play), "hard_hit": int(in_play and ev is not None and ev >= 95),
        "barrels": int(in_play and _f(raw.get("launch_speed_angle")) == 6),
        "ev_sum": ev or 0.0, "ev_n": int(ev is not None), "la_sum": la or 0.0, "la_n": int(la is not None),
        "gb": int(bb_type == "ground_ball"), "ld": int(bb_type == "line_drive"), "fb": int(bb_type == "fly_ball"),
        "pu": int(bb_type == "popup"), "pulled": int(pulled),
        "pulled_air": int(pulled and bb_type in ("line_drive", "fly_ball")),
        "woba_value": woba if denom else 0.0,
        "xwoba_value": ((xw if xw is not None else woba) if denom else 0.0),
        "woba_den": int(denom),
    }


def _add(row: Dict[str, Any], ev: Dict[str, float]) -> None:
    for k in COMPONENTS:
        row[k] = row.get(k, 0) + ev[k]


def daily_rows(pitches: List[Dict[str, Any]], player_id, dt: date) -> Dict[str, List[Dict[str, Any]]]:
    """feature_pitch_daily, feature_batter_pitch_daily and feature_contact_daily
    rows for one dt. player_id maps a source MLBAM id to the canonical id."""
    by_pitch: Dict[tuple, Dict[str, Any]] = {}
    by_group: Dict[tuple, Dict[str, Any]] = {}
    by_batter: Dict[str, Dict[str, Any]] = {}
    for p in pitches:
        if not p.get("game_pk"):
            continue
        try:
            raw = json.loads(p.get("raw_json") or "{}")
        except (TypeError, ValueError):
            raw = {}
        pitch_type = p.get("pitch_type") or raw.get("pitch_type") or ""
        pid = player_id(p.get("pitcher_id"))
        bid = player_id(p.get("batter_id"))
        ev = _event(p, raw)
        if pid and pitch_type:
            r = by_pitch.setdefault((pid, pitch_type), {"player_id": pid, "pitch_type": pitch_type, "velo_sum": 0.0,
                                                        "velo_n": 0, "spin_sum": 0.0, "spin_n": 0})
            _add(r, ev)
            velo = _f(p.get("release_speed")) if p.get("release_speed") is not None else _f(raw.get("release_speed"))
            if velo is not None:
                r["velo_sum"] += velo
                r["velo_n"] += 1
            spin = _f(raw.get("release_spin_rate"))
            if spin is not None:
                r["spin_sum"] += spin
                r["spin_n"] += 1
        if bid and pitch_type:
            g = pitch_group(pitch_type)
            _add(by_group.setdefault((bid, g), {"player_id": bid, "pitch_group": g}), ev)
        if bid and ev["woba_den"]:
            b = by_batter.setdefault(bid, {"player_id": bid, "woba_value": 0.0, "xwoba_value": 0.0, "woba_den": 0,
                                           "xwoba_bip_n": 0, "bip": 0, "babip_hits": 0})
            b["woba_value"] += ev["woba_value"]
            b["xwoba_value"] += ev["xwoba_value"]
            b["woba_den"] += ev["woba_den"]
            b["xwoba_bip_n"] += int(ev["bip"] and _f(raw.get("estimated_woba_using_speedangle")) is not None)
            if ev["bip"] and (p.get("events") or raw.get("events")) != "home_run":
                b["bip"] += 1
                b["babip_hits"] += int((_f(raw.get("babip_value")) or 0) > 0)
    for r in list(by_pitch.values()) + list(by_group.values()) + list(by_batter.values()):
        r["league_code"] = "MLB"
        r["source"] = "mlb_statcast"
        for k in ("woba_value", "xwoba_value", "velo_sum", "spin_sum", "ev_sum", "la_sum"):
            if k in r:
                r[k] = round(r[k], 6)
    return {"feature_pitch_daily": list(by_pitch.values()), "feature_batter_pitch_daily": list(by_group.values()),
            "feature_contact_daily": list(by_batter.values())}


def publish_statcast_features(start: str, end: str, root: Path | None = None, force: bool = True) -> List[Path]:
    from gbdp.gold import publish as gp
    from gbdp.utils.io import gold_root

    root = root or gold_root()
    out: List[Path] = []
    for d in daterange(parse_date(start), parse_date(end)):
        bridge = gp._read_gold_bridge(root, d)
        pitches = gp._read_silver(root, "mlb_statcast", "pitches", d)
        rows = daily_rows(pitches, lambda s: gp._canonical(bridge, "player", "mlb_statsapi", s), d)
        out.append(gp._write_gold_table(root, "feature_pitch_daily", d, rows["feature_pitch_daily"], force))
        out.append(gp._write_gold_table(root, "feature_contact_daily", d, rows["feature_contact_daily"], force))
        out.append(gp._write_gold_table(root, "feature_batter_pitch_daily", d, rows["feature_batter_pitch_daily"], force))
        logger.info("statcast features dt=%s: %d pitch-type rows, %d batter rows", d.isoformat(),
                    len(rows["feature_pitch_daily"]), len(rows["feature_contact_daily"]))
    return out
