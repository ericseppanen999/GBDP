"""Statcast features for the storyline detectors, from silver's raw Savant rows.

Two small gold tables per dt (MLB only; NPB has no Statcast):
  feature_pitch_daily    one row per pitcher x pitch type: pitches, swings,
                         whiffs, velocity and spin sums, and the wOBA / xwOBA
                         of the plate appearances that ended on that pitch type
  feature_contact_daily  one row per batter: wOBA and xwOBA sums over his PAs,
                         balls in play and their BABIP hits

xwOBA follows Savant: a ball in play counts its estimated_woba_using_speedangle,
any other PA-ending event (walk, HBP, strikeout) its woba_value. Only rows
whose woba_denom is 1 (the last pitch of a PA that counts for wOBA) add to
wOBA, xwOBA and their denominator.
"""
from __future__ import annotations

import json
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


def daily_rows(pitches: List[Dict[str, Any]], player_id, dt: date) -> Dict[str, List[Dict[str, Any]]]:
    """feature_pitch_daily and feature_contact_daily rows for one dt.
    player_id maps a source MLBAM id to the canonical player id."""
    by_pitch: Dict[tuple, Dict[str, Any]] = {}
    by_batter: Dict[str, Dict[str, Any]] = {}
    for p in pitches:
        if not p.get("game_pk"):
            continue
        try:
            raw = json.loads(p.get("raw_json") or "{}")
        except (TypeError, ValueError):
            raw = {}
        desc = p.get("description") or raw.get("description") or ""
        pitch_type = p.get("pitch_type") or raw.get("pitch_type") or ""
        pid = player_id(p.get("pitcher_id"))
        bid = player_id(p.get("batter_id"))
        denom = _f(raw.get("woba_denom")) or 0.0
        woba = _f(raw.get("woba_value")) or 0.0
        xw = _f(raw.get("estimated_woba_using_speedangle"))
        in_play = desc == "hit_into_play" or bool(raw.get("bb_type"))
        x_value = (xw if xw is not None else woba) if denom else 0.0
        if pid and pitch_type:
            r = by_pitch.setdefault((pid, pitch_type), {
                "player_id": pid, "pitch_type": pitch_type, "pitches": 0, "swings": 0, "whiffs": 0,
                "velo_sum": 0.0, "velo_n": 0, "spin_sum": 0.0, "spin_n": 0,
                "woba_value": 0.0, "xwoba_value": 0.0, "woba_den": 0})
            r["pitches"] += 1
            r["swings"] += desc in SWINGS
            r["whiffs"] += desc in WHIFFS
            velo = _f(p.get("release_speed")) if p.get("release_speed") is not None else _f(raw.get("release_speed"))
            if velo is not None:
                r["velo_sum"] += velo
                r["velo_n"] += 1
            spin = _f(raw.get("release_spin_rate"))
            if spin is not None:
                r["spin_sum"] += spin
                r["spin_n"] += 1
            if denom:
                r["woba_value"] += woba
                r["xwoba_value"] += x_value
                r["woba_den"] += int(denom)
        if bid and denom:
            b = by_batter.setdefault(bid, {"player_id": bid, "woba_value": 0.0, "xwoba_value": 0.0, "woba_den": 0,
                                           "xwoba_bip_n": 0, "bip": 0, "babip_hits": 0})
            b["woba_value"] += woba
            b["xwoba_value"] += x_value
            b["woba_den"] += int(denom)
            b["xwoba_bip_n"] += int(in_play and xw is not None)
            if in_play and (p.get("events") or raw.get("events")) != "home_run":
                b["bip"] += 1
                b["babip_hits"] += int((_f(raw.get("babip_value")) or 0) > 0)
    for r in list(by_pitch.values()) + list(by_batter.values()):
        r["league_code"] = "MLB"
        r["source"] = "mlb_statcast"
        for k in ("woba_value", "xwoba_value", "velo_sum", "spin_sum"):
            if k in r:
                r[k] = round(r[k], 6)
    return {"feature_pitch_daily": list(by_pitch.values()), "feature_contact_daily": list(by_batter.values())}


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
        logger.info("statcast features dt=%s: %d pitch-type rows, %d batter rows", d.isoformat(),
                    len(rows["feature_pitch_daily"]), len(rows["feature_contact_daily"]))
    return out
