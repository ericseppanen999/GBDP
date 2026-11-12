"""The "why" behind a storyline: which parts of a player's game changed.

A storyline says what happened (a .520 wOBA over two weeks, a new cutter, a
lucky season). Drivers say why, from Statcast components: chase rate,
contact in the zone, whiffs, hard-hit and barrel rates, exit velocity, launch
angle, batted-ball mix, pull rate, and results against fastballs, breaking
balls and offspeed pitches. Each component is compared between two spans
(recent vs before) or against the league, scored as a z-score against
sampling noise, and only the clearest changes are kept, so a writer can say
"he stopped chasing" only when the data says so.

Reads the views fbpd (feature_batter_pitch_daily) and fpd (feature_pitch_daily).
"""
from __future__ import annotations

import math
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple

Query = Callable[[str], List[Dict[str, Any]]]

SUMS = ("pitches", "swings", "whiffs", "zone_pitches", "zone_swings", "zone_whiffs", "chase_pitches",
        "chase_swings", "bip", "hard_hit", "barrels", "ev_sum", "ev_n", "la_sum", "la_n", "gb", "ld", "fb", "pu",
        "pulled", "pulled_air", "woba_value", "xwoba_value", "woba_den")

# (metric, label, kind, numerator, denominator, minimum denominator per side)
# kind "rate" is a proportion (binomial noise); "mean" is an average with a
# per-observation SD in the last slot's place (sd, n_key).
RATES: Tuple[tuple, ...] = (
    ("chase_rate", "chase rate (swings at pitches outside the zone)", "chase_swings", "chase_pitches", 60),
    ("zone_swing_rate", "swing rate on pitches in the zone", "zone_swings", "zone_pitches", 60),
    ("whiff_rate", "whiff rate (misses per swing)", "whiffs", "swings", 60),
    ("hard_hit_rate", "hard-hit rate (95+ mph balls in play)", "hard_hit", "bip", 25),
    ("barrel_rate", "barrel rate (per ball in play)", "barrels", "bip", 25),
    ("pull_rate", "pull rate (per ball in play)", "pulled", "bip", 25),
    ("pulled_air_rate", "pulled line drives and fly balls (per ball in play)", "pulled_air", "bip", 25),
    ("ground_ball_rate", "ground-ball rate", "gb", "bip", 25),
    ("fly_ball_rate", "fly-ball rate", "fb", "bip", 25),
)
MEANS: Tuple[tuple, ...] = (
    ("avg_exit_velo", "average exit velocity (mph)", "ev_sum", "ev_n", 15.0, 25),
    ("avg_launch_angle", "average launch angle (degrees)", "la_sum", "la_n", 25.0, 25),
)
GROUP_LABEL = {"fastball": "fastballs", "breaking": "breaking balls", "offspeed": "offspeed pitches"}


def _sums(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out = {k: 0.0 for k in SUMS}
    for r in rows:
        for k in SUMS:
            out[k] += float(r.get(k) or 0)
    return out


def _rate_z(x1, n1, x2, n2) -> float:
    p = (x1 + x2) / (n1 + n2)
    se = math.sqrt(max(p * (1 - p), 1e-9) * (1 / n1 + 1 / n2))
    return (x1 / n1 - x2 / n2) / se


def compare(a: Dict[str, float], b: Dict[str, float], prefix: str = "", label_suffix: str = "") -> List[Dict[str, Any]]:
    """Every component with enough data on both sides: a vs b, with a z-score."""
    out = []
    for metric, label, num, den, min_n in RATES:
        if a[den] >= min_n and b[den] >= min_n:
            out.append({"metric": prefix + metric, "label": label + label_suffix,
                        "since": round(a[num] / a[den], 4), "before": round(b[num] / b[den], 4),
                        "n_since": int(a[den]), "n_before": int(b[den]),
                        "z": round(_rate_z(a[num], a[den], b[num], b[den]), 2)})
    for metric, label, num, den, sd, min_n in MEANS:
        if a[den] >= min_n and b[den] >= min_n:
            ma, mb = a[num] / a[den], b[num] / b[den]
            out.append({"metric": prefix + metric, "label": label + label_suffix, "since": round(ma, 1),
                        "before": round(mb, 1), "n_since": int(a[den]), "n_before": int(b[den]),
                        "z": round((ma - mb) / (sd * math.sqrt(1 / a[den] + 1 / b[den])), 2)})
    if a["woba_den"] >= 20 and b["woba_den"] >= 20:
        xa, xb = a["xwoba_value"] / a["woba_den"], b["xwoba_value"] / b["woba_den"]
        out.append({"metric": prefix + "xwoba", "label": "xwOBA" + label_suffix, "since": round(xa, 3),
                    "before": round(xb, 3), "n_since": int(a["woba_den"]), "n_before": int(b["woba_den"]),
                    "z": round((xa - xb) / (0.5 * math.sqrt(1 / a["woba_den"] + 1 / b["woba_den"])), 2)})
    return out


def top(found: List[Dict[str, Any]], k: int = 3, min_abs_z: float = 2.0) -> List[Dict[str, Any]]:
    keep = [d for d in found if abs(d["z"]) >= min_abs_z]
    return sorted(keep, key=lambda d: -abs(d["z"]))[:k]


def _span_sql(table: str, where: str, split: date, start: date, end: date, extra_group: str = "") -> str:
    cols = ", ".join(f"sum({k}) AS {k}" for k in SUMS)
    group = f", {extra_group}" if extra_group else ""
    return f"""
        SELECT CASE WHEN dt >= '{split.isoformat()}' THEN 'since' ELSE 'before' END AS span{group}, {cols}
        FROM {table} WHERE {where} AND dt >= '{start.isoformat()}' AND dt <= '{end.isoformat()}'
        GROUP BY CASE WHEN dt >= '{split.isoformat()}' THEN 'since' ELSE 'before' END{group}"""


def batter_change(q: Query, player_id: str, split: date, as_of: date, k: int = 3) -> List[Dict[str, Any]]:
    """What changed in a batter's game since `split`, against the season before it."""
    start = date(as_of.year, 1, 1)
    rows = q(_span_sql("fbpd", f"player_id = '{player_id}'", split, start, as_of, "pitch_group"))
    spans = {s: _sums([r for r in rows if r["span"] == s]) for s in ("since", "before")}
    found = compare(spans["since"], spans["before"])
    for g, label in GROUP_LABEL.items():
        a = _sums([r for r in rows if r["span"] == "since" and r["pitch_group"] == g])
        b = _sums([r for r in rows if r["span"] == "before" and r["pitch_group"] == g])
        found += [d for d in compare(a, b, prefix=f"{g}_", label_suffix=f" against {label}")
                  if d["metric"].endswith(("xwoba", "whiff_rate", "hard_hit_rate", "chase_rate"))]
    return top(found, k)


def batter_vs_league(q: Query, player_id: str, as_of: date, k: int = 3) -> List[Dict[str, Any]]:
    """A batter's season against the league's: what his contact looks like
    behind a luck gap ("since" = him, "before" = the league)."""
    start = date(as_of.year, 1, 1)
    cols = ", ".join(f"sum({c}) AS {c}" for c in SUMS)
    rng = f"dt >= '{start.isoformat()}' AND dt <= '{as_of.isoformat()}'"
    me = q(f"SELECT {cols} FROM fbpd WHERE player_id = '{player_id}' AND {rng}")
    league = q(f"SELECT {cols} FROM fbpd WHERE {rng}")
    if not me or not league:
        return []
    found = [d for d in compare(_sums(me), _sums(league))
             if d["metric"] in ("hard_hit_rate", "barrel_rate", "avg_exit_velo", "avg_launch_angle", "pull_rate",
                                "ground_ball_rate", "fly_ball_rate", "xwoba")]
    for d in found:
        d["vs"] = "league"
    return top(found, k)


def pitcher_change(q: Query, player_id: str, split: date, as_of: date, pitch_type: Optional[str] = None,
                   k: int = 3) -> List[Dict[str, Any]]:
    """What changed in a pitcher's results since `split`: his whole line, and
    the named pitch's own whiff and chase rates when given."""
    start = date(as_of.year, 1, 1)
    rows = q(_span_sql("fpd", f"player_id = '{player_id}'", split, start, as_of, "pitch_type"))
    spans = {s: _sums([r for r in rows if r["span"] == s]) for s in ("since", "before")}
    found = [d for d in compare(spans["since"], spans["before"])
             if d["metric"] in ("chase_rate", "whiff_rate", "hard_hit_rate", "barrel_rate", "avg_exit_velo",
                                "ground_ball_rate", "xwoba")]
    for d in found:
        d["label"] = d["label"] + " (batters against him)"
    if pitch_type:
        mine = _sums([r for r in rows if r["span"] == "since" and r["pitch_type"] == pitch_type])
        rest = _sums([r for r in rows if r["span"] == "since" and r["pitch_type"] != pitch_type])
        for d in compare(mine, rest, prefix="new_pitch_", label_suffix=f" on his {pitch_type}, vs his other pitches since"):
            if d["metric"].endswith(("whiff_rate", "chase_rate", "xwoba")):
                d["vs"] = "his other pitches"
                found.append(d)
    return top(found, k)
