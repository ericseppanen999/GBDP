"""The breakout dataset: per-batter daily stats, window features and labels.

Gold computes these one dt at a time (gold/publish.py):

1. feature_batter_daily[dt]: each batter's counting stats from that day's
   fact_plate_appearance. Small (a few hundred rows a day), so later steps
   read windows of it instead of re-reading plate appearances.
2. ml_breakout_examples[as_of]: one row per batter with at least
   min_baseline_pa PAs in the baseline window.
   - Features use only days as_of - baseline_days + 1 .. as_of.
   - The label uses only days as_of + 1 .. as_of + label_days, and is
     filled once every one of those days has been published (null until
     then: label_status = pending).

A breakout: wOBA over the label window is at least margin_woba above wOBA
over the baseline window, with at least min_label_pa PAs in the label window.
"""
from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from gbdp.ml.config import BreakoutConfig

# fact_plate_appearance.event_type groups. They must equal the semantic
# layer's vocabularies.event_type.groups (tests/test_ml_breakout.py checks).
NOT_A_PA = frozenset({"TRUNCATED"})
AT_BAT = frozenset({"1B", "2B", "3B", "HR", "K", "OUT", "DP", "TP", "FC", "ERROR"})
HIT = frozenset({"1B", "2B", "3B", "HR"})
SAC_FLY = frozenset({"SF", "SAC"})  # SAC: legacy pre-rebuild code, counted as a fly (as the vocabulary does)
SAC_BUNT = frozenset({"SH"})

# Counting stats per batter per day, in feature_batter_daily and in every window sum.
DAILY_STATS: Tuple[str, ...] = (
    "pa", "ab", "h", "doubles", "triples", "hr", "bb", "hbp", "so", "sf", "sh", "woba_den", "woba_value",
)
# Summed over a window too: days with at least one PA.
WINDOW_STATS: Tuple[str, ...] = DAILY_STATS + ("days",)


def batter_daily_rows(pas: Iterable[Mapping[str, Any]], dt: date, cfg: BreakoutConfig,
                      source_league: Mapping[str, str]) -> List[Dict[str, Any]]:
    """One row per (batter, league) with that day's counting stats.

    wOBA's denominator is AB + BB + HBP + SF (intentional walks can't be told
    apart, so they count as walks). UNKNOWN outcomes are PAs but in neither
    wOBA term; TRUNCATED rows are not PAs at all.
    """
    weights = cfg.woba_weights
    sources = set(cfg.sources)
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in pas:
        source, batter = r.get("source"), r.get("batter_id")
        event = r.get("event_type")
        if source not in sources or not batter or event in NOT_A_PA:
            continue
        league = source_league.get(source)
        if league is None:
            continue
        s = out.get((batter, league))
        if s is None:
            s = out[(batter, league)] = {"player_id": batter, "league_code": league, "source": source,
                                         **{k: 0 for k in DAILY_STATS}}
            s["woba_value"] = 0.0
        s["pa"] += 1
        is_ab = event in AT_BAT
        s["ab"] += is_ab
        s["h"] += event in HIT
        s["doubles"] += event == "2B"
        s["triples"] += event == "3B"
        s["hr"] += event == "HR"
        s["bb"] += event == "BB"
        s["hbp"] += event == "HBP"
        s["so"] += event == "K"
        s["sf"] += event in SAC_FLY
        s["sh"] += event in SAC_BUNT
        s["woba_den"] += is_ab or event in ("BB", "HBP") or event in SAC_FLY
        s["woba_value"] += weights.get(str(event), 0.0)
    rows = []
    for key in sorted(out):
        row = out[key]
        row["woba_value"] = round(row["woba_value"], 6)
        row["dt"] = dt.isoformat()
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Windows
# ---------------------------------------------------------------------------

def feature_windows(as_of: date, cfg: BreakoutConfig) -> Dict[str, Tuple[date, date]]:
    """Inclusive date ranges the features of `as_of` read. All end at as_of."""
    return {
        "base": (as_of - timedelta(days=cfg.baseline_days - 1), as_of),
        "recent": (as_of - timedelta(days=cfg.recent_days - 1), as_of),
        "day": (as_of, as_of),
    }


def label_window(as_of: date, cfg: BreakoutConfig) -> Tuple[date, date]:
    """Inclusive date range the label of `as_of` reads: strictly after as_of."""
    return as_of + timedelta(days=1), as_of + timedelta(days=cfg.label_days)


def step_windows(dt: date, cfg: BreakoutConfig) -> Dict[str, Tuple[date, date]]:
    """Every window gold's step for `dt` sums: the features and label of the
    as-of dates it writes, dt (label filled if already complete) and
    dt - label_days (whose label window ends on dt)."""
    windows: Dict[str, Tuple[date, date]] = {}
    for prefix, as_of in (("d", dt), ("a", dt - timedelta(days=cfg.label_days))):
        for name, rng in feature_windows(as_of, cfg).items():
            windows[f"{prefix}_{name}"] = rng
        windows[f"{prefix}_label"] = label_window(as_of, cfg)
    return windows


def window_sum_sql(table: str, windows: Mapping[str, Tuple[date, date]]) -> str:
    """One pass over feature_batter_daily: every window's sums per (player, league).

    Plain SQL that DuckDB (parquet) and Spark (Delta) both run, so the two
    storage formats can't disagree. dt is a YYYY-MM-DD string in both.
    """
    lo = min(s for s, _ in windows.values()).isoformat()
    hi = max(e for _, e in windows.values()).isoformat()
    cols = []
    for name, (start, end) in sorted(windows.items()):
        cond = f"dt >= '{start.isoformat()}' AND dt <= '{end.isoformat()}'"
        for stat in DAILY_STATS:
            zero = "0.0" if stat == "woba_value" else "0"
            cols.append(f"SUM(CASE WHEN {cond} THEN {stat} ELSE {zero} END) AS {name}__{stat}")
        cols.append(f"SUM(CASE WHEN {cond} AND pa > 0 THEN 1 ELSE 0 END) AS {name}__days")
    return (
        f"SELECT player_id, league_code, {', '.join(cols)} FROM {table} "
        f"WHERE dt >= '{lo}' AND dt <= '{hi}' AND player_id IS NOT NULL GROUP BY player_id, league_code"
    )


def split_window_sums(row: Mapping[str, Any], windows: Iterable[str]) -> Dict[str, Dict[str, float]]:
    """{window: {stat: value}} from one row of window_sum_sql."""
    out: Dict[str, Dict[str, float]] = {}
    for w in windows:
        out[w] = {stat: (row.get(f"{w}__{stat}") or 0) for stat in WINDOW_STATS}
    return out


# ---------------------------------------------------------------------------
# Features and labels
# ---------------------------------------------------------------------------

def _ratio(num: float, den: float) -> Optional[float]:
    return round(num / den, 6) if den else None


def woba(s: Mapping[str, float]) -> Optional[float]:
    return _ratio(s["woba_value"], s["woba_den"])


def window_features(s: Mapping[str, float], prefix: str) -> Dict[str, Any]:
    babip_den = s["ab"] - s["so"] - s["hr"] + s["sf"]
    return {
        f"{prefix}_pa": int(s["pa"]),
        f"{prefix}_woba": woba(s),
        f"{prefix}_k_rate": _ratio(s["so"], s["pa"]),
        f"{prefix}_bb_rate": _ratio(s["bb"], s["pa"]),
        f"{prefix}_iso": _ratio(s["doubles"] + 2 * s["triples"] + 3 * s["hr"], s["ab"]),
        f"{prefix}_babip": _ratio(s["h"] - s["hr"], babip_den) if babip_den > 0 else None,
        f"{prefix}_days": int(s["days"]),
    }


def league_woba(players: Iterable[Tuple[str, Mapping[str, Mapping[str, float]]]], window: str) -> Dict[str, Optional[float]]:
    """{league_code: wOBA of every PA in the window}, from per-player window sums."""
    num: Dict[str, float] = {}
    den: Dict[str, float] = {}
    for league, sums in players:
        s = sums[window]
        num[league] = num.get(league, 0.0) + s["woba_value"]
        den[league] = den.get(league, 0.0) + s["woba_den"]
    return {lg: _ratio(num[lg], den[lg]) for lg in num}


# Every column an ml_breakout_examples row carries, in order. Features are
# f_*, labels label_*; the rest describe the row.
FEATURE_COLUMNS: Tuple[str, ...] = (
    "f_base_pa", "f_base_woba", "f_base_k_rate", "f_base_bb_rate", "f_base_iso", "f_base_babip", "f_base_days",
    "f_recent_pa", "f_recent_woba", "f_league_base_woba", "f_base_woba_vs_league",
    "f_day_pa", "f_day_ab", "f_day_h", "f_day_hr",
)
LABEL_COLUMNS: Tuple[str, ...] = (
    "label_status", "label_window_end", "label_pa", "label_woba", "label_woba_delta", "label_breakout",
)


def example_rows(as_of: date, players: List[Tuple[str, str, Dict[str, Dict[str, float]]]], prefix: str,
                 cfg: BreakoutConfig, label_complete: bool) -> List[Dict[str, Any]]:
    """ml_breakout_examples rows for one as-of date.

    players: (player_id, league_code, {window: sums}) with windows named
    "<prefix>_base", "<prefix>_recent", "<prefix>_day", "<prefix>_label".
    label_complete: every day of the label window has been published. When
    false, every label column is null and label_status is "pending", whatever
    partial data the window already holds.
    """
    leagues = league_woba(((lg, sums) for _, lg, sums in players), f"{prefix}_base")
    label_end = label_window(as_of, cfg)[1].isoformat()
    rows: List[Dict[str, Any]] = []
    for player_id, league, sums in sorted(players, key=lambda p: (p[1], p[0])):
        base = sums[f"{prefix}_base"]
        if base["pa"] < cfg.min_baseline_pa:
            continue
        f = window_features(base, "f_base")
        recent, day = sums[f"{prefix}_recent"], sums[f"{prefix}_day"]
        lg_woba = leagues.get(league)
        row: Dict[str, Any] = {
            "player_id": player_id,
            "league_code": league,
            "season_year": as_of.year,
            **f,
            "f_recent_pa": int(recent["pa"]),
            "f_recent_woba": woba(recent),
            "f_league_base_woba": lg_woba,
            "f_base_woba_vs_league": (round(f["f_base_woba"] - lg_woba, 6)
                                      if f["f_base_woba"] is not None and lg_woba is not None else None),
            "f_day_pa": int(day["pa"]),
            "f_day_ab": int(day["ab"]),
            "f_day_h": int(day["h"]),
            "f_day_hr": int(day["hr"]),
            "baseline_days": cfg.baseline_days,
            "recent_days": cfg.recent_days,
            "label_days": cfg.label_days,
        }
        row.update(label_values(f["f_base_woba"], sums[f"{prefix}_label"], cfg, label_complete))
        row["label_window_end"] = label_end
        rows.append(row)
    return rows


def label_values(base_woba: Optional[float], label: Mapping[str, float], cfg: BreakoutConfig,
                 complete: bool) -> Dict[str, Any]:
    out: Dict[str, Any] = {"label_status": "pending", "label_pa": None, "label_woba": None,
                           "label_woba_delta": None, "label_breakout": None}
    if not complete:
        return out
    pa = int(label["pa"])
    lw = woba(label)
    out.update(label_pa=pa, label_woba=lw)
    if lw is not None and base_woba is not None:
        out["label_woba_delta"] = round(lw - base_woba, 6)
    if pa < cfg.min_label_pa or out["label_woba_delta"] is None:
        out["label_status"] = "insufficient_pa"
        return out
    out["label_status"] = "labeled"
    # 1e-9: the stored values are rounded to 6 places; decide on what's stored.
    out["label_breakout"] = out["label_woba_delta"] >= cfg.margin_woba - 1e-9
    return out


# ---------------------------------------------------------------------------
# Model inputs
# ---------------------------------------------------------------------------

# What the model sees, computed from an example row's f_* columns. The model
# artifact lists these names; scoring refuses an artifact whose list differs.
MODEL_INPUTS: Tuple[str, ...] = (
    "base_woba_vs_league",   # how far above/below the league he has been (regression to the mean)
    "recent_minus_base_woba",  # last recent_days vs the baseline (a hot or cold streak)
    "base_k_rate",
    "base_bb_rate",
    "base_iso",
    "base_babip",            # luck on balls in play, which regresses hardest
    "log_base_pa",           # playing time: more PAs, a steadier baseline
    "recent_pa_share",       # recent PAs per baseline PA, scaled by window length (playing time trend)
    "is_npb",
)


def model_inputs(row: Mapping[str, Any]) -> List[Optional[float]]:
    """MODEL_INPUTS for one example row. None where the row can't say (the
    model imputes the training mean)."""
    base_woba, recent_woba = row.get("f_base_woba"), row.get("f_recent_woba")
    base_pa = row.get("f_base_pa") or 0
    base_days, recent_days = row.get("baseline_days") or 0, row.get("recent_days") or 0
    share = None
    if base_pa and recent_days:
        share = (row.get("f_recent_pa") or 0) / base_pa * base_days / recent_days
    return [
        row.get("f_base_woba_vs_league"),
        (recent_woba - base_woba) if recent_woba is not None and base_woba is not None else None,
        row.get("f_base_k_rate"),
        row.get("f_base_bb_rate"),
        row.get("f_base_iso"),
        row.get("f_base_babip"),
        math.log(base_pa) if base_pa > 0 else None,
        share,
        1.0 if row.get("league_code") == "NPB" else 0.0,
    ]
