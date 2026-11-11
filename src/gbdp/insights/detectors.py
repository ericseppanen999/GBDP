"""Insight detectors over gold, as of one date.

Each detector runs SQL over two views the caller registers:
  fbd     feature_batter_daily rows of the season up to the as-of date
          (player_id, league_code, pa, ab, h, doubles, triples, hr, bb, hbp, so,
          sf, sh, woba_den, woba_value, dt)
  players one row per player_id with primary_name (dim_player's newest snapshot)
and returns candidate dicts: the pattern, its numbers, sample size, a score
(bigger = stronger), and `evidence_sql`, a query over gold that re-derives
the headline number so a writer (or a fact-checker) can verify it.

The SQL uses only what DuckDB and Spark SQL share; dates are inlined literals.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date, timedelta
from typing import Any, Callable, Dict, List

Query = Callable[[str], List[Dict[str, Any]]]

RATES = {
    "k_rate": ("so", "pa", "strikeout rate"),
    "bb_rate": ("bb", "pa", "walk rate"),
    "hr_rate": ("hr", "pa", "home runs per PA"),
}


def _id(*parts: Any) -> str:
    return hashlib.sha1("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:20]


def _candidate(detector: str, as_of: date, league: str, subject_type: str, subject_id: str, subject_name: str,
               metric: str, value: float, baseline: float | None, sample_size: int, score: float,
               facts: Dict[str, Any], evidence_sql: str) -> Dict[str, Any]:
    return {
        "insight_id": _id(detector, as_of, league, subject_id, metric),
        "detector": detector,
        "league_code": league,
        "subject_type": subject_type,
        "subject_id": subject_id,
        "subject_name": subject_name,
        "metric": metric,
        "value": round(float(value), 4),
        "baseline": None if baseline is None else round(float(baseline), 4),
        "delta": None if baseline is None else round(float(value) - float(baseline), 4),
        "sample_size": int(sample_size),
        "score": round(float(score), 3),
        "facts_json": json.dumps(facts, ensure_ascii=False, sort_keys=True, default=str),
        "evidence_sql": evidence_sql.strip(),
    }


def hitting_streaks(q: Query, as_of: date, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Batters whose latest games are a run of games with a hit."""
    rows = q(f"""
        WITH games AS (
          SELECT player_id, league_code, dt, CASE WHEN h > 0 THEN 1 ELSE 0 END AS hit
          FROM fbd WHERE pa > 0 AND dt <= '{as_of.isoformat()}'),
        islands AS (
          SELECT player_id, league_code, dt, hit,
                 ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY dt)
                 - ROW_NUMBER() OVER (PARTITION BY player_id, hit ORDER BY dt) AS grp
          FROM games),
        last_game AS (SELECT player_id, max(dt) AS last_dt FROM games GROUP BY player_id),
        runs AS (
          SELECT i.player_id, i.league_code, min(i.dt) AS start_dt, max(i.dt) AS end_dt, count(*) AS games
          FROM islands i WHERE i.hit = 1 GROUP BY i.player_id, i.league_code, i.grp)
        SELECT r.player_id, r.league_code, r.start_dt, r.end_dt, r.games, p.primary_name
        FROM runs r JOIN last_game l ON l.player_id = r.player_id AND l.last_dt = r.end_dt
        LEFT JOIN players p ON p.player_id = r.player_id
        WHERE r.games >= {int(cfg['min_games'])}""")
    out = []
    for r in rows:
        evidence = f"""
            SELECT dt, pa, h FROM {{{{table:feature_batter_daily}}}}
            WHERE player_id = '{r['player_id']}' AND dt BETWEEN '{r['start_dt']}' AND '{r['end_dt']}' AND pa > 0
            ORDER BY dt"""
        out.append(_candidate(
            "hitting_streak", as_of, r["league_code"], "player", r["player_id"], r.get("primary_name") or "",
            "hitting_streak_games", r["games"], None, r["games"], float(r["games"]),
            {"games": r["games"], "start_dt": str(r["start_dt"]), "end_dt": str(r["end_dt"]),
             "active": True}, evidence))
    return out


def change_vs_baseline(q: Query, as_of: date, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """wOBA over the last N days vs the same batter's season before them.
    The score is a z-score, so a big swing on few PAs doesn't make the list;
    it is still mostly regression to the mean, which the facts say."""
    recent_start = as_of - timedelta(days=int(cfg["recent_days"]) - 1)
    season_start = date(as_of.year, 1, 1)
    rows = q(f"""
        SELECT f.player_id, f.league_code, max(p.primary_name) AS primary_name,
               sum(CASE WHEN f.dt >= '{recent_start.isoformat()}' THEN f.pa ELSE 0 END) AS recent_pa,
               sum(CASE WHEN f.dt >= '{recent_start.isoformat()}' THEN f.woba_value ELSE 0 END) AS recent_wv,
               sum(CASE WHEN f.dt >= '{recent_start.isoformat()}' THEN f.woba_den ELSE 0 END) AS recent_wd,
               sum(CASE WHEN f.dt < '{recent_start.isoformat()}' THEN f.pa ELSE 0 END) AS base_pa,
               sum(CASE WHEN f.dt < '{recent_start.isoformat()}' THEN f.woba_value ELSE 0 END) AS base_wv,
               sum(CASE WHEN f.dt < '{recent_start.isoformat()}' THEN f.woba_den ELSE 0 END) AS base_wd
        FROM fbd f LEFT JOIN players p ON p.player_id = f.player_id
        WHERE f.dt >= '{season_start.isoformat()}' AND f.dt <= '{as_of.isoformat()}'
        GROUP BY f.player_id, f.league_code""")
    sd = float(cfg["woba_sd_per_pa"])
    out = []
    for r in rows:
        if (r["recent_pa"] or 0) < cfg["min_recent_pa"] or (r["base_pa"] or 0) < cfg["min_baseline_pa"]:
            continue
        if not r["recent_wd"] or not r["base_wd"]:
            continue
        recent = r["recent_wv"] / r["recent_wd"]
        base = r["base_wv"] / r["base_wd"]
        se = sd * math.sqrt(1 / r["recent_wd"] + 1 / r["base_wd"])
        z = (recent - base) / se
        if abs(z) < cfg["min_abs_z"]:
            continue
        evidence = f"""
            SELECT CASE WHEN dt >= '{recent_start.isoformat()}' THEN 'recent' ELSE 'baseline' END AS span,
                   sum(pa) AS pa, sum(woba_value) / sum(woba_den) AS woba
            FROM {{{{table:feature_batter_daily}}}}
            WHERE player_id = '{r['player_id']}' AND dt BETWEEN '{season_start.isoformat()}' AND '{as_of.isoformat()}'
            GROUP BY 1"""
        out.append(_candidate(
            "change_vs_baseline", as_of, r["league_code"], "player", r["player_id"], r.get("primary_name") or "",
            "woba", recent, base, int(r["recent_pa"]), abs(z),
            {"direction": "up" if z > 0 else "down", "recent_days": int(cfg["recent_days"]),
             "recent_pa": int(r["recent_pa"]), "baseline_pa": int(r["base_pa"]), "z": round(z, 2),
             "caveat": "Short-window swings mostly regress toward the season baseline."}, evidence))
    return out


def _league_months(q: Query, as_of: date) -> List[Dict[str, Any]]:
    season_start = date(as_of.year, 1, 1)
    return q(f"""
        SELECT league_code, substr(dt, 1, 7) AS month, sum(pa) AS pa, sum(so) AS so, sum(bb) AS bb,
               sum(hr) AS hr, sum(woba_value) AS wv, sum(woba_den) AS wd
        FROM fbd WHERE dt >= '{season_start.isoformat()}' AND dt <= '{as_of.isoformat()}'
        GROUP BY league_code, substr(dt, 1, 7)""")


def _rate_z(x1: float, n1: float, x2: float, n2: float) -> float:
    p1, p2 = x1 / n1, x2 / n2
    pooled = (x1 + x2) / (n1 + n2)
    se = math.sqrt(max(pooled * (1 - pooled), 1e-12) * (1 / n1 + 1 / n2))
    return (p1 - p2) / se


def league_month_shift(q: Query, as_of: date, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """A league's month vs its own previous month, for K%, BB% and HR/PA."""
    by_league: Dict[str, List[Dict[str, Any]]] = {}
    for r in _league_months(q, as_of):
        if (r["pa"] or 0) >= cfg["min_month_pa"]:
            by_league.setdefault(r["league_code"], []).append(r)
    out = []
    for league, months in by_league.items():
        months.sort(key=lambda r: r["month"])
        for prev, cur in zip(months, months[1:]):
            for metric, (num, den, label) in RATES.items():
                z = _rate_z(cur[num], cur[den], prev[num], prev[den])
                if abs(z) < cfg["min_abs_z"]:
                    continue
                evidence = f"""
                    SELECT substr(dt, 1, 7) AS month, sum({num}) * 1.0 / sum({den}) AS {metric}, sum(pa) AS pa
                    FROM {{{{table:feature_batter_daily}}}}
                    WHERE league_code = '{league}' AND substr(dt, 1, 7) IN ('{prev['month']}', '{cur['month']}')
                    GROUP BY 1 ORDER BY 1"""
                out.append(_candidate(
                    "league_month_shift", as_of, league, "league", league, league, metric,
                    cur[num] / cur[den], prev[num] / prev[den], int(cur["pa"]), abs(z),
                    {"label": label, "month": cur["month"], "previous_month": prev["month"],
                     "month_pa": int(cur["pa"]), "previous_pa": int(prev["pa"]), "z": round(z, 2)}, evidence))
    return out


def cross_league_gap(q: Query, as_of: date, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Season-to-date MLB vs NPB, for K%, BB% and HR/PA: the comparison no
    single league's own feed can make."""
    season: Dict[str, Dict[str, float]] = {}
    for r in _league_months(q, as_of):
        s = season.setdefault(r["league_code"], {"pa": 0, "so": 0, "bb": 0, "hr": 0})
        for k in s:
            s[k] += r[k] or 0
    leagues = sorted(lg for lg, s in season.items() if s["pa"] >= cfg["min_season_pa"])
    out = []
    for i, a in enumerate(leagues):
        for b in leagues[i + 1:]:
            for metric, (num, den, label) in RATES.items():
                z = _rate_z(season[a][num], season[a][den], season[b][num], season[b][den])
                if abs(z) < cfg["min_abs_z"]:
                    continue
                evidence = f"""
                    SELECT league_code, sum({num}) * 1.0 / sum({den}) AS {metric}, sum(pa) AS pa
                    FROM {{{{table:feature_batter_daily}}}}
                    WHERE league_code IN ('{a}', '{b}') AND dt BETWEEN '{as_of.year}-01-01' AND '{as_of.isoformat()}'
                    GROUP BY 1"""
                out.append(_candidate(
                    "cross_league_gap", as_of, f"{a}-{b}", "league_pair", f"{a}-{b}", f"{a} vs {b}", metric,
                    season[a][num] / season[a][den], season[b][num] / season[b][den],
                    int(min(season[a]["pa"], season[b]["pa"])), abs(z),
                    {"label": label, "leagues": [a, b], "pa": {a: int(season[a]["pa"]), b: int(season[b]["pa"])},
                     "z": round(z, 2), "season": as_of.year}, evidence))
    return out


DETECTORS = {
    "hitting_streak": hitting_streaks,
    "change_vs_baseline": change_vs_baseline,
    "league_month_shift": league_month_shift,
    "cross_league_gap": cross_league_gap,
}


def detect(q: Query, as_of: date, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every detector's candidates, best first within each detector, capped at
    cfg['top_per_detector']."""
    out: List[Dict[str, Any]] = []
    for name, fn in DETECTORS.items():
        found = sorted(fn(q, as_of, cfg[name]), key=lambda c: (-c["score"], c["insight_id"]))
        for rank, c in enumerate(found[: int(cfg["top_per_detector"])], 1):
            c["rank"] = rank
            c["definition_version"] = cfg["definition_version"]
            out.append(c)
    return out


def recompute(candidate: Dict[str, Any], evidence_rows: List[Dict[str, Any]]) -> Dict[str, float | None]:
    """The candidate's value (and baseline) recomputed from its evidence_sql
    rows, so a stated number can be checked against gold."""
    det, metric = candidate["detector"], candidate["metric"]
    if det == "hitting_streak":
        return {"value": float(sum(1 for r in evidence_rows if (r.get("h") or 0) > 0)), "baseline": None}
    if det == "change_vs_baseline":
        by = {r["span"]: r for r in evidence_rows}
        return {"value": by.get("recent", {}).get("woba"), "baseline": by.get("baseline", {}).get("woba")}
    if det == "league_month_shift":
        facts = json.loads(candidate["facts_json"])
        by = {r["month"]: r[metric] for r in evidence_rows}
        return {"value": by.get(facts["month"]), "baseline": by.get(facts["previous_month"])}
    if det == "cross_league_gap":
        a, b = json.loads(candidate["facts_json"])["leagues"]
        by = {r["league_code"]: r[metric] for r in evidence_rows}
        return {"value": by.get(a), "baseline": by.get(b)}
    raise ValueError(f"unknown detector {det!r}")
