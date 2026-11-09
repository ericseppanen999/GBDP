"""Reconcile production gold against the leagues' own records for a whole date range.

    python scripts/reconcile_season.py --start 2026-05-13 --end 2026-11-05 [--gold-delta]

MLB truth comes from the Stats API (schedule linescores for runs and hits,
each game's boxscore for plate appearances), fetched live and independently of
anything the pipeline stored. NPB truth is the final score from SPAIA's games
feed (a separate endpoint from the play-by-play gold is rebuilt from) and the
hit totals SPAIA embeds in its play-by-play events. Gold's plate appearances
are summed per game and team and compared. Read-only; exits 1 on any mismatch.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import verify_lakehouse as vl  # noqa: E402
from reconcile_day import PLAYED, mlb_truth  # noqa: E402

HITS = "('1B', '2B', '3B', 'HR')"


def _days(start: str, end: str) -> list[str]:
    d, last = date.fromisoformat(start), date.fromisoformat(end)
    out = []
    while d <= last:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _gold_by_game(wh, source: str, start: str, end: str) -> dict:
    rows = wh.query(f"""
        SELECT g.source_game_id, pa.is_top_inning,
               count_if(pa.event_type <> 'TRUNCATED'), coalesce(sum(pa.runs_scored_on_play), 0),
               count_if(pa.event_type IN {HITS})
        FROM {vl.GOLD}.fact_plate_appearance pa
        JOIN {vl.GOLD}.fact_game g ON g.game_id = pa.game_id AND g.dt = pa.dt
        WHERE pa.dt BETWEEN '{start}' AND '{end}' AND pa.source = '{source}'
        GROUP BY 1, 2""")
    got: dict = {}
    for gid, top, pas, runs, hits in rows:
        side = 1 if str(top).lower() == "true" else 0  # [home, away]
        for key, v in (("pa", pas), ("runs", runs), ("hits", hits)):
            got.setdefault(str(gid), {}).setdefault(key, [0, 0])[side] = int(v)
    return got


def _compare(rep, league: str, truth: dict, got: dict, keys) -> dict:
    misses = {}
    for key in keys:
        bad = [g for g, t in truth.items()
               if t.get(key) is not None and tuple(got.get(g, {}).get(key, [0, 0])) != tuple(t[key])]
        checked = sum(1 for t in truth.values() if t.get(key) is not None)
        misses[key] = bad
        rep.line("PASS" if not bad else "FAIL", f"{league} {key} per team = source", f"{checked - len(bad)}/{checked} games"
                 + (f"; e.g. {[(g, tuple(got.get(g, {}).get(key, [0, 0])), tuple(truth[g][key])) for g in bad[:3]]}" if bad else ""))
    return misses


def reconcile_mlb(wh, rep, start: str, end: str) -> None:
    truth, suspended = {}, []
    for day in _days(start, end):
        for attempt in range(3):
            try:
                games = mlb_truth(day)
                break
            except Exception:  # transient API error
                if attempt == 2:
                    raise
                time.sleep(5)
        for pk, g in games.items():
            if "Suspended" in str(g.get("detail")):
                suspended.append(pk)
            if g["status"] in PLAYED:
                truth[pk] = g
    got = _gold_by_game(wh, "mlb_statcast", start, end)
    print(f"MLB: {len(truth)} final games in the source, {len(got)} with plate appearances in gold; suspended: {sorted(set(suspended)) or 'none'}")
    _compare(rep, "MLB", truth, got, ("pa", "runs", "hits"))


def reconcile_npb(wh, rep, start: str, end: str) -> None:
    rng = f"dt BETWEEN '{start}' AND '{end}'"
    finals = {str(g): {"runs": (int(h), int(a)), "hits": None}
              for g, h, a in wh.query(f"""
                SELECT game_id, max_by(home_score, dt), max_by(away_score, dt) FROM {vl._silver('npb_spaia', 'games')}
                WHERE {rng} AND game_id IS NOT NULL AND game_state_name = '試合終了' GROUP BY game_id""")}
    for gid, hh, vh in wh.query(f"""
            SELECT get_json_object(raw_event_json, '$.GameID'),
                   max(CAST(get_json_object(raw_event_json, '$.H_H') AS INT)),
                   max(CAST(get_json_object(raw_event_json, '$.V_H') AS INT))
            FROM {vl._silver('npb_spaia', 'game_pbp')} WHERE {rng} GROUP BY 1"""):
        if str(gid) in finals and hh is not None and vh is not None:
            finals[str(gid)]["hits"] = (int(hh), int(vh))
    got = _gold_by_game(wh, "npb_spaia", start, end)
    print(f"NPB: {len(finals)} finished games in the games feed, {len(got)} with plate appearances in gold")
    no_pas = sorted(set(finals) - set(got))
    rep.line("PASS" if not no_pas else "FAIL", "NPB every finished game has PAs", f"missing: {no_pas[:5] or 'none'}")
    misses = _compare(rep, "NPB", finals, got, ("runs", "hits"))
    if misses.get("runs"):
        print("  NPB run mismatches (game, gold [home, away], final score):")
        for g in misses["runs"][:20]:
            print(f"    {g} {got.get(g, {}).get('runs')} {finals[g]['runs']}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", default="2026-05-13")
    ap.add_argument("--end", default="2026-11-05")
    ap.add_argument("--leagues", default="mlb,npb")
    ap.add_argument("--gold-delta", action="store_true",
                    help="read gold from its Delta tables on the Volume, not the UC tables")
    ap.add_argument("--warehouse", default=os.getenv("GBDP_WAREHOUSE_ID", "a9d3d2589a30ab67"))
    ap.add_argument("--profile", default=os.getenv("DATABRICKS_CONFIG_PROFILE"))
    args = ap.parse_args()
    if args.gold_delta:
        vl.GOLD = vl._GOLD_DELTA
    wh, rep = vl.Warehouse(args.warehouse, args.profile), vl.Report()
    leagues = set(args.leagues.split(","))
    if "mlb" in leagues:
        reconcile_mlb(wh, rep, args.start, args.end)
    if "npb" in leagues:
        reconcile_npb(wh, rep, args.start, args.end)
    print(f"\n{rep.failed} FAIL, {rep.warned} WARN")
    return 1 if rep.failed else 0


if __name__ == "__main__":
    sys.exit(main())
