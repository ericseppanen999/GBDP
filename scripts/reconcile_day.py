"""Reconcile one day of a local pipeline run, layer by layer, against the source APIs.

    python scripts/reconcile_day.py 2026-06-10 --data-root ./data

Fetches ground truth independently (MLB: statsapi schedule + linescore +
boxscores; NPB: the totals SPAIA embeds in every play-by-play event), then
checks bronze -> silver -> gold -> API. Prints PASS/FAIL per check and exits
1 on any FAIL. Reads a parquet data root (GBDP_STORAGE_FORMAT=parquet).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

import requests

HITS = {"1B", "2B", "3B", "HR"}
# A game is reconciled when statsapi's abstractGameState is "Final": that
# covers "Final", "Game Over", "Completed Early: Rain", "Final: Tied", ...
# (an exact detailedState list missed variants, CR-012e).
PLAYED = {"Final"}


class Report:
    def __init__(self) -> None:
        self.failed = 0

    def check(self, ok: bool, name: str, detail: str) -> None:
        self.failed += not ok
        print(f"{'PASS' if ok else 'FAIL'}  {name:44} {detail}", flush=True)


def _rows(root: Path, *parts: str) -> list:
    from gbdp.utils.io import read_parquet_rows

    path = root.joinpath(*parts)
    return read_parquet_rows(path) if path.exists() else []


def _files(root: Path, *parts: str) -> int:
    path = root.joinpath(*parts)
    return sum(1 for f in path.rglob("*") if f.is_file()) if path.exists() else 0


def mlb_truth(day: str) -> dict:
    s = requests.Session()
    s.headers["User-Agent"] = "gbdp-reconcile"
    sched = s.get("https://statsapi.mlb.com/api/v1/schedule",
                  params={"sportId": 1, "date": day, "hydrate": "linescore"}, timeout=30).json()
    games = {}
    for d in sched.get("dates", []):
        for g in d["games"]:
            ls = g.get("linescore", {}).get("teams", {})
            st = g["status"]
            # A postponed game is abstractGameState "Final" too (codedGameState
            # "D"), and its gamePk's boxscore is the makeup game's (823042: rained
            # out 2026-06-25, played 07-23), so it isn't a game played that day.
            not_played = st.get("codedGameState") in ("D", "C") or str(st.get("detailedState", "")).startswith(("Postponed", "Cancelled"))
            games[str(g["gamePk"])] = {
                "status": st.get("detailedState") if not_played else st.get("abstractGameState"),
                "detail": g["status"].get("detailedState"),
                "runs": (ls.get("home", {}).get("runs"), ls.get("away", {}).get("runs")),
                "hits": (ls.get("home", {}).get("hits"), ls.get("away", {}).get("hits")),
            }
    for pk, g in games.items():
        if g["status"] in PLAYED:
            box = s.get(f"https://statsapi.mlb.com/api/v1/game/{pk}/boxscore", timeout=30).json()
            bat = [box["teams"][side]["teamStats"]["batting"] for side in ("home", "away")]
            g["pa"] = tuple(b.get("plateAppearances") for b in bat)
    return games


def reconcile_mlb(root: Path, day: str, rep: Report) -> None:
    from gbdp.utils.ids import ulid_from_key

    truth = mlb_truth(day)
    final = {pk: g for pk, g in truth.items() if g["status"] in PLAYED}
    print(f"\nMLB {day}: source has {len(truth)} games, {len(final)} final")
    # Suspended games resume another day; their partial PAs are listed, not hidden.
    suspended = sorted(pk for pk, g in truth.items() if "Suspended" in str(g.get("detail")))
    if suspended:
        print(f"      suspended (not reconciled today): {suspended}")

    # Bronze: every entity fetched and parsed.
    for entity in ("schedule", "rosters", "boxscore", "player_bios"):
        n = _files(root, "bronze", "parsed", "mlb_statsapi", entity, f"dt={day}")
        rep.check(n > 0, f"bronze mlb_statsapi/{entity} parsed", f"{n} file(s)")
    rep.check(_files(root, "bronze", "parsed", "mlb_statcast", "pitches", f"dt={day}") > 0,
              "bronze mlb_statcast/pitches parsed", "")

    # Silver: one row per game, scores as the source has them.
    sg = {str(r["game_id"]): r for r in _rows(root, "silver", "mlb_statsapi", "games", f"dt={day}")}
    rep.check(set(sg) == set(truth), "silver games = source games", f"{len(sg)} vs {len(truth)}")
    bad = [pk for pk, g in final.items() if (sg.get(pk, {}).get("home_score"), sg.get(pk, {}).get("away_score")) != g["runs"]]
    rep.check(not bad, "silver final scores = source", f"mismatches: {bad[:5]}")
    pitches = _rows(root, "silver", "mlb_statcast", "pitches", f"dt={day}")
    by_game = Counter(str(p.get("game_pk")) for p in pitches if p.get("game_pk") is not None)
    missing = [pk for pk in final if not by_game.get(pk)]
    rep.check(not missing, "silver Statcast pitches for every final game", f"{len(pitches)} pitches; games without: {missing[:5]}")

    # Gold: games, PAs per team, runs and hits rebuilt from PAs.
    gid = {ulid_from_key(f"game:mlb_statsapi:{pk}"): pk for pk in truth}
    fg = {gid.get(r["game_id"]): r for r in _rows(root, "gold", "fact_game", f"dt={day}") if r["source"] == "mlb_statsapi"}
    rep.check(set(fg) == set(truth), "gold fact_game = source games", f"{len(fg)} vs {len(truth)}")
    teams_ok = all(r["home_team_id"] and r["away_team_id"] for r in fg.values())
    rep.check(teams_ok, "gold fact_game has both team IDs", "")
    pas = [p for p in _rows(root, "gold", "fact_plate_appearance", f"dt={day}") if p["source"] == "mlb_statcast"]
    agg = defaultdict(lambda: {"pa": [0, 0], "runs": [0, 0], "hits": [0, 0]})
    for p in pas:
        pk = gid.get(p["game_id"])
        side = 1 if p["is_top_inning"] else 0  # [home, away]; the away team bats in the top
        # A truncated PA (inning ended on the bases) isn't a PA in the box score.
        agg[pk]["pa"][side] += p["event_type"] != "TRUNCATED"
        agg[pk]["runs"][side] += p["runs_scored_on_play"] or 0
        agg[pk]["hits"][side] += p["event_type"] in HITS
    for key, label in (("pa", "PAs per team = boxscore"), ("runs", "runs from PAs = final score"), ("hits", "hits from PAs = linescore")):
        bad = [pk for pk, g in final.items() if tuple(agg[pk][key]) != tuple(g[key])]
        detail = "; ".join(f"{pk}: {tuple(agg[pk][key])} vs {g[key]}" for pk, g in final.items() if pk in bad[:4])
        rep.check(not bad, f"gold MLB {label}", f"{len(final) - len(bad)}/{len(final)} games" + (f"; {detail}" if bad else ""))
    unknown = sum(p["event_type"] == "UNKNOWN" for p in pas)
    rep.check(unknown == 0, "gold MLB PAs with UNKNOWN event", f"{unknown} of {len(pas)}")
    teams = sum(bool(p["batting_team_id"] and p["fielding_team_id"]) for p in pas)
    rep.check(teams == len(pas), "gold MLB PAs with both teams", f"{teams} of {len(pas)}")
    batters = sum(bool(p["batter_id"]) for p in pas)
    rep.check(batters == len(pas), "gold MLB PAs with a batter ID", f"{batters} of {len(pas)}")
    fp = _rows(root, "gold", "fact_pitch", f"dt={day}")
    rep.check(len(fp) == len(pitches), "gold fact_pitch = silver pitches", f"{len(fp)} vs {len(pitches)}")
    pa_ids = {p["pa_id"] for p in pas}
    orphans = sum(1 for p in fp if p["pa_id"] not in pa_ids)
    rep.check(orphans == 0, "every pitch belongs to a gold PA", f"{orphans} orphan pitch(es)")


def npb_truth(root: Path, day: str) -> dict:
    """Final scores from SPAIA's games feed, a separate endpoint from the
    play-by-play being checked (CR-012d); hits come from the PBP events'
    embedded box line, the only place SPAIA gives them."""
    finals = {}
    for g in _rows(root, "silver", "npb_spaia", "games", f"dt={day}"):
        if g.get("game_id") and g.get("game_state_name") == "試合終了":
            finals[str(g["game_id"])] = {"runs": (g.get("home_score"), g.get("away_score")), "hits": None,
                                         "state": g.get("game_state_name")}
    for r in _rows(root, "silver", "npb_spaia", "game_pbp", f"dt={day}"):
        raw = json.loads(r.get("raw_event_json") or "{}")
        gid = str(raw.get("GameID") or "")
        if gid in finals and finals[gid]["hits"] is None and raw.get("H_H") not in (None, ""):
            finals[gid]["hits"] = (int(raw["H_H"]), int(raw["V_H"]))
    return finals


def reconcile_npb(root: Path, day: str, rep: Report) -> None:
    from gbdp.utils.ids import ulid_from_key

    truth = npb_truth(root, day)
    print(f"\nNPB {day}: source has {len(truth)} games")
    for entity in ("games", "rosters", "game_pbp"):
        n = _files(root, "bronze", "parsed", "npb_spaia", entity, f"dt={day}")
        rep.check(n > 0, f"bronze npb_spaia/{entity} parsed", f"{n} file(s)")
    pbp_games = {str(r["game_id"]) for r in _rows(root, "silver", "npb_spaia", "game_pbp", f"dt={day}") if r.get("game_id")}
    no_pbp = sorted(set(truth) - pbp_games)
    rep.check(not no_pbp, "every played NPB game has play-by-play", f"{len(truth)} played; without PBP: {no_pbp[:5] or 'none'}")
    roster = _rows(root, "silver", "npb_spaia", "rosters", f"dt={day}")
    dupes = len(roster) - len({(r["team_id"], r["player_id"]) for r in roster})
    rep.check(dupes == 0 and len(roster) > 0, "silver rosters one row per team+player", f"{len(roster)} rows, {dupes} duplicate(s)")

    gid = {ulid_from_key(f"game:npb_spaia:{g}"): g for g in truth}
    pas = [p for p in _rows(root, "gold", "fact_plate_appearance", f"dt={day}") if p["source"] == "npb_spaia"]
    agg = defaultdict(lambda: {"runs": [0, 0], "hits": [0, 0]})
    for p in pas:
        side = 1 if p["is_top_inning"] else 0
        agg[gid.get(p["game_id"])]["runs"][side] += p["runs_scored_on_play"] or 0
        agg[gid.get(p["game_id"])]["hits"][side] += p["event_type"] in HITS
    for key, label in (("runs", "runs from PAs = final score"), ("hits", "hits from PAs = feed totals")):
        bad = [g for g, t in truth.items() if t[key] is not None and tuple(agg[g][key]) != tuple(t[key])]
        rep.check(not bad, f"gold NPB {label}", f"{len(truth) - len(bad)}/{len(truth)} games"
                  + (f"; {[(g, tuple(agg[g][key]), truth[g][key]) for g in bad[:3]]}" if bad else ""))
    per_game = len(pas) / max(1, len(truth))
    rep.check(60 <= per_game <= 95, "gold NPB PAs per game is baseball-shaped", f"{per_game:.1f}")
    unknown = sum(p["event_type"] == "UNKNOWN" for p in pas)
    rep.check(unknown == 0, "gold NPB PAs with UNKNOWN event", f"{unknown} of {len(pas)}")
    teams = sum(bool(p["batting_team_id"] and p["fielding_team_id"]) for p in pas)
    rep.check(teams == len(pas), "gold NPB PAs with both teams", f"{teams} of {len(pas)}")


def reconcile_gold_misc(root: Path, day: str, rep: Report) -> None:
    print("\nGold dimensions and derived tables")
    players = [r for r in _rows(root, "gold", "dim_player", f"dt={day}") if r.get("is_current")]
    mlb = [r for r in players if r["source"] == "mlb_statsapi"]
    filled = sum(bool(r.get("dob") and r.get("height_cm")) for r in mlb)
    rep.check(bool(mlb) and filled / len(mlb) >= 0.9, "dim_player MLB bios filled", f"{filled}/{len(mlb)}")
    ids = Counter(r["player_id"] for r in players)
    rep.check(max(ids.values(), default=1) == 1, "dim_player one current row per player", f"{len(players)} players")
    teams = [r for r in _rows(root, "gold", "dim_team", f"dt={day}") if r["source"] == "mlb_statsapi" and r.get("is_current")]
    named = sum(bool(r.get("team_abbrev") and r.get("home_city")) for r in teams)
    rep.check(len(teams) == 30 and named == 30, "dim_team: 30 MLB teams with abbrev + city", f"{named}/{len(teams)}")
    roster = _rows(root, "gold", "fact_roster", f"dt={day}")
    keys = Counter((r["team_id"], r["player_id"], r["source"]) for r in roster)
    rep.check(max(keys.values(), default=1) == 1, "fact_roster one row per team+player", f"{len(roster)} rows")
    re = _rows(root, "gold", "run_expectancy", f"dt={day}")
    leagues = {r["league_code"] for r in re}
    cells = Counter(r["league_code"] for r in re)
    rep.check({"MLB", "NPB"} <= leagues, "run_expectancy has MLB and NPB", f"cells per league: {dict(cells)}")
    empty0 = {r["league_code"]: r["exp_runs"] for r in re if r["base_state"] == 0 and r["outs"] == 0}
    sane = all(0.2 <= v <= 0.8 for v in empty0.values())
    rep.check(sane, "RE24 bases empty / 0 outs is plausible", f"{empty0}")


def reconcile_api(root: Path, day: str, rep: Report) -> None:
    from fastapi.testclient import TestClient

    from gbdp.serve import db
    from gbdp.serve.api import app

    print("\nAPI over the same gold")
    db._CACHE.clear()
    c = TestClient(app)
    games = c.get("/games", params={"date": day})
    gold_games = _rows(root, "gold", "fact_game", f"dt={day}")
    rep.check(games.status_code == 200 and len(games.json()) == len(gold_games),
              "GET /games?date returns every gold game", f"{len(games.json())} vs {len(gold_games)}")
    re = c.get("/run-expectancy", params={"league": "MLB", "season": int(day[:4])})
    rep.check(re.status_code == 200 and len(re.json()) > 0, "GET /run-expectancy?league=MLB", f"{len(re.json())} cells")
    q = c.post("/query", json={"sql": "SELECT count(*) AS n FROM fact_plate_appearance"})
    n = q.json()[0]["n"] if q.status_code == 200 else None
    gold_pas = len(_rows(root, "gold", "fact_plate_appearance", f"dt={day}"))
    rep.check(n == gold_pas, "POST /query counts match gold", f"{n} vs {gold_pas}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("day")
    ap.add_argument("--data-root", default=os.getenv("GBDP_DATA_ROOT", "data"))
    ap.add_argument("--leagues", default="mlb,npb")
    args = ap.parse_args()
    root = Path(args.data_root)
    os.environ["GBDP_DATA_ROOT"] = str(root)
    os.environ["GBDP_STORAGE_FORMAT"] = "parquet"
    rep = Report()
    if "mlb" in args.leagues:
        reconcile_mlb(root, args.day, rep)
    if "npb" in args.leagues:
        reconcile_npb(root, args.day, rep)
    reconcile_gold_misc(root, args.day, rep)
    reconcile_api(root, args.day, rep)
    print(f"\n{rep.failed} FAIL")
    return 1 if rep.failed else 0


if __name__ == "__main__":
    sys.exit(main())
