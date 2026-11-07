"""Read-only health check of the live lakehouse, run from a workstation.

Queries the Unity Catalog tables through a SQL warehouse (via the `databricks`
CLI's auth), so it needs no deploy and is safe to run while jobs are running.

    python scripts/verify_lakehouse.py [--warehouse ID] [--profile NAME] [--start 2026-05-13]

Prints one line per check (PASS / WARN / FAIL) and exits 1 if anything FAILs.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import date

CATALOG = "gbdp"
GOLD = f"{CATALOG}.gold_gbdp"
SILVER_VOL = f"/Volumes/{CATALOG}/silver_gbdp/silver_vol/gbdp/silver"

# MLB regular season + postseason. Outside it, empty days are expected.
SEASON_MONTHS = range(4, 10)


class Warehouse:
    def __init__(self, warehouse_id: str, profile: str | None) -> None:
        self.warehouse_id = warehouse_id
        self.profile = profile

    def _api(self, *args: str, body: dict | None = None) -> dict:
        cmd = ["databricks", "api", *args]
        if self.profile:
            cmd += ["--profile", self.profile]
        path = None
        if body is not None:
            fd, path = tempfile.mkstemp(suffix=".json")
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(body, f)
            cmd += ["--json", "@" + path]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
        finally:
            if path:
                os.unlink(path)
        if r.returncode:
            raise RuntimeError((r.stderr or r.stdout).strip()[:500])
        return json.loads(r.stdout)

    def query(self, sql: str) -> list[list]:
        s = self._api("post", "/api/2.0/sql/statements", body={
            "warehouse_id": self.warehouse_id, "statement": sql,
            "wait_timeout": "50s", "on_wait_timeout": "CONTINUE",
        })
        while s["status"]["state"] in ("PENDING", "RUNNING"):
            time.sleep(3)
            s = self._api("get", f"/api/2.0/sql/statements/{s['statement_id']}")
        if s["status"]["state"] != "SUCCEEDED":
            raise RuntimeError(json.dumps(s["status"].get("error", s["status"]))[:500])
        rows = s.get("result", {}).get("data_array") or []
        # Results over one chunk are paged.
        nxt = s.get("result", {}).get("next_chunk_internal_link")
        while nxt:
            chunk = self._api("get", nxt)
            rows += chunk.get("data_array") or []
            nxt = chunk.get("next_chunk_internal_link")
        return rows


class Report:
    def __init__(self) -> None:
        self.failed = 0
        self.warned = 0

    def line(self, status: str, name: str, detail: str) -> None:
        if status == "FAIL":
            self.failed += 1
        elif status == "WARN":
            self.warned += 1
        print(f"{status:4}  {name:34} {detail}", flush=True)


def _f(x) -> float:
    return float(x) if x is not None else 0.0


def check_daily_counts(wh: Warehouse, rep: Report, start: str) -> None:
    rows = wh.query(f"""
        WITH g AS (SELECT dt, count(*) n FROM {GOLD}.fact_game WHERE dt >= '{start}' GROUP BY dt),
             p AS (SELECT dt, count(*) n FROM {GOLD}.fact_plate_appearance WHERE dt >= '{start}' GROUP BY dt),
             x AS (SELECT dt, count(*) n FROM {GOLD}.fact_pitch WHERE dt >= '{start}' GROUP BY dt)
        SELECT coalesce(g.dt, p.dt, x.dt) dt, coalesce(g.n, 0), coalesce(p.n, 0), coalesce(x.n, 0)
        FROM g FULL OUTER JOIN p ON g.dt = p.dt FULL OUTER JOIN x ON coalesce(g.dt, p.dt) = x.dt
        ORDER BY 1""")
    empty = [r[0] for r in rows if date.fromisoformat(r[0]).month in SEASON_MONTHS and (int(r[1]) == 0 or int(r[2]) == 0 or int(r[3]) == 0)]
    total = len(rows)
    detail = f"{total} dates; in-season dates missing games/PAs/pitches: {empty or 'none'}"
    rep.line("FAIL" if empty else "PASS", "daily games / PAs / pitches", detail)


def check_pa_per_game(wh: Warehouse, rep: Report, start: str) -> None:
    rows = wh.query(f"""
        WITH games AS (SELECT dt, game_id FROM {GOLD}.fact_game WHERE dt >= '{start}' AND source = 'mlb_statsapi'),
             pa AS (SELECT dt, game_id, count(*) n FROM {GOLD}.fact_plate_appearance WHERE dt >= '{start}' GROUP BY dt, game_id)
        SELECT count(*), count(pa.n), avg(pa.n), min(pa.n)
        FROM games LEFT JOIN pa ON games.dt = pa.dt AND games.game_id = pa.game_id""")
    games, with_pa, avg_pa, min_pa = rows[0]
    games, with_pa = int(games), int(with_pa)
    if games == 0:
        rep.line("WARN", "MLB plate appearances per game", "no MLB games in range")
        return
    cover = with_pa / games
    detail = f"{with_pa}/{games} MLB games have PAs ({cover:.0%}), avg {_f(avg_pa):.1f}, min {min_pa}"
    # A 9-inning game is ~75 PAs; postponed games legitimately have none.
    status = "PASS" if cover >= 0.9 and _f(avg_pa) >= 60 else ("WARN" if cover >= 0.7 else "FAIL")
    rep.line(status, "MLB plate appearances per game", detail)


def check_pa_teams(wh: Warehouse, rep: Report, start: str) -> None:
    rows = wh.query(f"""
        SELECT source, count(*), avg(CASE WHEN batting_team_id IS NOT NULL AND fielding_team_id IS NOT NULL THEN 1 ELSE 0 END)
        FROM {GOLD}.fact_plate_appearance WHERE dt >= '{start}' GROUP BY source ORDER BY 1""")
    for source, n, rate in rows:
        rate = _f(rate)
        rep.line("PASS" if rate >= 0.95 else "FAIL", f"PA teams filled ({source})", f"{rate:.1%} of {n} PAs")


def check_uniqueness(wh: Warehouse, rep: Report, start: str) -> None:
    keys = {
        "fact_game": "game_id",
        "fact_plate_appearance": "pa_id",
        "fact_pitch": "pitch_id",
        "fact_roster": "team_id, player_id, source",
        "dim_player": "player_id, valid_from_dt",
        "dim_team": "team_id, valid_from_dt",
    }
    for table, key in keys.items():
        (total, dupes), = wh.query(f"""
            SELECT coalesce(sum(n), 0), coalesce(sum(n - 1), 0) FROM (
              SELECT count(*) n FROM {GOLD}.{table} WHERE dt >= '{start}' GROUP BY dt, {key})""")
        total, dupes = int(total), int(dupes)
        rep.line("FAIL" if dupes else "PASS", f"unique ({key}) in {table}", f"{dupes} duplicate rows of {total}")


def check_dim_player_current(wh: Warehouse, rep: Report) -> None:
    (players, multi), = wh.query(f"""
        SELECT count(*), count_if(n > 1) FROM (
          SELECT player_id, count(*) n FROM {GOLD}.dim_player
          WHERE dt = (SELECT max(dt) FROM {GOLD}.dim_player) AND is_current GROUP BY player_id)""")
    rep.line("FAIL" if int(multi) else "PASS", "dim_player one current row", f"{multi} of {players} players have >1 current row")


def check_bio_fill(wh: Warehouse, rep: Report) -> None:
    rows = wh.query(f"""
        SELECT source, count(*),
               avg(CASE WHEN dob IS NOT NULL THEN 1 ELSE 0 END),
               avg(CASE WHEN bats IS NOT NULL AND bats <> 'UNK' THEN 1 ELSE 0 END),
               avg(CASE WHEN throws IS NOT NULL AND throws <> 'UNK' THEN 1 ELSE 0 END),
               avg(CASE WHEN height_cm IS NOT NULL THEN 1 ELSE 0 END),
               avg(CASE WHEN weight_kg IS NOT NULL THEN 1 ELSE 0 END),
               avg(CASE WHEN nationality IS NOT NULL THEN 1 ELSE 0 END),
               avg(CASE WHEN alternate_names IS NOT NULL AND alternate_names NOT IN ('', '[]') THEN 1 ELSE 0 END)
        FROM {GOLD}.dim_player
        WHERE dt = (SELECT max(dt) FROM {GOLD}.dim_player) AND is_current
        GROUP BY source ORDER BY 2 DESC""")
    for source, n, *rates in rows:
        dob, bats, throws, height, weight, nat, alt = (_f(r) for r in rates)
        detail = (f"{n} players: dob {dob:.0%} bats {bats:.0%} throws {throws:.0%} "
                  f"height {height:.0%} weight {weight:.0%} nationality {nat:.0%} alt_names {alt:.0%}")
        if source.startswith("mlb"):
            # The MLB season player list covers 40-man rosters, not every minor leaguer.
            status = "PASS" if min(dob, bats, height) >= 0.9 else ("WARN" if dob >= 0.5 else "FAIL")
        elif source.startswith("npb"):
            status = "PASS" if alt >= 0.9 else "WARN"
        else:
            status = "PASS"
        rep.line(status, f"dim_player bios ({source})", detail)


def check_roster_size(wh: Warehouse, rep: Report, start: str) -> None:
    (lo, hi, avg), = wh.query(f"""
        SELECT min(n), max(n), avg(n) FROM (
          SELECT dt, count(*) n FROM {GOLD}.fact_roster WHERE dt >= '{start}' GROUP BY dt)""")
    # ~30 MLB teams x 26-40 + 12 NPB teams x ~70 is a few thousand, not 100k+.
    status = "PASS" if hi is not None and int(hi) < 20000 else "FAIL"
    rep.line(status, "fact_roster rows per date", f"min {lo}, max {hi}, avg {_f(avg):.0f}")


def _silver(src: str, entity: str) -> str:
    # mergeSchema: empty days are written as a one-row {dt, empty} placeholder,
    # and a plain parquet read can pick that as the schema for every partition.
    return f"read_files('{SILVER_VOL}/{src}/{entity}', format => 'parquet', mergeSchema => true)"


def check_silver_statcast(wh: Warehouse, rep: Report, start: str) -> None:
    rows = wh.query(f"""
        SELECT dt, count(game_pk) FROM {_silver('mlb_statcast', 'pitches')}
        WHERE dt >= '{start}' GROUP BY dt ORDER BY dt""")
    # A full MLB slate is ~4,000 pitches; even a 2-game day is several hundred.
    thin = [r[0] for r in rows if date.fromisoformat(r[0]).month in SEASON_MONTHS and int(r[1]) < 200]
    shown = ", ".join(thin[:8]) + (f" (+{len(thin) - 8} more)" if len(thin) > 8 else "")
    rep.line("FAIL" if thin else "PASS", "silver Statcast pitches per date",
             f"{len(rows)} dates; in-season dates under 200 pitches: {shown or 'none'}")


def check_silver_duplicates(wh: Warehouse, rep: Report, start: str) -> None:
    for src, entity, key in [
        ("mlb_statsapi", "games", "game_id"),
        ("npb_spaia", "games", "game_id"),
        ("npb_spaia", "rosters", "team_id, player_id"),
        ("mlb_statsapi", "rosters", "team_id, player_id"),
    ]:
        try:
            (total, dupes), = wh.query(f"""
                SELECT coalesce(sum(n), 0), coalesce(sum(n - 1), 0) FROM (
                  SELECT count(*) n FROM {_silver(src, entity)}
                  WHERE dt >= '{start}' AND {key.split(',')[0]} IS NOT NULL GROUP BY dt, {key})""")
        except RuntimeError as e:
            rep.line("WARN", f"silver {src}/{entity} duplicates", f"could not read: {str(e)[:120]}")
            continue
        total, dupes = int(total), int(dupes)
        rep.line("FAIL" if dupes else "PASS", f"silver {src}/{entity} duplicates", f"{dupes} duplicate rows of {total}")


def check_quality_results(wh: Warehouse, rep: Report, start: str) -> None:
    rows = wh.query(f"""
        SELECT `check`, coalesce(`table`, ''), count(DISTINCT dt), concat_ws(',', sort_array(collect_set(dt)))
        FROM {GOLD}.audit_quality WHERE dt >= '{start}' AND NOT ok
        GROUP BY 1, 2 ORDER BY 3 DESC""")
    if not rows:
        rep.line("PASS", "quality checks", "no failed checks recorded")
    for check, table, ndates, dts in rows:
        dts = dts.split(",")
        shown = ", ".join(dts[:6]) + (f" (+{len(dts) - 6} more)" if len(dts) > 6 else "")
        rep.line("WARN", f"quality: {check} {table}".strip(), f"failed on {ndates} date(s): {shown}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--warehouse", default=os.getenv("GBDP_WAREHOUSE_ID", "a9d3d2589a30ab67"))
    ap.add_argument("--profile", default=os.getenv("DATABRICKS_CONFIG_PROFILE"))
    ap.add_argument("--start", default="2026-05-13", help="first dt to check (default: start of the replay)")
    args = ap.parse_args()

    wh = Warehouse(args.warehouse, args.profile)
    rep = Report()
    checks = [
        lambda: check_daily_counts(wh, rep, args.start),
        lambda: check_pa_per_game(wh, rep, args.start),
        lambda: check_pa_teams(wh, rep, args.start),
        lambda: check_uniqueness(wh, rep, args.start),
        lambda: check_dim_player_current(wh, rep),
        lambda: check_bio_fill(wh, rep),
        lambda: check_roster_size(wh, rep, args.start),
        lambda: check_silver_statcast(wh, rep, args.start),
        lambda: check_silver_duplicates(wh, rep, args.start),
        lambda: check_quality_results(wh, rep, args.start),
    ]
    for check in checks:
        try:
            check()
        except RuntimeError as e:
            rep.line("FAIL", "query error", str(e)[:300])
    print(f"\n{rep.failed} FAIL, {rep.warned} WARN")
    return 1 if rep.failed else 0


if __name__ == "__main__":
    sys.exit(main())
