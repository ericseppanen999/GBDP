"""Read-only health check of the live lakehouse, run from a workstation.

Queries the Unity Catalog tables through a SQL warehouse (via the `databricks`
CLI's auth), so it needs no deploy and is safe to run while jobs are running.

    python scripts/verify_lakehouse.py [--warehouse ID] [--profile NAME] [--start 2026-05-13]
        [--gold-delta] [--only npb_players_only,npb_pa_per_game,...]

--gold-delta reads gold from its Delta tables on the Volume instead of the UC
tables, which only change when register_uc runs: the way to check a rebuilt
gold before its gate opens.

Prints one line per check (PASS / WARN / FAIL) and exits 1 if anything FAILs.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import re
import tempfile
import time
from datetime import date

CATALOG = "gbdp"
GOLD = f"{CATALOG}.gold_gbdp"
SILVER_VOL = f"/Volumes/{CATALOG}/silver_gbdp/silver_vol/gbdp/silver"
GOLD_VOL = f"/Volumes/{CATALOG}/gold_gbdp/gold_vol/gbdp/gold"
# With --gold-delta, GOLD is this marker and query() points each table at its
# Delta path instead.
_GOLD_DELTA = "__gold_delta__"

# Late-March openers through the October postseason. No-game days inside it
# (All-Star break, NPB Mondays, rainouts) come from the schedules, not dates.
SEASON_MONTHS = range(3, 11)


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
        sql = re.sub(rf"\b{_GOLD_DELTA}\.(\w+)", lambda m: f"delta.`{GOLD_VOL}/{m.group(1)}`", sql)
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


BRONZE_VOL = f"/Volumes/{CATALOG}/bronze_gbdp/bronze_vol/gbdp/bronze"
# Entities every in-season day should have, and the smallest body that counts
# as real data (an empty schedule or roster response is a few hundred bytes).
BRONZE_ENTITIES = [
    ("mlb_statsapi", "schedule", 500),  # the All-Star Game day has a single game
    ("mlb_statsapi", "rosters", 50_000),
    ("mlb_statsapi", "teams", 20_000),
    # A day with no transactions is real (most of October): status checks only.
    ("mlb_statsapi", "transactions", 0),
    ("mlb_statsapi", "boxscore", 50_000),
    ("mlb_statsapi", "player_bios", 100_000),
    ("mlb_statcast", "pitches", 100_000),
    # One NPB game is ~400 bytes (09-03 had 2 games in 785); a day off is "[]".
    ("npb_spaia", "games", 100),
    ("npb_spaia", "rosters", 50_000),
    ("npb_spaia", "game_pbp", 50_000),
    # KBO games were missing for 30 May-June dates while box scores were there,
    # so fact_game and dim_team had no KBO rows for them. Monday is KBO's day off.
    ("kbo_api", "games", 100),
    ("kbo_api", "boxscore", 100),
]


def check_bronze(wh: Warehouse, rep: Report, start: str) -> None:
    """Every raw payload records its HTTP status, so fetch health is auditable from bronze alone."""
    off_days: dict = {}
    loaded: dict = {}
    # Statcast has nothing on a day with no MLB game played: the All-Star
    # break, playoff off days, after the World Series (and a rained-out game
    # still counts in the schedule's totalGames, so that alone isn't enough).
    mlb_played = _game_days(wh, start)["MLB"]
    kbo_idle = _kbo_idle_days(wh, start)
    for source, entity, min_bytes in BRONZE_ENTITIES:
        path = f"{BRONZE_VOL}/raw/{source}/{entity}"
        try:
            rows = wh.query(f"""
                WITH f AS (
                  SELECT regexp_extract(_metadata.file_path, 'dt=([0-9-]+)', 1) AS dt,
                         CAST(status_code AS INT) AS status, _metadata.file_modification_time AS fetched_at_utc, length(body_text) AS bytes
                  FROM read_files('{path}', format => 'json')
                ), latest AS (
                  SELECT dt, max_by(status, fetched_at_utc) AS status, max_by(bytes, fetched_at_utc) AS bytes,
                         count(*) AS fetches
                  FROM f WHERE dt >= '{start}' GROUP BY dt
                )
                SELECT dt, status, bytes, fetches FROM latest ORDER BY dt""")
        except RuntimeError as e:
            rep.line("FAIL", f"bronze {source}/{entity}", f"unreadable: {str(e)[:120]}")
            continue
        # The statement API returns every value as a string.
        in_season = [r for r in rows if date.fromisoformat(r[0]).month in SEASON_MONTHS]
        bad_status = [r[0] for r in rows if r[1] is not None and int(r[1]) != 200]
        thin = [r[0] for r in in_season if int(r[2] or 0) < min_bytes and r[0] not in off_days.get(source, set())]
        if source == "mlb_statcast":
            thin = [d for d in thin if d in mlb_played]
        if source == "kbo_api":
            thin = [d for d in thin if d not in kbo_idle]
        if entity == "schedule" and source == "mlb_statsapi":
            # MLB no-game days (All-Star break, offseason): totalGames = 0.
            played = wh.query(f"""
                SELECT dt, max_by(n, fetched_at_utc) FROM (
                  SELECT regexp_extract(_metadata.file_path, 'dt=([0-9-]+)', 1) AS dt, _metadata.file_modification_time AS fetched_at_utc,
                         CAST(get_json_object(body_text, '$.totalGames') AS INT) AS n
                  FROM read_files('{path}', format => 'json'))
                WHERE dt >= '{start}' GROUP BY dt""")
            for src in ("mlb_statsapi", "mlb_statcast"):
                off_days[src] = {d for d, n in played if int(n or 0) == 0}
            thin = [d for d in thin if d not in off_days["mlb_statsapi"]]
        if entity == "rosters" and source == "mlb_statsapi":
            # A roster fetched without date= is TODAY's roster, whatever dt it
            # is filed under (fixed in aa8c4a0; older payloads must be refetched).
            undated = wh.query(f"""
                SELECT dt FROM (
                  SELECT regexp_extract(_metadata.file_path, 'dt=([0-9-]+)', 1) AS dt,
                         max_by(to_json(params), _metadata.file_modification_time) AS p
                  FROM read_files('{path}', format => 'json') GROUP BY 1)
                WHERE dt >= '{start}' AND p NOT LIKE '%"date"%' ORDER BY dt""")
            undated = [r[0] for r in undated]
            rep.line("FAIL" if undated else "PASS", "bronze mlb_statsapi/rosters dated",
                     f"latest fetch without date=: {undated[:6] or 'none'}" + (f" (+{len(undated) - 6})" if len(undated) > 6 else ""))
        if entity == "rosters" and source == "npb_spaia":
            # The pre-team-ID-fix fetches repeated every roster ~110x; a sane
            # directory is ~1,400 people, well under 3 MB.
            big = [r[0] for r in rows if int(r[2] or 0) > 3_000_000]
            rep.line("FAIL" if big else "PASS", "bronze npb_spaia/rosters not repeated",
                     f"latest fetch over 3 MB: {big[:6] or 'none'}")
        if entity == "games" and source == "npb_spaia":
            # A day with no game played (Mondays, the All-Star break, or every
            # game rained out: 06-08's only game was 試合中止). Recorded so the
            # other NPB entities skip those days.
            played = wh.query(f"""
                SELECT dt, max_by(n, fetched_at_utc) FROM (
                  SELECT regexp_extract(_metadata.file_path, 'dt=([0-9-]+)', 1) AS dt, _metadata.file_modification_time AS fetched_at_utc,
                         size(filter(from_json(body_text, 'array<struct<gameStateName:string>>'),
                                     g -> g.gameStateName <> '試合中止')) AS n
                  FROM read_files('{path}', format => 'json'))
                WHERE dt >= '{start}' GROUP BY dt""")
            off_days[source] = {d for d, n in played if int(n or 0) == 0}
            thin = []
        # A league's schedule (MLB) / games (NPB) entity defines which dates were
        # loaded; every other entity of that league should have the same dates.
        dates = {r[0] for r in rows}
        league = source.split("_")[0]
        if entity in ("schedule", "games"):
            loaded[league] = dates
        missing = sorted(loaded.get(league, set()) - dates)
        detail = (f"{len(rows)} dates; missing vs schedule: {missing[:4] or 'none'}"
                  + (f" (+{len(missing) - 4})" if len(missing) > 4 else "")
                  + f"; non-200: {bad_status[:5] or 'none'}; in-season under {min_bytes:,} bytes: {thin[:6] or 'none'}")
        rep.line("FAIL" if bad_status or thin or missing else "PASS", f"bronze {source}/{entity}", detail)


def _kbo_idle_days(wh: Warehouse, start: str) -> set:
    """Dates both KBO feeds came back empty: preseason, Mondays, rainouts. The
    check is for one feed missing while the other has the day (May-June games
    were missing while their box scores were there), not for days off."""
    sizes: dict = {}
    for entity in ("games", "boxscore"):
        for dt, n in wh.query(f"""
            SELECT dt, max_by(bytes, mt) FROM (
              SELECT regexp_extract(_metadata.file_path, 'dt=([0-9-]+)', 1) AS dt,
                     _metadata.file_modification_time AS mt, length(body_text) AS bytes
              FROM read_files('{BRONZE_VOL}/raw/kbo_api/{entity}', format => 'json'))
            WHERE dt >= '{start}' GROUP BY dt"""):
            sizes.setdefault(dt, []).append(int(n or 0))
    return {d for d, ns in sizes.items() if len(ns) == 2 and max(ns) < 100}


def _game_days(wh: Warehouse, start: str) -> dict:
    """Dates each league actually played, from bronze schedules (CR-009, CR-011)."""
    def played(path, expr):
        return {d for d, n in wh.query(f"""
            SELECT dt, max_by(n, mt) FROM (
              SELECT regexp_extract(_metadata.file_path, 'dt=([0-9-]+)', 1) AS dt,
                     _metadata.file_modification_time AS mt, {expr} AS n
              FROM read_files('{path}', format => 'json'))
            WHERE dt >= '{start}' GROUP BY dt""") if int(n or 0) > 0}

    return {
        # totalGames counts postponed games too: 10-16's only game was a
        # rained-out LCS game, so there was nothing to load. D = postponed, C = cancelled.
        "MLB": played(f"{BRONZE_VOL}/raw/mlb_statsapi/schedule",
                      "size(filter(from_json(get_json_object(body_text, '$.dates[0].games'), "
                      "'array<struct<status:struct<codedGameState:string>>>'), "
                      "g -> g.status.codedGameState NOT IN ('D', 'C')))"),
        "NPB": played(f"{BRONZE_VOL}/raw/npb_spaia/games",
                      "size(filter(from_json(body_text, 'array<struct<gameStateName:string>>'), g -> g.gameStateName <> '試合中止'))"),
    }


def check_daily_counts(wh: Warehouse, rep: Report, start: str) -> None:
    """Every day a league played must have games, PAs (and MLB pitches) in gold.

    Driven by the schedules, so a day missing from every gold table still shows
    up (CR-009), and NPB-only days aren't failed for having no MLB pitches (CR-011).
    """
    days = _game_days(wh, start)
    counts = {}
    for table, src_filter in (("fact_game", "source IN ('mlb_statsapi', 'npb_spaia')"),
                              ("fact_plate_appearance", "source IN ('mlb_statcast', 'npb_spaia')"),
                              ("fact_pitch", "source = 'mlb_statcast'")):
        for dt, source, n in wh.query(f"SELECT dt, source, count(*) FROM {GOLD}.{table} WHERE dt >= '{start}' AND {src_filter} GROUP BY 1, 2"):
            counts[(table, "MLB" if source.startswith("mlb") else "NPB", dt)] = int(n)
    for league, tables in (("MLB", ("fact_game", "fact_plate_appearance", "fact_pitch")),
                           ("NPB", ("fact_game", "fact_plate_appearance"))):
        missing = sorted(d for d in days[league] for t in tables if not counts.get((t, league, d)))
        missing = sorted(set(missing))
        rep.line("FAIL" if missing else "PASS", f"{league} game days loaded in gold",
                 f"{len(days[league])} game days; missing games/PAs{'/pitches' if league == 'MLB' else ''}: "
                 f"{missing[:6] or 'none'}" + (f" (+{len(missing) - 6})" if len(missing) > 6 else ""))


def check_npb_pa_per_game(wh: Warehouse, rep: Report, start: str) -> None:
    """NPB PAs per played game, every date (CR-005): the old builder made ~400.
    Per 9 innings, so a 12-inning game (06-22: 104 PAs) isn't flagged."""
    rows = wh.query(f"""
        WITH per_game AS (
          SELECT dt, game_id, count_if(event_type <> 'TRUNCATED') n, greatest(9, max(inning)) innings
          FROM {GOLD}.fact_plate_appearance WHERE dt >= '{start}' AND source = 'npb_spaia' GROUP BY dt, game_id)
        SELECT dt, sum(n) * 9 / sum(innings) FROM per_game GROUP BY dt""")
    bad = sorted(d for d, r in rows if not 55 <= float(r or 0) <= 100)
    rep.line("FAIL" if bad else "PASS", "NPB PAs per 9-inning game in 55..100",
             f"{len(rows)} dates; outside: {bad[:6] or 'none'}" + (f" (+{len(bad) - 6})" if len(bad) > 6 else ""))


def check_npb_players_only(wh: Warehouse, rep: Report) -> None:
    """No current NPB dim_player row may be a coach or manager (CR-002).

    Staff is anyone listed with a non-player role on ANY silver roster date
    and never as a player, so a coach who has since left the directory
    entirely is still caught. Current rows with no roster entry at all are
    reported too."""
    (n, orphans), = wh.query(f"""
        WITH r AS (SELECT player_id, role FROM {_silver('npb_spaia', 'rosters')}
                   WHERE player_id IS NOT NULL),
             staff AS (SELECT player_id FROM r GROUP BY player_id
                       HAVING count_if(role IS NOT NULL AND role <> 'player') > 0
                          AND count_if(role IS NULL OR role = 'player') = 0),
             cur AS (SELECT source_player_id FROM {GOLD}.dim_player
                     WHERE dt = (SELECT max(dt) FROM {GOLD}.dim_player) AND is_current AND source = 'npb_spaia')
        SELECT count_if(c.source_player_id IN (SELECT player_id FROM staff)),
               count_if(c.source_player_id NOT IN (SELECT DISTINCT player_id FROM r))
        FROM cur c""")
    rep.line("FAIL" if int(n) or int(orphans) else "PASS", "dim_player NPB: players only",
             f"{n} current coach/manager rows; {orphans} current rows on no silver roster")


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
    # Days no MLB game was played (All-Star break, playoff off days) have none.
    played = _game_days(wh, start)["MLB"]
    thin = [r[0] for r in rows if r[0] in played and int(r[1]) < 200]
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


def check_season_coverage(wh: Warehouse, rep: Report, start: str) -> None:
    """Gold covers MLB's whole season, from the league's own calendar: the
    2026 rebuild began 2026-05-13 while the season began 03-25, and every
    other check passed because they all asked whether loaded data was right,
    not whether all of it was loaded."""
    import requests

    season = start[:4]
    s = requests.get(f"https://statsapi.mlb.com/api/v1/seasons/{season}", params={"sportId": 1},
                     headers={"User-Agent": "gbdp-verify"}, timeout=30).json()["seasons"][0]
    first, last = s["regularSeasonStartDate"], s.get("postSeasonEndDate") or s["regularSeasonEndDate"]
    (lo, hi, n), = wh.query(f"""
        SELECT min(dt), max(dt), count(DISTINCT dt) FROM {GOLD}.fact_game
        WHERE source = 'mlb_statsapi' AND dt BETWEEN '{season}-01-01' AND '{season}-12-31' AND lower(status) LIKE 'final%'""")
    missing_start = lo is None or lo > first
    missing_end = hi is None or (hi < last and date.today().isoformat() > last)
    rep.line("FAIL" if missing_start or missing_end else "PASS", f"MLB {season} season covered in gold",
             f"season {first}..{last}; gold final games {lo}..{hi} on {n} dates"
             + (f"; missing {first}..{lo}" if missing_start and lo else "") + (f"; missing {hi}..{last}" if missing_end and hi else ""))


def check_silver_keys(wh: Warehouse, rep: Report, start: str) -> None:
    """Preflight, from silver alone and in minutes: the key collisions and gaps
    a gold run would otherwise find hours in. Run it before a long rebuild.
    FAIL = gold can't handle it; WARN = gold handles it, shown so it's seen."""
    pbp = _silver("npb_spaia", "game_pbp")
    (n,), = wh.query(f"""
        SELECT count(*) FROM (
          SELECT 1 FROM {pbp}
          WHERE dt >= '{start}' AND get_json_object(raw_event_json, '$.TextInfo_Bat_Text_Sec') = '6'
          GROUP BY dt, game_id, inning, top_bottom, play_ab HAVING count(*) > 1)""")
    rep.line("WARN" if int(n) else "PASS", "silver NPB repeated play_ab",
             f"{n} half-innings where SPAIA numbers two PA headers alike (gold suffixes #2)")
    for entity in ("boxscore_batting", "boxscore_pitching"):
        (n,), = wh.query(f"""
            SELECT count(*) FROM (
              SELECT 1 FROM {_silver('kbo_api', entity)}
              WHERE dt >= '{start}' AND player_name IS NOT NULL
              GROUP BY dt, game_id, team_id, player_name HAVING count(*) > 1)""")
        rep.line("WARN" if int(n) else "PASS", f"silver KBO same-name teammates ({entity})",
                 f"{n} (game, team, name) repeats (gold keys them team:name#2)")
    missing = wh.query(f"""
        SELECT b.dt FROM (SELECT DISTINCT dt FROM {_silver('kbo_api', 'boxscore_batting')}
                          WHERE dt >= '{start}' AND game_id IS NOT NULL) b
        LEFT ANTI JOIN (SELECT DISTINCT dt FROM {_silver('kbo_api', 'games')}
                        WHERE dt >= '{start}' AND game_id IS NOT NULL) g ON g.dt = b.dt
        ORDER BY 1""")
    dts = [r[0] for r in missing]
    rep.line("FAIL" if dts else "PASS", "silver KBO box scores without games",
             f"{len(dts)} dates" + (f": {', '.join(dts[:6])}" if dts else ""))
    (n,), = wh.query(f"""
        SELECT count(*) FROM (
          SELECT 1 FROM {_silver('mlb_statcast', 'pitches')}
          WHERE dt >= '{start}' AND game_pk IS NOT NULL
          GROUP BY dt, game_pk, at_bat_number, pitch_number HAVING count(*) > 1)""")
    rep.line("FAIL" if int(n) else "PASS", "silver Statcast one row per pitch", f"{n} repeated pitch keys")


def check_against_mlb(wh: Warehouse, rep: Report, days: list) -> None:
    """Production gold vs MLB's own boxscores: PAs, runs and hits per team, per game."""
    from reconcile_day import PLAYED, mlb_truth

    for day in days:
        truth = {pk: g for pk, g in mlb_truth(day).items() if g["status"] in PLAYED}
        rows = wh.query(f"""
            SELECT g.source_game_id, pa.is_top_inning,
                   count_if(pa.event_type <> 'TRUNCATED') AS pas,
                   coalesce(sum(pa.runs_scored_on_play), 0) AS runs,
                   count_if(pa.event_type IN ('1B', '2B', '3B', 'HR')) AS hits
            FROM {GOLD}.fact_plate_appearance pa
            JOIN {GOLD}.fact_game g ON g.game_id = pa.game_id AND g.dt = pa.dt
            WHERE pa.dt = '{day}' AND pa.source = 'mlb_statcast'
            GROUP BY 1, 2""")
        got: dict = {}
        for pk, top, pas, runs, hits in rows:
            side = 1 if str(top).lower() == "true" else 0  # [home, away]
            for key, v in (("pa", pas), ("runs", runs), ("hits", hits)):
                got.setdefault(pk, {}).setdefault(key, [0, 0])[side] = int(v)
        for key in ("pa", "runs", "hits"):
            bad = [pk for pk, g in truth.items() if tuple(got.get(pk, {}).get(key, [0, 0])) != tuple(g[key])]
            rep.line("PASS" if not bad else "FAIL", f"{day} MLB {key} per team = boxscore",
                     f"{len(truth) - len(bad)}/{len(truth)} games" + (f"; e.g. {bad[:3]}" if bad else ""))


def _audit_quality() -> str:
    # quality_checks writes plain parquet, not Delta; UC registers it as a
    # table at register_uc. Before that (--gold-delta), read the files.
    if GOLD == _GOLD_DELTA:
        return f"read_files('{GOLD_VOL}/audit_quality', format => 'parquet', mergeSchema => true)"
    return f"{GOLD}.audit_quality"


def check_quality_results(wh: Warehouse, rep: Report, start: str) -> None:
    rows = wh.query(f"""
        SELECT `check`, coalesce(`table`, ''), count(DISTINCT dt), concat_ws(',', sort_array(collect_set(dt)))
        FROM {_audit_quality()} WHERE dt >= '{start}' AND NOT ok
        GROUP BY 1, 2 ORDER BY 3 DESC""")
    if not rows:
        rep.line("PASS", "quality checks", "no failed checks recorded")
    # The pipeline's hard invariants are failures here too (CR-010).
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
    from gbdp.quality.checks import _HARD_CHECKS

    for check, table, ndates, dts in rows:
        dts = dts.split(",")
        shown = ", ".join(dts[:6]) + (f" (+{len(dts) - 6} more)" if len(dts) > 6 else "")
        rep.line("FAIL" if check in _HARD_CHECKS else "WARN", f"quality: {check} {table}".strip(),
                 f"failed on {ndates} date(s): {shown}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--warehouse", default=os.getenv("GBDP_WAREHOUSE_ID", "a9d3d2589a30ab67"))
    ap.add_argument("--profile", default=os.getenv("DATABRICKS_CONFIG_PROFILE"))
    ap.add_argument("--start", default="2026-03-25", help="first dt to check (default: 2026 opening day)")
    ap.add_argument("--gold-delta", action="store_true",
                    help="read gold from its Delta tables on the Volume, not the UC tables")
    ap.add_argument("--only", default="", help="comma-separated check names to run (default: all)")
    ap.add_argument("--truth-days", default="2026-05-20,2026-06-10",
                    help="comma-separated days to reconcile against MLB's boxscores ('' to skip)")
    args = ap.parse_args()
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    global GOLD
    if args.gold_delta:
        GOLD = _GOLD_DELTA
    wh = Warehouse(args.warehouse, args.profile)
    rep = Report()
    checks = {
        "bronze": lambda: check_bronze(wh, rep, args.start),
        "daily_counts": lambda: check_daily_counts(wh, rep, args.start),
        "pa_per_game": lambda: check_pa_per_game(wh, rep, args.start),
        "npb_pa_per_game": lambda: check_npb_pa_per_game(wh, rep, args.start),
        "npb_players_only": lambda: check_npb_players_only(wh, rep),
        "pa_teams": lambda: check_pa_teams(wh, rep, args.start),
        "uniqueness": lambda: check_uniqueness(wh, rep, args.start),
        "dim_player_current": lambda: check_dim_player_current(wh, rep),
        "bio_fill": lambda: check_bio_fill(wh, rep),
        "roster_size": lambda: check_roster_size(wh, rep, args.start),
        "silver_statcast": lambda: check_silver_statcast(wh, rep, args.start),
        "silver_duplicates": lambda: check_silver_duplicates(wh, rep, args.start),
        "silver_keys": lambda: check_silver_keys(wh, rep, args.start),
        "season_coverage": lambda: check_season_coverage(wh, rep, args.start),
        "quality_results": lambda: check_quality_results(wh, rep, args.start),
        "against_mlb": lambda: check_against_mlb(wh, rep, [d for d in args.truth_days.split(",") if d]),
    }
    only = [c for c in args.only.split(",") if c]
    unknown = sorted(set(only) - set(checks))
    if unknown:
        ap.error(f"unknown check(s) {unknown}; choose from {sorted(checks)}")
    for name, check in checks.items():
        if only and name not in only:
            continue
        try:
            check()
        except RuntimeError as e:
            rep.line("FAIL", "query error", str(e)[:300])
    print(f"\n{rep.failed} FAIL, {rep.warned} WARN")
    return 1 if rep.failed else 0


if __name__ == "__main__":
    sys.exit(main())
