"""Silver gate: is every silver entity complete, unique and actually populated?

    python scripts/audit_silver.py --start 2026-05-13 --end 2026-11-05

For each silver entity over the range: dates present versus bronze, rows per
date versus the bronze records they came from, duplicates on the natural key,
and the null rate of EVERY column (a column that is 100% null is a mapping
bug even when row counts look fine). Reads the Volumes through a SQL
warehouse; read-only. Exits 1 on any FAIL.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from verify_lakehouse import BRONZE_VOL, SILVER_VOL, Report, Warehouse  # noqa: E402

# (source, entity, natural key, bronze source/entity it is normalized from,
#  SQL that counts expected silver rows in one bronze body, or None)
ENTITIES = [
    ("mlb_statsapi", "games", ["game_id"], "mlb_statsapi/schedule",
     "size(from_json(body_text, 'struct<dates:array<struct<games:array<string>>>>').dates[0].games)"),
    ("mlb_statsapi", "rosters", ["team_id", "player_id"], "mlb_statsapi/rosters", None),
    ("mlb_statsapi", "teams", ["team_id"], "mlb_statsapi/teams",
     "size(from_json(body_text, 'struct<teams:array<string>>').teams)"),
    ("mlb_statsapi", "transactions", ["transaction_id"], "mlb_statsapi/transactions",
     "size(from_json(body_text, 'struct<transactions:array<string>>').transactions)"),
    ("mlb_statsapi", "player_bios", ["player_id"], "mlb_statsapi/player_bios",
     "size(from_json(body_text, 'struct<people:array<string>>').people)"),
    ("mlb_statsapi", "boxscore_batting", ["game_id", "player_id"], "mlb_statsapi/boxscore", None),
    ("mlb_statsapi", "boxscore_pitching", ["game_id", "player_id"], "mlb_statsapi/boxscore", None),
    ("mlb_statcast", "pitches", ["game_pk", "at_bat_number", "pitch_number"], "mlb_statcast/pitches", None),
    ("npb_spaia", "games", ["game_id"], "npb_spaia/games",
     "size(from_json(body_text, 'array<string>'))"),
    ("npb_spaia", "rosters", ["team_id", "player_id"], "npb_spaia/rosters", None),
    ("npb_spaia", "game_pbp", ["game_id", "event_id"], "npb_spaia/game_pbp", None),
]
# Columns that are legitimately sparse: (source/entity, column) -> why.
SPARSE_OK = {
    ("mlb_statcast/pitches", "events"): "set only on a PA's last pitch",
    ("mlb_statcast/pitches", "on_1b"): "runner on first",
    ("mlb_statcast/pitches", "on_2b"): "runner on second",
    ("mlb_statcast/pitches", "on_3b"): "runner on third",
    ("mlb_statsapi/transactions", "player_id"): "some transactions name no player",
    ("npb_spaia/rosters", "bats"): "SPAIA's roster gives no handedness",
    ("npb_spaia/rosters", "throws"): "SPAIA's roster gives no handedness",
    ("npb_spaia/rosters", "staff_title"): "set only for coaches",
    ("npb_spaia/rosters", "latest_movement"): "set only for players with a roster move this season",
    ("npb_spaia/rosters", "movement_date"): "set only for players with a roster move this season",
}


def _src(source: str, entity: str) -> str:
    return f"read_files('{SILVER_VOL}/{source}/{entity}', format => 'parquet', mergeSchema => true)"


def audit_entity(wh: Warehouse, rep: Report, source, entity, key, bronze, expected_sql, start, end) -> None:
    name = f"{source}/{entity}"
    rng = f"dt BETWEEN '{start}' AND '{end}'"
    try:
        cols = [r[0] for r in wh.query(f"DESCRIBE QUERY SELECT * FROM {_src(source, entity)}")]
    except RuntimeError as e:
        rep.line("FAIL", f"silver {name}", f"unreadable: {str(e)[:150]}")
        return
    if key[0] not in cols:
        rep.line("FAIL", f"silver {name}", "no data rows at all (only empty placeholders)")
        return
    data_cols = [c for c in cols if c not in ("dt", "empty", "_rescued_data", "source")]
    real = f"{rng} AND {key[0]} IS NOT NULL"

    # Dates: silver vs bronze dates whose latest fetch holds data (a no-game
    # day's empty payload correctly yields an empty silver placeholder).
    s_dates = {r[0] for r in wh.query(f"SELECT DISTINCT dt FROM {_src(source, entity)} WHERE {real}")}
    count_expr = expected_sql or "length(body_text)"
    latest = f"""
        SELECT dt, max_by(n, fetched_at_utc) AS n FROM (
          SELECT regexp_extract(_metadata.file_path, 'dt=([0-9-]+)', 1) AS dt, fetched_at_utc, {count_expr} AS n
          FROM read_files('{BRONZE_VOL}/raw/{bronze}', format => 'json'))
        WHERE {rng} GROUP BY dt"""
    by_date = {d: int(n or 0) for d, n in wh.query(latest)}
    # Without a record count, a body under ~200 bytes is an empty response.
    b_dates = {d for d, n in by_date.items() if (n > 0 if expected_sql else n > 200)}

    # Rows vs what bronze says should be there (latest fetch per date).
    count_note = ""
    if expected_sql:
        exp = sum(by_date.values())
        got = wh.query(f"SELECT count(*) FROM {_src(source, entity)} WHERE {real}")[0][0]
        exp, got = int(exp or 0), int(got or 0)
        # player_bios is one season list per date; teams likewise.
        count_note = f"; rows {got:,} vs bronze {exp:,}"
        if got != exp:
            rep.line("FAIL", f"silver {name} rows = bronze records", f"{got:,} vs {exp:,}")

    # Duplicates on the natural key, within a date.
    key_sql = ", ".join(key)
    total, dupes = wh.query(f"""
        SELECT coalesce(sum(n), 0), coalesce(sum(n - 1), 0) FROM (
          SELECT count(*) n FROM {_src(source, entity)} WHERE {real} GROUP BY dt, {key_sql})""")[0]
    total, dupes = int(total), int(dupes)

    # Null rate of every column.
    exprs = ", ".join(f"avg(CASE WHEN `{c}` IS NULL OR CAST(`{c}` AS STRING) IN ('', '[]', '{{}}') THEN 1.0 ELSE 0 END)" for c in data_cols)
    rates = dict(zip(data_cols, (float(x or 0) for x in wh.query(f"SELECT {exprs} FROM {_src(source, entity)} WHERE {real}")[0])))
    empty_cols = [c for c, r in rates.items() if r >= 0.999 and (name, c) not in SPARSE_OK]
    sparse = {c: r for c, r in rates.items() if 0.2 <= r < 0.999 and (name, c) not in SPARSE_OK}

    missing = sorted(b_dates - s_dates)
    ok = not missing and not dupes and not empty_cols
    rep.line("PASS" if ok else "FAIL", f"silver {name}",
             f"{len(s_dates)} dates (bronze {len(b_dates)}), {total:,} rows{count_note}, {dupes} dupes"
             + (f"; missing dates {missing[:4]}" if missing else "")
             + (f"; ALL-NULL columns {empty_cols}" if empty_cols else ""))
    if sparse:
        rep.line("WARN", f"  {name} sparse columns", ", ".join(f"{c} {r:.0%} null" for c, r in sorted(sparse.items(), key=lambda kv: -kv[1])))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", default="2026-05-13")
    ap.add_argument("--end", default="2026-11-05")
    ap.add_argument("--warehouse", default=os.getenv("GBDP_WAREHOUSE_ID", "a9d3d2589a30ab67"))
    ap.add_argument("--only", default="", help="comma-separated source/entity names to audit")
    args = ap.parse_args()
    wh, rep = Warehouse(args.warehouse, os.getenv("DATABRICKS_CONFIG_PROFILE")), Report()
    only = {x for x in args.only.split(",") if x}
    for source, entity, key, bronze, expected in ENTITIES:
        if only and f"{source}/{entity}" not in only:
            continue
        try:
            audit_entity(wh, rep, source, entity, key, bronze, expected, args.start, args.end)
        except RuntimeError as e:
            rep.line("FAIL", f"silver {source}/{entity}", f"query error: {str(e)[:200]}")
    print(f"\n{rep.failed} FAIL, {rep.warned} WARN")
    return 1 if rep.failed else 0


if __name__ == "__main__":
    sys.exit(main())
