from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, List

import yaml

from gbdp.utils.io import (
    data_root,
    ensure_dir,
    list_dir,
    path_exists,
    read_text,
    spark_path,
    storage_format,
    write_parquet_table,
    write_text,
)
from gbdp.utils.time import daterange, parse_date
from gbdp.utils.logging import get_logger
import time

logger = get_logger("gbdp.quality")


def run_quality_checks(
    start: str, end: str, root: Path | None = None, force: bool = False
) -> List[Path]:
    from gbdp.utils.io import gold_root
    root = root or gold_root()
    outputs: List[Path] = []
    cfg = _load_quality_config()
    days = list(daterange(parse_date(start), parse_date(end)))
    # Every date is checked (and recorded in audit_quality) even after one
    # fails: raising at the first hard failure left the rest of its window
    # unchecked. The stage still fails, naming every failed date.
    failed: List[str] = []
    for i, d in enumerate(days, 1):
        t0 = time.monotonic()
        logger.info("quality_checks dt=%s (%d/%d)", d.isoformat(), i, len(days))
        try:
            outputs.append(_run_for_date(root, d, cfg, force))
        except RuntimeError as exc:
            logger.error("%s", exc)
            failed.append(str(exc))
        logger.info("quality_checks dt=%s done (%.1fs)", d.isoformat(), time.monotonic() - t0)
    if failed:
        raise RuntimeError("; ".join(failed))
    return outputs


def _run_for_date(root: Path, dt: date, cfg: Dict, force: bool) -> Path:
    fmt = storage_format()
    con = None
    if fmt == "parquet":
        try:
            import duckdb
        except Exception as exc:
            raise RuntimeError("duckdb is required for parquet quality checks") from exc
        con = duckdb.connect()
    results: List[Dict[str, object]] = []

    dt_str = dt.isoformat()

    def _exists_for_check(table: str) -> bool:
        # Delta tables are one table per entity partitioned by dt, not a
        # standalone delta table per dt= folder -- existence has to be
        # checked at the table root, or a real table with (correctly) zero
        # rows for this date reads as "doesn't exist".
        return path_exists(root / table) if fmt != "parquet" else path_exists(root / table / f"dt={dt_str}")

    # Uniqueness checks
    for rule in cfg.get("uniqueness", []):
        table = rule["table"].split(".")[-1]
        cols = rule["columns"]
        if not _exists_for_check(table):
            continue
        cols_sql = ", ".join(cols)
        if fmt == "parquet":
            path = root / table / f"dt={dt_str}"
            con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{path}/*.parquet')")
            dupes = con.execute(
                f"SELECT COUNT(*) FROM (SELECT {cols_sql}, COUNT(*) c FROM t GROUP BY {cols_sql} HAVING c>1)"
            ).fetchone()[0]
        else:
            dupes = _spark_uniqueness(root / table, dt_str, cols)
        results.append(
            {"check": "uniqueness", "table": table, "columns": cols_sql, "dupes": dupes, "dt": dt_str, "ok": dupes == 0}
        )

    # Referential integrity
    for rule in cfg.get("referential_integrity", []):
        child = rule["child"].split(".")[-1]
        parent = rule["parent"].split(".")[-1]
        key = rule["key"]
        if not _exists_for_check(child) or not _exists_for_check(parent):
            continue
        if fmt == "parquet":
            child_path = root / child / f"dt={dt_str}"
            parent_path = root / parent / f"dt={dt_str}"
            con.execute(f"CREATE OR REPLACE VIEW c AS SELECT * FROM read_parquet('{child_path}/*.parquet')")
            con.execute(f"CREATE OR REPLACE VIEW p AS SELECT * FROM read_parquet('{parent_path}/*.parquet')")
            missing = con.execute(
                f"SELECT COUNT(*) FROM c LEFT JOIN p ON c.{key}=p.{key} WHERE c.{key} IS NOT NULL AND p.{key} IS NULL"
            ).fetchone()[0]
        else:
            missing = _spark_ref_integrity(root / child, root / parent, dt_str, key)
        results.append(
            {
                "check": "referential_integrity",
                "child": child,
                "parent": parent,
                "key": key,
                "missing": missing,
                "dt": dt_str,
                "ok": missing == 0,
            }
        )

    # Row count sanity
    for rule in cfg.get("row_count_min", []):
        table = rule["table"].split(".")[-1]
        min_count = int(rule.get("min_count", 0))
        if not _exists_for_check(table):
            continue
        if fmt == "parquet":
            path = root / table / f"dt={dt_str}"
            con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{path}/*.parquet')")
            count = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        else:
            count = _spark_count(root / table, dt_str)
        # An off day for every league (the All-Star break Monday, 07-13) has
        # no games to publish: only a shortfall against silver's schedule fails.
        scheduled = _scheduled_games(dt) if count < min_count and table == "fact_game" else None
        if scheduled == 0:
            logger.info("quality_checks dt=%s: no league scheduled a game; row_count_min %s passes", dt_str, table)
        results.append(
            {
                "check": "row_count_min",
                "table": table,
                "count": count,
                "min_count": min_count,
                "ok": count >= min_count or not _in_season(dt) or scheduled == 0,
                "dt": dt_str,
            }
        )

    # Completeness thresholds (coverage of games)
    thresholds = cfg.get("thresholds", {})
    if thresholds:
        game_path = root / "fact_game" / f"dt={dt_str}"
        if _exists_for_check("fact_game"):
            if fmt == "parquet":
                con.execute(
                    f"CREATE OR REPLACE VIEW games AS SELECT DISTINCT game_id FROM read_parquet('{game_path}/*.parquet')"
                )
                total_games = con.execute("SELECT COUNT(*) FROM games").fetchone()[0]
            else:
                total_games = _spark_distinct_count(root / "fact_game", dt_str, "game_id")
            # Statcast covers MLB only, and only games that were played: the
            # denominator was every game of every league (15 MLB of 21 games =
            # "71%" on a day every MLB game had its pitches).
            mlb_final = "source = 'mlb_statsapi' AND lower(status) LIKE 'final%'"
            if fmt == "parquet":
                total_mlb = con.execute(
                    f"SELECT COUNT(DISTINCT game_id) FROM read_parquet('{game_path}/*.parquet') WHERE {mlb_final}"
                ).fetchone()[0]
            else:
                total_mlb = _spark_distinct_count(root / "fact_game", dt_str, "game_id", where=mlb_final)
            if _exists_for_check("fact_pitch") and total_mlb > 0:
                pitch_path = root / "fact_pitch" / f"dt={dt_str}"
                if fmt == "parquet":
                    con.execute(
                        f"CREATE OR REPLACE VIEW pitches AS SELECT DISTINCT game_id FROM read_parquet('{pitch_path}/*.parquet')"
                    )
                    with_pitch = con.execute("SELECT COUNT(*) FROM pitches").fetchone()[0]
                else:
                    with_pitch = _spark_distinct_count(root / "fact_pitch", dt_str, "game_id")
                pct = min(1.0, float(with_pitch) / total_mlb)
                results.append(
                    {
                        "check": "pitch_coverage",
                        "pct": pct,
                        "threshold": float(thresholds.get("mlb_pitch_coverage_pct", 0)),
                        "ok": pct >= float(thresholds.get("mlb_pitch_coverage_pct", 0)),
                        "dt": dt_str,
                    }
                )
            # NPB PBP coverage
            if _exists_for_check("fact_plate_appearance") and thresholds.get("npb_pbp_coverage_pct") is not None:
                pa_path = root / "fact_plate_appearance" / f"dt={dt_str}"
                if fmt == "parquet":
                    con.execute(
                        f"CREATE OR REPLACE VIEW games_npb AS SELECT DISTINCT game_id FROM read_parquet('{game_path}/*.parquet') WHERE source='npb_spaia' AND coalesce(status, '') NOT LIKE '%中止%'"
                    )
                    total_npb = con.execute("SELECT COUNT(*) FROM games_npb").fetchone()[0]
                    con.execute(
                        f"CREATE OR REPLACE VIEW pa_npb AS SELECT DISTINCT game_id FROM read_parquet('{pa_path}/*.parquet') WHERE source='npb_spaia'"
                    )
                    with_pbp = con.execute("SELECT COUNT(*) FROM pa_npb").fetchone()[0]
                else:
                    from pyspark.sql import SparkSession

                    spark = SparkSession.builder.getOrCreate()
                    games_df = (
                        spark.read.format("delta").load(spark_path(root / "fact_game"))
                        .where(f"dt = '{dt_str}' AND source = 'npb_spaia' AND coalesce(status, '') NOT LIKE '%中止%'")
                    )
                    total_npb = games_df.select("game_id").distinct().count()
                    pa_df = (
                        spark.read.format("delta").load(spark_path(root / "fact_plate_appearance"))
                        .where(f"dt = '{dt_str}' AND source = 'npb_spaia'")
                    )
                    with_pbp = pa_df.select("game_id").distinct().count()
                # No NPB game played (an off day, or every game rained out:
                # 試合中止) has no play-by-play to cover; there's nothing to check.
                # pct is always a float: an int 0 made the column BIGINT on such
                # days and the UC write clashed with DOUBLE.
                pct_npb = float(with_pbp) / total_npb if total_npb else None
                if pct_npb is not None:
                    results.append(
                        {
                            "check": "npb_pbp_coverage",
                            "pct": pct_npb,
                            "threshold": float(thresholds.get("npb_pbp_coverage_pct", 0)),
                            "ok": pct_npb >= float(thresholds.get("npb_pbp_coverage_pct", 0)),
                            "dt": dt_str,
                        }
                    )

    # Base/out validity checks
    if _exists_for_check("fact_plate_appearance"):
        if fmt == "parquet":
            pa_path = root / "fact_plate_appearance" / f"dt={dt_str}"
            con.execute(f"CREATE OR REPLACE VIEW pa AS SELECT * FROM read_parquet('{pa_path}/*.parquet')")
            invalid_base = con.execute(
                "SELECT COUNT(*) FROM pa WHERE (base_state_before IS NOT NULL AND (base_state_before < 0 OR base_state_before > 7)) "
                "OR (base_state_after IS NOT NULL AND (base_state_after < 0 OR base_state_after > 7))"
            ).fetchone()[0]
            invalid_outs = con.execute(
                "SELECT COUNT(*) FROM pa WHERE (outs_before IS NOT NULL AND (outs_before < 0 OR outs_before > 3)) "
                "OR (outs_after IS NOT NULL AND (outs_after < 0 OR outs_after > 3))"
            ).fetchone()[0]
        else:
            pa_root = root / "fact_plate_appearance"
            invalid_base = _spark_invalid_range(pa_root, dt_str, "base_state_before", 0, 7) + _spark_invalid_range(
                pa_root, dt_str, "base_state_after", 0, 7
            )
            invalid_outs = _spark_invalid_range(pa_root, dt_str, "outs_before", 0, 3) + _spark_invalid_range(
                pa_root, dt_str, "outs_after", 0, 3
            )
        results.append(
            {"check": "base_state_range", "invalid": invalid_base, "dt": dt_str, "ok": invalid_base == 0}
        )
        results.append(
            {"check": "outs_range", "invalid": invalid_outs, "dt": dt_str, "ok": invalid_outs == 0}
        )

    # Schema drift detection for bronze parsed
    results.extend(_schema_drift_checks(root, dt))

    out_dir = root / "audit_quality" / f"dt={dt_str}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if force or not path_exists(out_path):
        _write_parquet(results, out_path)
    # Always enforce: skipping this when the file exists let a stage retry pass
    # a hard failure, because the failed attempt had just written that file.
    _enforce(results, dt_str)
    return out_path


# Structural invariants: these should never fail on correct code, regardless
# of upstream data availability, so a violation blocks the pipeline.
_HARD_CHECKS = {"uniqueness", "referential_integrity", "row_count_min", "base_state_range", "outs_range"}
# Completeness/coverage signals: these can legitimately vary with upstream
# publishing lag (confirmed: Statcast often isn't published yet for "yesterday"
# by the time the nightly job runs), so they're monitored but non-blocking.
_SOFT_CHECKS = {"pitch_coverage", "npb_pbp_coverage", "schema_drift"}


def _enforce(results: List[Dict[str, object]], dt_str: str) -> None:
    hard_failures = [r for r in results if r.get("check") in _HARD_CHECKS and r.get("ok") is False]
    soft_failures = [r for r in results if r.get("check") in _SOFT_CHECKS and r.get("ok") is False]
    for r in soft_failures:
        logger.warning("quality_checks dt=%s SOFT FAIL: %s", dt_str, r)
    if hard_failures:
        summary = "; ".join(f"{r.get('check')}:{r.get('table') or r.get('child') or ''}" for r in hard_failures)
        raise RuntimeError(f"quality_checks dt={dt_str} failed hard invariant(s): {summary}")


# Silver game lists gold builds fact_game from (MLB, NPB, KBO API).
_SCHEDULE_SOURCES = (("mlb_statsapi", "games"), ("npb_spaia", "games"), ("kbo_api", "games"))


def _scheduled_games(dt: date) -> int:
    """Games silver lists for dt across the sources fact_game reads; 0 on a
    day no league played."""
    from gbdp.gold.publish import _read_silver

    return sum(len(_read_silver(Path("."), src, entity, dt)) for src, entity in _SCHEDULE_SOURCES)


def _in_season(dt: date) -> bool:
    # A date with no games is normal once every league's season is over.
    path = Path("configs/leagues.yaml")
    if not path_exists(path):
        return True
    with path.open("r", encoding="utf-8") as f:
        leagues = yaml.safe_load(f).get("leagues", {})
    return any(m["season_start_month"] <= dt.month <= m["season_end_month"] for m in leagues.values())


def _load_quality_config() -> Dict:
    path = Path("configs/quality.yaml")
    if not path_exists(path):
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f).get("quality", {})


def _write_parquet(rows: List[Dict[str, object]], path: Path) -> None:
    if not rows:
        rows = [{"empty": True}]
    import pyarrow as pa
    # pa.Table.from_pylist infers its schema from a subset of rows and does not
    # union heterogeneous keys across the list, silently dropping/misaligning
    # fields for rows shaped differently than the ones it sampled. Normalize
    # every row to the full key set first so each check type keeps its fields.
    all_keys: List[str] = []
    seen = set()
    for row in rows:
        for k in row:
            if k not in seen:
                seen.add(k)
                all_keys.append(k)
    normalized = [{k: row.get(k) for k in all_keys} for row in rows]
    table = pa.Table.from_pylist(normalized)
    write_parquet_table(table, path, force=True)


def _schema_drift_checks(root: Path, dt: date) -> List[Dict[str, object]]:
    import json
    import pyarrow.parquet as pq

    results: List[Dict[str, object]] = []
    from gbdp.utils.io import bronze_root as _bronze_root
    bronze_root = _bronze_root() / "parsed"
    if not path_exists(bronze_root):
        return results
    for source_dir in list_dir(bronze_root, dirs_only=True):
        for entity_dir in list_dir(source_dir, dirs_only=True):
            part_dir = entity_dir / f"dt={dt.isoformat()}"
            if not path_exists(part_dir):
                continue
            # Skip schema drift checks on DBFS/Volumes where local FS access is blocked
            from gbdp.utils.io import is_dbfs_path
            if is_dbfs_path(part_dir):
                continue
            try:
                files = list(part_dir.glob("*.parquet"))
            except Exception:
                continue
            if not files:
                continue
            schema = pq.read_schema(files[0])
            fields = {name: str(schema.field(name).type) for name in schema.names}
            audit_dir = root / "audit_schema" / source_dir.name / entity_dir.name
            ensure_dir(audit_dir)
            current_path = audit_dir / f"dt={dt.isoformat()}.json"
            prev = None
            try:
                prev_files = sorted(audit_dir.glob("dt=*.json"))
            except Exception:
                prev_files = []
            if prev_files:
                prev_path = prev_files[-1]
                try:
                    prev = json.loads(read_text(prev_path, encoding="utf-8"))
                except Exception:
                    prev = None
            write_text(
                current_path,
                json.dumps(fields, ensure_ascii=True, sort_keys=True),
                encoding="utf-8",
                force=True,
            )
            if prev and prev != fields:
                results.append(
                    {
                        "check": "schema_drift",
                        "source": source_dir.name,
                        "entity": entity_dir.name,
                        "dt": dt.isoformat(),
                        "changed": True,
                    }
                )
    return results


def _spark_load_dt(table_root: Path, dt_str: str):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    return spark.read.format("delta").load(spark_path(table_root)).where(f"dt = '{dt_str}'")


def _spark_count(table_root: Path, dt_str: str) -> int:
    return _spark_load_dt(table_root, dt_str).count()


def _spark_distinct_count(table_root: Path, dt_str: str, col: str, where: str | None = None) -> int:
    df = _spark_load_dt(table_root, dt_str)
    if col not in df.columns:
        return 0
    if where:
        df = df.where(where)
    return df.select(col).distinct().count()


def _spark_uniqueness(table_root: Path, dt_str: str, cols: List[str]) -> int:
    from pyspark.sql import functions as F

    df = _spark_load_dt(table_root, dt_str)
    if any(c not in df.columns for c in cols):
        return 0
    dupes = df.groupBy(cols).count().where(F.col("count") > 1).count()
    return dupes


def _spark_ref_integrity(child_root: Path, parent_root: Path, dt_str: str, key: str) -> int:
    c = _spark_load_dt(child_root, dt_str)
    p = _spark_load_dt(parent_root, dt_str)
    if key not in c.columns or key not in p.columns:
        return 0
    missing = c.join(p, c[key] == p[key], "left").where(c[key].isNotNull() & p[key].isNull()).count()
    return missing


def _spark_invalid_range(table_root: Path, dt_str: str, col: str, min_val: int, max_val: int) -> int:
    from pyspark.sql import functions as F

    df = _spark_load_dt(table_root, dt_str)
    if col not in df.columns:
        return 0
    return (
        df.where(F.col(col).isNotNull() & ((F.col(col) < min_val) | (F.col(col) > max_val)))
        .count()
    )
