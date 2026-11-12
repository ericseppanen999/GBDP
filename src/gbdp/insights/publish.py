"""insight_candidates: the detectors run as of one date, written to gold.

One partition per as-of date (the stage's end date). Reads only
feature_batter_daily (season to date) and dim_player's newest snapshot, so a
run costs one season scan of a small table, not of plate appearances.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import yaml

from gbdp.insights.detectors import detect
from gbdp.utils.io import gold_root, path_exists, spark_path, storage_format
from gbdp.utils.logging import get_logger
from gbdp.utils.time import parse_date

logger = get_logger("gbdp.insights")


def insights_config(path: Path | None = None) -> Dict[str, Any]:
    """configs/insights.yaml, or the file at GBDP_INSIGHTS_CONFIG (tests)."""
    import os

    path = path or Path(os.environ.get("GBDP_INSIGHTS_CONFIG") or "configs/insights.yaml")
    with Path(path).open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def _latest_dim_player_dt(root: Path, as_of: date) -> date | None:
    from gbdp.gold.publish import _gold_dates

    dts = _gold_dates(root, "dim_player", date(as_of.year, 1, 1), as_of)
    return max(dts) if dts else None


# The views the detectors read, and an empty stand-in for each when its
# table doesn't exist yet (a first run, or a league with no Statcast).
VIEWS = {"fbd": "feature_batter_daily", "fpd": "feature_pitch_daily", "fcd": "feature_contact_daily",
         "fbpd": "feature_batter_pitch_daily"}
# The pitch-level counters feature_pitch_daily and feature_batter_pitch_daily share.
_COMPONENTS = ("0 AS pitches, 0 AS swings, 0 AS whiffs, 0 AS zone_pitches, 0 AS zone_swings, 0 AS zone_whiffs, "
               "0 AS chase_pitches, 0 AS chase_swings, 0 AS bip, 0 AS hard_hit, 0 AS barrels, 0.0 AS ev_sum, "
               "0 AS ev_n, 0.0 AS la_sum, 0 AS la_n, 0 AS gb, 0 AS ld, 0 AS fb, 0 AS pu, 0 AS pulled, "
               "0 AS pulled_air, 0.0 AS woba_value, 0.0 AS xwoba_value, 0 AS woba_den")
EMPTY = {
    "fbd": "CAST(NULL AS STRING) AS player_id, CAST(NULL AS STRING) AS league_code, 0 AS pa, 0 AS ab, 0 AS h, "
           "0 AS hr, 0 AS bb, 0 AS so, 0 AS hbp, 0 AS sf, 0 AS sh, 0 AS doubles, 0 AS triples, 0 AS woba_den, "
           "0.0 AS woba_value, CAST(NULL AS STRING) AS dt",
    "fpd": "CAST(NULL AS STRING) AS player_id, CAST(NULL AS STRING) AS pitch_type, " + _COMPONENTS
           + ", 0.0 AS velo_sum, 0 AS velo_n, 0.0 AS spin_sum, 0 AS spin_n, CAST(NULL AS STRING) AS dt",
    "fbpd": "CAST(NULL AS STRING) AS player_id, CAST(NULL AS STRING) AS pitch_group, " + _COMPONENTS
            + ", CAST(NULL AS STRING) AS dt",
    "fcd": "CAST(NULL AS STRING) AS player_id, 0.0 AS woba_value, 0.0 AS xwoba_value, 0 AS woba_den, "
           "0 AS xwoba_bip_n, 0 AS bip, 0 AS babip_hits, CAST(NULL AS STRING) AS dt",
}


def _duckdb_query(root: Path, as_of: date):
    import duckdb
    import pyarrow as pa

    from gbdp.gold.publish import _read_gold_snapshot

    con = duckdb.connect()
    for view, table in VIEWS.items():
        base = root / table
        files = sorted(str(p).replace("\\", "/") for p in base.glob("dt=*/*.parquet")
                       if f"{as_of.year}-01-01" <= p.parent.name[3:] <= as_of.isoformat())
        if files:
            paths = ", ".join(f"'{f}'" for f in files)
            con.execute(f"CREATE VIEW {view} AS SELECT * FROM read_parquet([{paths}], union_by_name=true, hive_partitioning=false)")
        else:
            con.execute(f"CREATE VIEW {view} AS SELECT {EMPTY[view]} WHERE false")
    snap = _latest_dim_player_dt(root, as_of)
    names = [{"player_id": r["player_id"], "primary_name": r.get("primary_name")}
             for r in (_read_gold_snapshot(root, "dim_player", snap, ["player_id", "primary_name"]) if snap else [])]
    players = pa.Table.from_pylist(names or [], schema=pa.schema([("player_id", pa.string()), ("primary_name", pa.string())]))
    con.register("players", players)

    def q(sql: str) -> List[Dict[str, Any]]:
        cur = con.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    return q


def _spark_query(root: Path, as_of: date):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    for view, table in VIEWS.items():
        if path_exists(root / table):
            spark.read.format("delta").load(spark_path(root / table)) \
                .where((F.col("dt") >= f"{as_of.year}-01-01") & (F.col("dt") <= as_of.isoformat())) \
                .createOrReplaceTempView(view)
        else:
            spark.sql(f"SELECT {EMPTY[view]} WHERE false").createOrReplaceTempView(view)
    snap = _latest_dim_player_dt(root, as_of)
    dp = spark.read.format("delta").load(spark_path(root / "dim_player"))
    dp = dp.where(F.col("dt") == snap.isoformat()) if snap else dp.limit(0)
    dp.select("player_id", "primary_name").dropDuplicates(["player_id"]).createOrReplaceTempView("players")

    def q(sql: str) -> List[Dict[str, Any]]:
        return [r.asDict() for r in spark.sql(sql).collect()]

    return q


def publish_insights(start: str, end: str, root: Path | None = None, force: bool = True) -> Path | None:
    from gbdp.gold.publish import _write_gold_table

    root = root or gold_root()
    as_of = parse_date(end)
    if not path_exists(root / "feature_batter_daily"):
        logger.info("insights: no feature_batter_daily yet; nothing to detect")
        return None
    cfg = insights_config()
    q = _duckdb_query(root, as_of) if storage_format() == "parquet" else _spark_query(root, as_of)
    rows = detect(q, as_of, cfg)
    counts: Dict[str, int] = {}
    for r in rows:
        counts[r["detector"]] = counts.get(r["detector"], 0) + 1
    logger.info("insights as of %s: %s", as_of.isoformat(), counts or "none")
    return _write_gold_table(root, "insight_candidates", as_of, rows, force)
