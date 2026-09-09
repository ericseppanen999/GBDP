from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict

import yaml

from gbdp.bronze.cache import ResponseCache
from gbdp.bronze.writer import BronzeWriter
from gbdp.connectors.mlb_statcast import MlbStatcastConnector
from gbdp.connectors.mlb_statsapi import MlbStatsApiConnector
from gbdp.connectors.npb_spaia import NpbSpaiaConnector
from gbdp.connectors.kbo import KboLocalConnector
from gbdp.connectors.lmb import LmbLocalConnector
from gbdp.connectors.indy import IndyLocalConnector
from gbdp.connectors.retrosheet import RetrosheetLocalConnector
from gbdp.silver.npb import normalize_npb
from gbdp.silver.mlb import normalize_mlb
from gbdp.silver.indy import normalize_indy
from gbdp.silver.retrosheet import normalize_retrosheet
from gbdp.silver.local_boxscore import normalize_local_boxscore
from gbdp.identity.resolver import resolve_identity
from gbdp.gold.publish import publish_gold
from gbdp.quality.checks import run_quality_checks
from gbdp.quality.metrics import write_run_audit
from gbdp.pipeline.runner import run_pipeline
from gbdp.catalog.uc import register_uc_tables, register_uc_tables_from_env
from gbdp.utils.io import data_root, bronze_root, silver_root, gold_root
from gbdp.utils.logging import get_logger

logger = get_logger("gbdp.cli")


def load_sources_config(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_connector(source: str, cfg: Dict, writer: BronzeWriter, cache: ResponseCache):
    if source == "mlb_statsapi":
        return MlbStatsApiConnector(writer, cache, cfg["base_url"])
    if source == "mlb_statcast":
        return MlbStatcastConnector(writer, cache, cfg["base_url"])
    if source == "npb_spaia":
        return NpbSpaiaConnector(writer, cache, cfg["base_url"])
    if source == "indy_local":
        return IndyLocalConnector(writer, cache)
    if source == "kbo_local":
        return KboLocalConnector(writer, cache)
    if source == "lmb_local":
        return LmbLocalConnector(writer, cache)
    if source == "retrosheet_local":
        return RetrosheetLocalConnector(writer, cache)
    raise ValueError(f"Unknown source: {source}")


def ingest(args: argparse.Namespace) -> None:
    cfg = load_sources_config(Path(args.sources))
    writer = BronzeWriter()
    cache = ResponseCache()

    source_cfg = cfg[args.source]
    connector = build_connector(args.source, source_cfg, writer, cache)

    partitions = connector.list_partitions(args.start, args.end, args.entity)
    logger.info("ingest source=%s entity=%s partitions=%d", args.source, args.entity, len(partitions))
    for partition in partitions:
        payload = connector.fetch_partition(partition)
        records = connector.parse_payload(payload)
        connector.write_bronze(payload, records, force=args.force)
        connector.emit_watermark(partition, "ok")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gbdp")
    sub = parser.add_subparsers(dest="cmd", required=True)

    ingest_p = sub.add_parser("ingest", help="Ingest bronze data for a source/entity")
    ingest_p.add_argument(
        "--source",
        required=True,
        choices=["mlb_statsapi", "mlb_statcast", "npb_spaia", "indy_local", "kbo_local", "lmb_local", "retrosheet_local"],
    )
    ingest_p.add_argument(
        "--entity",
        required=True,
        help="Entity name (e.g., schedule, rosters, transactions, pitches, schedules, rosters, game_pbp)",
    )
    ingest_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    ingest_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    ingest_p.add_argument("--sources", default="configs/sources.yaml", help="Path to sources.yaml")
    ingest_p.add_argument("--force", action="store_true", help="Overwrite existing bronze outputs")
    ingest_p.add_argument("--storage-format", choices=["parquet", "delta"], help="Override storage format")
    ingest_p.add_argument("--bronze-root", help="Override bronze root path")
    ingest_p.add_argument("--manual-root", help="Override manual data root path")
    ingest_p.add_argument("--http-timeout", type=int, help="HTTP timeout seconds")
    ingest_p.add_argument("--http-retries", type=int, help="HTTP retry count")
    ingest_p.add_argument("--http-backoff", type=float, help="HTTP backoff base")
    ingest_p.add_argument("--http-min-interval", type=float, help="HTTP min interval seconds")

    silver_p = sub.add_parser("silver", help="Normalize bronze to silver")
    silver_p.add_argument(
        "--source",
        required=True,
        choices=["npb_spaia", "mlb_statsapi", "mlb_statcast", "indy_local", "kbo_local", "lmb_local", "retrosheet_local"],
    )
    silver_p.add_argument(
        "--entity",
        required=True,
        choices=[
            "games",
            "rosters",
            "game_pbp",
            "standings",
            "game_batter_stats",
            "game_pitcher_stats",
            "player_batting_saber",
            "player_pitching_saber",
            "player_stats_by_year",
            "player_stats_by_month",
            "player_stats_by_game",
            "player_hitting_career",
            "transactions",
            "pitches",
            "boxscore_batting",
            "boxscore_pitching",
            "gameinfo",
            "batting",
            "pitching",
            "plays",
            "allplayers",
            "teamstats",
            "fielding",
        ],
    )
    silver_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    silver_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    silver_p.add_argument("--force", action="store_true", help="Overwrite existing silver outputs")
    silver_p.add_argument("--storage-format", choices=["parquet", "delta"], help="Override storage format")
    silver_p.add_argument("--silver-root", help="Override silver root path")

    identity_p = sub.add_parser("identity", help="Resolve identity and build bridge table")
    identity_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    identity_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    identity_p.add_argument("--force", action="store_true", help="Overwrite existing bridge outputs")
    identity_p.add_argument("--storage-format", choices=["parquet", "delta"], help="Override storage format")
    identity_p.add_argument("--gold-root", help="Override gold root path")

    gold_p = sub.add_parser("gold", help="Publish gold dims/facts")
    gold_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    gold_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    gold_p.add_argument("--force", action="store_true", help="Overwrite existing gold outputs")
    gold_p.add_argument("--storage-format", choices=["parquet", "delta"], help="Override storage format")
    gold_p.add_argument("--gold-root", help="Override gold root path")

    quality_p = sub.add_parser("quality", help="Run quality checks")
    quality_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    quality_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    quality_p.add_argument("--force", action="store_true", help="Overwrite existing quality outputs")
    quality_p.add_argument("--storage-format", choices=["parquet", "delta"], help="Override storage format")
    quality_p.add_argument("--gold-root", help="Override gold root path")

    serve_p = sub.add_parser("serve", help="Run FastAPI server (local)")
    serve_p.add_argument("--host", default="0.0.0.0")
    serve_p.add_argument("--port", default=8000, type=int)

    run_p = sub.add_parser("run", help="Run nightly pipeline stages")
    run_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    run_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    run_p.add_argument("--sources", default="configs/sources.yaml", help="Path to sources.yaml")
    run_p.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    run_p.add_argument("--stages", help="Comma-separated list of stages to run (optional)")
    run_p.add_argument("--retries", type=int, default=0, help="Retry count per stage")
    run_p.add_argument("--retry-delay", type=float, default=1.0, help="Retry delay seconds")
    run_p.add_argument("--storage-format", choices=["parquet", "delta"], help="Override storage format")
    run_p.add_argument("--chunk", choices=["month"], help="Chunk window for long ranges")
    run_p.add_argument("--bronze-root", help="Override bronze root path")
    run_p.add_argument("--silver-root", help="Override silver root path")
    run_p.add_argument("--gold-root", help="Override gold root path")
    run_p.add_argument("--manual-root", help="Override manual data root path")
    run_p.add_argument(
        "--window",
        choices=["nightly", "yesterday"],
        help="Override start/end with a built-in window (nightly = last 7 days ending yesterday, yesterday = yesterday only)",
    )
    run_p.add_argument(
        "--leagues",
        help="Comma-separated leagues to run (mlb,npb,indy,kbo,lmb,retrosheet)",
    )
    run_p.add_argument("--uc-catalog", help="UC catalog name (alternative to env var)")
    run_p.add_argument("--uc-bronze-schema", help="UC bronze schema (alternative to env var)")
    run_p.add_argument("--uc-silver-schema", help="UC silver schema (alternative to env var)")
    run_p.add_argument("--uc-gold-schema", help="UC gold schema (alternative to env var)")
    run_p.add_argument(
        "--uc-mode",
        choices=["managed", "external"],
        help="UC mode: managed writes tables (serverless) or external LOCATION tables",
    )
    run_p.add_argument("--http-timeout", type=int, help="HTTP timeout seconds")
    run_p.add_argument("--http-retries", type=int, help="HTTP retry count")
    run_p.add_argument("--http-backoff", type=float, help="HTTP backoff base")
    run_p.add_argument("--http-min-interval", type=float, help="HTTP min interval seconds")

    backfill_p = sub.add_parser("backfill", help="Backfill pipeline stages (alias of run)")
    backfill_p.add_argument("--start", default=None, help="Start date YYYY-MM-DD (default: 7 days ago)")
    backfill_p.add_argument("--end", default=None, help="End date YYYY-MM-DD (default: yesterday)")
    backfill_p.add_argument("--sources", default="configs/sources.yaml", help="Path to sources.yaml")
    backfill_p.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    backfill_p.add_argument("--stages", help="Comma-separated list of stages to run (optional)")
    backfill_p.add_argument("--retries", type=int, default=0, help="Retry count per stage")
    backfill_p.add_argument("--retry-delay", type=float, default=1.0, help="Retry delay seconds")
    backfill_p.add_argument("--storage-format", choices=["parquet", "delta"], help="Override storage format")
    backfill_p.add_argument("--chunk", choices=["month"], help="Chunk window for long ranges")
    backfill_p.add_argument("--bronze-root", help="Override bronze root path")
    backfill_p.add_argument("--silver-root", help="Override silver root path")
    backfill_p.add_argument("--gold-root", help="Override gold root path")
    backfill_p.add_argument("--manual-root", help="Override manual data root path")
    backfill_p.add_argument(
        "--window",
        choices=["nightly", "yesterday"],
        help="Override start/end with a built-in window (nightly = last 7 days ending yesterday, yesterday = yesterday only)",
    )
    backfill_p.add_argument(
        "--leagues",
        help="Comma-separated leagues to run (mlb,npb,indy,kbo,lmb,retrosheet)",
    )
    backfill_p.add_argument("--uc-catalog", help="UC catalog name (alternative to env var)")
    backfill_p.add_argument("--uc-bronze-schema", help="UC bronze schema (alternative to env var)")
    backfill_p.add_argument("--uc-silver-schema", help="UC silver schema (alternative to env var)")
    backfill_p.add_argument("--uc-gold-schema", help="UC gold schema (alternative to env var)")
    backfill_p.add_argument(
        "--uc-mode",
        choices=["managed", "external"],
        help="UC mode: managed writes tables (serverless) or external LOCATION tables",
    )
    backfill_p.add_argument("--http-timeout", type=int, help="HTTP timeout seconds")
    backfill_p.add_argument("--http-retries", type=int, help="HTTP retry count")
    backfill_p.add_argument("--http-backoff", type=float, help="HTTP backoff base")
    backfill_p.add_argument("--http-min-interval", type=float, help="HTTP min interval seconds")

    sub.add_parser("purge-stale", help="Delete gold tables with stale nested delta logs so next run starts clean").add_argument("--gold-root", help="Override gold root path")

    cd_p = sub.add_parser("check-data", help="Report row counts on key gold tables for a given date")
    cd_p.add_argument("--dt", default=None, help="Date to check (YYYY-MM-DD), defaults to yesterday")
    cd_p.add_argument("--gold-root", help="Override gold root path")
    cd_p.add_argument("--storage-format", choices=["parquet", "delta"], default="delta")

    tw_p = sub.add_parser("test-write", help="Smoke-test Delta write path against a throwaway volume table")
    tw_p.add_argument("--gold-root", help="Override gold root path")
    tw_p.add_argument("--storage-format", choices=["parquet", "delta"], default="delta")

    uc_p = sub.add_parser("register-uc", help="Register bronze/silver/gold tables in Unity Catalog")
    uc_p.add_argument("--catalog", required=False, help="UC catalog name (e.g., gbdp)")
    uc_p.add_argument("--bronze-schema", default="bronze_gbdp", help="UC schema for bronze tables")
    uc_p.add_argument("--silver-schema", default="silver_gbdp", help="UC schema for silver tables")
    uc_p.add_argument("--gold-schema", default="gold_gbdp", help="UC schema for gold tables")
    uc_p.add_argument("--storage-format", choices=["parquet", "delta"], help="Override storage format")
    uc_p.add_argument("--bronze-root", help="Override bronze root path")
    uc_p.add_argument("--silver-root", help="Override silver root path")
    uc_p.add_argument("--gold-root", help="Override gold root path")
    uc_p.add_argument("--uc-catalog", help="UC catalog name (alternative to env var)")
    uc_p.add_argument("--uc-bronze-schema", help="UC bronze schema (alternative to env var)")
    uc_p.add_argument("--uc-silver-schema", help="UC silver schema (alternative to env var)")
    uc_p.add_argument("--uc-gold-schema", help="UC gold schema (alternative to env var)")
    uc_p.add_argument(
        "--uc-mode",
        choices=["managed", "external"],
        help="UC mode: managed writes tables (serverless) or external LOCATION tables",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.cmd == "ingest":
        _set_storage_format(args)
        _set_roots(args)
        _set_http(args)
        ingest(args)
    if args.cmd == "silver":
        _set_storage_format(args)
        _set_roots(args)
        if args.source == "npb_spaia":
            normalize_npb(args.entity, args.start, args.end, force=args.force)
        elif args.source in {"mlb_statsapi", "mlb_statcast"}:
            normalize_mlb(args.entity, args.start, args.end, force=args.force)
        elif args.source == "indy_local":
            normalize_indy(args.entity, args.start, args.end, force=args.force)
        elif args.source in {"kbo_local", "lmb_local"}:
            normalize_local_boxscore(args.source, args.entity, args.start, args.end, force=args.force)
        elif args.source == "retrosheet_local":
            normalize_retrosheet(args.entity, args.start, args.end, force=args.force)
    if args.cmd == "identity":
        _set_storage_format(args)
        _set_roots(args)
        resolve_identity(args.start, args.end, force=args.force)
    if args.cmd == "gold":
        _set_storage_format(args)
        _set_roots(args)
        publish_gold(args.start, args.end, force=args.force)
    if args.cmd == "quality":
        _set_storage_format(args)
        _set_roots(args)
        run_quality_checks(args.start, args.end, force=args.force)
    if args.cmd == "serve":
        import uvicorn

        uvicorn.run("gbdp.serve.api:app", host=args.host, port=args.port, reload=False)
    if args.cmd == "run":
        _set_storage_format(args)
        _set_roots(args)
        _set_uc(args)
        _set_http(args)
        stages = args.stages.split(",") if args.stages else None
        leagues = args.leagues.split(",") if args.leagues and args.leagues != "all" else None
        start, end = _resolve_window(args.start, args.end, args.window)
        run_pipeline(start, end, args.sources, args.force, stages, args.retries, args.retry_delay, args.chunk, leagues)
    if args.cmd == "backfill":
        _set_storage_format(args)
        _set_roots(args)
        _set_uc(args)
        _set_http(args)
        raw_stages = args.stages or os.getenv("stages") or os.getenv("GBDP_BACKFILL_STAGES")
        stages = raw_stages.split(",") if raw_stages else None
        from datetime import timedelta
        from gbdp.utils.time import utc_now
        yesterday = (utc_now().date() - timedelta(days=1)).isoformat()
        week_ago = (utc_now().date() - timedelta(days=7)).isoformat()
        # Job params are injected as env vars on serverless: os.getenv("start_date") etc.
        raw_start = args.start or os.getenv("start_date") or os.getenv("GBDP_BACKFILL_START") or week_ago
        raw_end = args.end or os.getenv("end_date") or os.getenv("GBDP_BACKFILL_END") or yesterday
        raw_leagues = args.leagues or os.getenv("leagues") or os.getenv("GBDP_BACKFILL_LEAGUES") or "all"
        leagues = raw_leagues.split(",") if raw_leagues and raw_leagues != "all" else None
        start, end = _resolve_window(raw_start, raw_end, args.window)
        run_pipeline(start, end, args.sources, args.force, stages, args.retries, args.retry_delay, args.chunk, leagues)
    if args.cmd == "purge-stale":
        _set_roots(args)
        _purge_stale()
    if args.cmd == "check-data":
        _set_storage_format(args)
        _set_roots(args)
        _check_data(args.dt)
    if args.cmd == "test-write":
        _set_storage_format(args)
        _set_roots(args)
        _test_write()
    if args.cmd == "register-uc":
        _set_storage_format(args)
        _set_roots(args)
        _set_uc(args)
        if args.catalog:
            register_uc_tables(
                catalog=args.catalog,
                bronze_schema=args.bronze_schema,
                silver_schema=args.silver_schema,
                gold_schema=args.gold_schema,
                bronze_path=bronze_root(),
                silver_path=silver_root(),
                gold_path=gold_root(),
                fmt=os.getenv("GBDP_STORAGE_FORMAT", "parquet"),
            )
        else:
            register_uc_tables_from_env()


def _purge_stale() -> None:
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils
    from gbdp.utils.io import gold_root, spark_path, list_dir, path_exists

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    root = gold_root()

    purged = 0
    for table_dir in list_dir(root, dirs_only=True):
        for part_dir in list_dir(table_dir, dirs_only=True):
            if not part_dir.name.startswith("dt="):
                continue
            nested_log = part_dir / "_delta_log"
            if path_exists(nested_log):
                target = spark_path(table_dir)
                logger.info("purge-stale: removing %s (nested delta log found)", target)
                dbutils.fs.rm(target, True)
                purged += 1
                break  # whole table removed, move on
    logger.info("purge-stale: removed %d table(s)", purged)


def _check_data(dt: str | None) -> None:
    from datetime import timedelta
    from pyspark.sql import SparkSession
    from gbdp.utils.io import gold_root, spark_path, list_dir, path_exists
    from gbdp.utils.time import utc_now

    if not dt:
        dt = (utc_now().date() - timedelta(days=1)).isoformat()

    spark = SparkSession.builder.getOrCreate()
    root = gold_root()
    tables = [
        "fact_pitch", "fact_plate_appearance", "fact_game",
        "fact_roster", "fact_boxscore_batting", "fact_boxscore_pitching",
        "fact_standings", "dim_player", "dim_team", "bridge_source_ids",
    ]
    logger.info("=== Gold table row counts for dt=%s ===", dt)
    for table in tables:
        base = root / table
        if not path_exists(base):
            logger.info("  %-35s NOT FOUND", table)
            continue
        try:
            count = (
                spark.read.format("delta")
                .load(spark_path(base))
                .where(f"dt = '{dt}'")
                .count()
            )
            logger.info("  %-35s %d rows", table, count)
        except Exception as e:
            logger.info("  %-35s ERROR: %s", table, str(e)[:80])


def _test_write() -> None:
    from pyspark.sql import SparkSession
    from gbdp.utils.io import gold_root, spark_path

    spark = SparkSession.builder.getOrCreate()
    table_root = spark_path(gold_root() / "_test_write")

    try:
        from pyspark.dbutils import DBUtils
        DBUtils(spark).fs.rm(table_root, True)
    except Exception:
        pass

    rows1 = [{"entity_type": "player", "source": "mlb", "source_id": "123", "dt": "2026-05-22"}]
    df = spark.createDataFrame(rows1)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("dt").save(table_root)
    logger.info("test-write [1/3] fresh partitioned write OK")

    rows2 = [{"entity_type": "player", "source": "mlb", "source_id": "456", "dt": "2026-05-22"}]
    df2 = spark.createDataFrame(rows2)
    df2.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("replaceWhere", "dt = '2026-05-22'").save(table_root)
    logger.info("test-write [2/3] replaceWhere OK")

    count = spark.read.format("delta").load(table_root).count()
    assert count == 1, f"expected 1 row, got {count}"
    logger.info("test-write [3/3] read-back OK — %d row(s)", count)

    try:
        from pyspark.dbutils import DBUtils
        DBUtils(spark).fs.rm(table_root, True)
    except Exception:
        pass

    logger.info("test-write PASSED")


def _run_pipeline(args: argparse.Namespace) -> None:
    raise RuntimeError("Legacy runner removed. Use `gbdp run` with --stages if needed.")


def _resolve_window(start: str, end: str, window: str | None) -> tuple[str, str]:
    if not window:
        return start, end
    from datetime import timedelta
    from gbdp.utils.time import utc_now

    if window == "nightly":
        yesterday = (utc_now().date() - timedelta(days=1))
        start_dt = (yesterday - timedelta(days=6))
        return start_dt.isoformat(), yesterday.isoformat()
    if window == "yesterday":
        yesterday = (utc_now().date() - timedelta(days=1)).isoformat()
        return yesterday, yesterday
    return start, end


def _set_storage_format(args: argparse.Namespace) -> None:
    fmt = getattr(args, "storage_format", None)
    if fmt:
        os.environ["GBDP_STORAGE_FORMAT"] = fmt


def _set_roots(args: argparse.Namespace) -> None:
    bronze = getattr(args, "bronze_root", None)
    silver = getattr(args, "silver_root", None)
    gold = getattr(args, "gold_root", None)
    manual = getattr(args, "manual_root", None)
    if bronze:
        os.environ["GBDP_BRONZE_ROOT"] = bronze
    if silver:
        os.environ["GBDP_SILVER_ROOT"] = silver
    if gold:
        os.environ["GBDP_GOLD_ROOT"] = gold
    if manual:
        os.environ["GBDP_MANUAL_ROOT"] = manual


def _set_uc(args: argparse.Namespace) -> None:
    uc_catalog = getattr(args, "uc_catalog", None)
    uc_bronze = getattr(args, "uc_bronze_schema", None)
    uc_silver = getattr(args, "uc_silver_schema", None)
    uc_gold = getattr(args, "uc_gold_schema", None)
    uc_mode = getattr(args, "uc_mode", None)
    if uc_catalog:
        os.environ["GBDP_UC_CATALOG"] = uc_catalog
    if uc_bronze:
        os.environ["GBDP_UC_BRONZE_SCHEMA"] = uc_bronze
    if uc_silver:
        os.environ["GBDP_UC_SILVER_SCHEMA"] = uc_silver
    if uc_gold:
        os.environ["GBDP_UC_GOLD_SCHEMA"] = uc_gold
    if uc_mode:
        os.environ["GBDP_UC_MODE"] = uc_mode
    elif uc_catalog and not os.getenv("GBDP_UC_MODE"):
        os.environ["GBDP_UC_MODE"] = "managed"
    # Serverless-safe defaults: disable cache and request logs unless explicitly enabled.
    if os.environ.get("GBDP_UC_MODE") == "managed":
        if "GBDP_DISABLE_CACHE" not in os.environ:
            os.environ["GBDP_DISABLE_CACHE"] = "true"
        if "GBDP_DISABLE_REQUEST_LOG" not in os.environ:
            os.environ["GBDP_DISABLE_REQUEST_LOG"] = "true"
        if "GBDP_DBFS_FORCE_DBUTILS" not in os.environ:
            os.environ["GBDP_DBFS_FORCE_DBUTILS"] = "true"


def _set_http(args: argparse.Namespace) -> None:
    timeout = getattr(args, "http_timeout", None)
    retries = getattr(args, "http_retries", None)
    backoff = getattr(args, "http_backoff", None)
    min_interval = getattr(args, "http_min_interval", None)
    if timeout is not None:
        os.environ["GBDP_HTTP_TIMEOUT"] = str(timeout)
    if retries is not None:
        os.environ["GBDP_HTTP_RETRIES"] = str(retries)
    if backoff is not None:
        os.environ["GBDP_HTTP_BACKOFF"] = str(backoff)
    if min_interval is not None:
        os.environ["GBDP_HTTP_MIN_INTERVAL"] = str(min_interval)


if __name__ == "__main__":
    main()
