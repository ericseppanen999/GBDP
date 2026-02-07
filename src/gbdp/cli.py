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
        choices=["nightly"],
        help="Override start/end with a built-in window (nightly = last 7 days ending yesterday)",
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

    backfill_p = sub.add_parser("backfill", help="Backfill pipeline stages (alias of run)")
    backfill_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    backfill_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
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
        choices=["nightly"],
        help="Override start/end with a built-in window (nightly = last 7 days ending yesterday)",
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
        stages = args.stages.split(",") if args.stages else None
        leagues = args.leagues.split(",") if args.leagues else None
        start, end = _resolve_window(args.start, args.end, args.window)
        run_pipeline(start, end, args.sources, args.force, stages, args.retries, args.retry_delay, args.chunk, leagues)
    if args.cmd == "backfill":
        _set_storage_format(args)
        _set_roots(args)
        _set_uc(args)
        stages = args.stages.split(",") if args.stages else None
        leagues = args.leagues.split(",") if args.leagues else None
        start, end = _resolve_window(args.start, args.end, args.window)
        run_pipeline(start, end, args.sources, args.force, stages, args.retries, args.retry_delay, args.chunk, leagues)
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


if __name__ == "__main__":
    main()
