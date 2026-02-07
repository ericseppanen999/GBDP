from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

import yaml

from gbdp.bronze.cache import ResponseCache
from gbdp.bronze.writer import BronzeWriter
from gbdp.connectors.mlb_statcast import MlbStatcastConnector
from gbdp.connectors.mlb_statsapi import MlbStatsApiConnector
from gbdp.connectors.npb_spaia import NpbSpaiaConnector
from gbdp.connectors.indy import IndyLocalConnector
from gbdp.connectors.retrosheet import RetrosheetLocalConnector
from gbdp.silver.npb import normalize_npb
from gbdp.silver.mlb import normalize_mlb
from gbdp.silver.indy import normalize_indy
from gbdp.silver.retrosheet import normalize_retrosheet
from gbdp.identity.resolver import resolve_identity
from gbdp.gold.publish import publish_gold
from gbdp.quality.checks import run_quality_checks
from gbdp.quality.metrics import write_run_audit
from gbdp.pipeline.runner import run_pipeline
from gbdp.utils.io import data_root
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
    if source == "retrosheet_local":
        return RetrosheetLocalConnector(writer, cache)
    raise ValueError(f"Unknown source: {source}")


def ingest(args: argparse.Namespace) -> None:
    cfg = load_sources_config(Path(args.sources))
    root = data_root()
    writer = BronzeWriter(root)
    cache = ResponseCache(root / "cache")

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
        choices=["mlb_statsapi", "mlb_statcast", "npb_spaia", "indy_local", "retrosheet_local"],
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

    silver_p = sub.add_parser("silver", help="Normalize bronze to silver")
    silver_p.add_argument(
        "--source",
        required=True,
        choices=["npb_spaia", "mlb_statsapi", "mlb_statcast", "indy_local", "retrosheet_local"],
    )
    silver_p.add_argument(
        "--entity",
        required=True,
        choices=[
            "games",
            "rosters",
            "game_pbp",
            "standings",
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

    identity_p = sub.add_parser("identity", help="Resolve identity and build bridge table")
    identity_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    identity_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    identity_p.add_argument("--force", action="store_true", help="Overwrite existing bridge outputs")

    gold_p = sub.add_parser("gold", help="Publish gold dims/facts")
    gold_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    gold_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    gold_p.add_argument("--force", action="store_true", help="Overwrite existing gold outputs")

    quality_p = sub.add_parser("quality", help="Run quality checks")
    quality_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    quality_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    quality_p.add_argument("--force", action="store_true", help="Overwrite existing quality outputs")

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
    run_p.add_argument(
        "--window",
        choices=["nightly"],
        help="Override start/end with a built-in window (nightly = last 7 days ending yesterday)",
    )

    backfill_p = sub.add_parser("backfill", help="Backfill pipeline stages (alias of run)")
    backfill_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    backfill_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    backfill_p.add_argument("--sources", default="configs/sources.yaml", help="Path to sources.yaml")
    backfill_p.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    backfill_p.add_argument("--stages", help="Comma-separated list of stages to run (optional)")
    backfill_p.add_argument("--retries", type=int, default=0, help="Retry count per stage")
    backfill_p.add_argument("--retry-delay", type=float, default=1.0, help="Retry delay seconds")
    backfill_p.add_argument(
        "--window",
        choices=["nightly"],
        help="Override start/end with a built-in window (nightly = last 7 days ending yesterday)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.cmd == "ingest":
        ingest(args)
    if args.cmd == "silver":
        if args.source == "npb_spaia":
            normalize_npb(args.entity, args.start, args.end, force=args.force)
        elif args.source in {"mlb_statsapi", "mlb_statcast"}:
            normalize_mlb(args.entity, args.start, args.end, force=args.force)
        elif args.source == "indy_local":
            normalize_indy(args.entity, args.start, args.end, force=args.force)
        elif args.source == "retrosheet_local":
            normalize_retrosheet(args.entity, args.start, args.end, force=args.force)
    if args.cmd == "identity":
        resolve_identity(args.start, args.end, force=args.force)
    if args.cmd == "gold":
        publish_gold(args.start, args.end, force=args.force)
    if args.cmd == "quality":
        run_quality_checks(args.start, args.end, force=args.force)
    if args.cmd == "serve":
        import uvicorn

        uvicorn.run("gbdp.serve.api:app", host=args.host, port=args.port, reload=False)
    if args.cmd == "run":
        stages = args.stages.split(",") if args.stages else None
        start, end = _resolve_window(args.start, args.end, args.window)
        run_pipeline(start, end, args.sources, args.force, stages, args.retries, args.retry_delay)
    if args.cmd == "backfill":
        stages = args.stages.split(",") if args.stages else None
        start, end = _resolve_window(args.start, args.end, args.window)
        run_pipeline(start, end, args.sources, args.force, stages, args.retries, args.retry_delay)


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


if __name__ == "__main__":
    main()
