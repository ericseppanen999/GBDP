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
        connector.write_bronze(payload, records)
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

    identity_p = sub.add_parser("identity", help="Resolve identity and build bridge table")
    identity_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    identity_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")

    gold_p = sub.add_parser("gold", help="Publish gold dims/facts")
    gold_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    gold_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")

    quality_p = sub.add_parser("quality", help="Run quality checks")
    quality_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    quality_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")

    serve_p = sub.add_parser("serve", help="Run FastAPI server (local)")
    serve_p.add_argument("--host", default="0.0.0.0")
    serve_p.add_argument("--port", default=8000, type=int)

    run_p = sub.add_parser("run", help="Run nightly pipeline stages")
    run_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    run_p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    run_p.add_argument("--sources", default="configs/sources.yaml", help="Path to sources.yaml")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.cmd == "ingest":
        ingest(args)
    if args.cmd == "silver":
        if args.source == "npb_spaia":
            normalize_npb(args.entity, args.start, args.end)
        elif args.source in {"mlb_statsapi", "mlb_statcast"}:
            normalize_mlb(args.entity, args.start, args.end)
        elif args.source == "indy_local":
            normalize_indy(args.entity, args.start, args.end)
        elif args.source == "retrosheet_local":
            normalize_retrosheet(args.entity, args.start, args.end)
    if args.cmd == "identity":
        resolve_identity(args.start, args.end)
    if args.cmd == "gold":
        publish_gold(args.start, args.end)
    if args.cmd == "quality":
        run_quality_checks(args.start, args.end)
    if args.cmd == "serve":
        import uvicorn

        uvicorn.run("gbdp.serve.api:app", host=args.host, port=args.port, reload=False)
    if args.cmd == "run":
        _run_pipeline(args)


def _run_pipeline(args: argparse.Namespace) -> None:
    cfg = load_sources_config(Path(args.sources))
    root = data_root()
    writer = BronzeWriter(root)
    cache = ResponseCache(root / "cache")

    # Ingest MLB
    mlb_stats = build_connector("mlb_statsapi", cfg["mlb_statsapi"], writer, cache)
    for entity in ["schedule", "rosters", "transactions"]:
        for p in mlb_stats.list_partitions(args.start, args.end, entity):
            payload = mlb_stats.fetch_partition(p)
            mlb_stats.write_bronze(payload, mlb_stats.parse_payload(payload))
    mlb_statcast = build_connector("mlb_statcast", cfg["mlb_statcast"], writer, cache)
    for p in mlb_statcast.list_partitions(args.start, args.end, "pitches"):
        payload = mlb_statcast.fetch_partition(p)
        mlb_statcast.write_bronze(payload, mlb_statcast.parse_payload(payload))

    # Ingest NPB
    npb = build_connector("npb_spaia", cfg["npb_spaia"], writer, cache)
    for entity in ["schedules", "rosters", "standings", "games", "game_pbp", "game_pitches"]:
        for p in npb.list_partitions(args.start, args.end, entity):
            payload = npb.fetch_partition(p)
            npb.write_bronze(payload, npb.parse_payload(payload))

    # Ingest Indy (local file-based, optional)
    if "indy_local" in cfg:
        indy = build_connector("indy_local", cfg["indy_local"], writer, cache)
        for entity in ["games", "rosters", "boxscore_batting", "boxscore_pitching"]:
            for p in indy.list_partitions(args.start, args.end, entity):
                payload = indy.fetch_partition(p)
                indy.write_bronze(payload, indy.parse_payload(payload))

    # Ingest Retrosheet (local zip/csv)
    if "retrosheet_local" in cfg:
        retro = build_connector("retrosheet_local", cfg["retrosheet_local"], writer, cache)
        for entity in ["allplayers", "gameinfo", "teamstats", "batting", "pitching", "fielding", "plays"]:
            for p in retro.list_partitions(args.start, args.end, entity):
                payload = retro.fetch_partition(p)
                retro.write_bronze(payload, retro.parse_payload(payload))

    # Silver
    normalize_mlb("games", args.start, args.end)
    normalize_mlb("rosters", args.start, args.end)
    normalize_mlb("transactions", args.start, args.end)
    normalize_mlb("pitches", args.start, args.end)
    normalize_npb("games", args.start, args.end)
    normalize_npb("rosters", args.start, args.end)
    normalize_npb("game_pbp", args.start, args.end)
    normalize_npb("standings", args.start, args.end)
    normalize_indy("games", args.start, args.end)
    normalize_indy("rosters", args.start, args.end)
    normalize_indy("boxscore_batting", args.start, args.end)
    normalize_indy("boxscore_pitching", args.start, args.end)
    normalize_retrosheet("allplayers", args.start, args.end)
    normalize_retrosheet("gameinfo", args.start, args.end)
    normalize_retrosheet("teamstats", args.start, args.end)
    normalize_retrosheet("batting", args.start, args.end)
    normalize_retrosheet("pitching", args.start, args.end)
    normalize_retrosheet("fielding", args.start, args.end)
    normalize_retrosheet("plays", args.start, args.end)

    # Identity + Gold + Quality
    resolve_identity(args.start, args.end)
    publish_gold(args.start, args.end)
    run_quality_checks(args.start, args.end)
    write_run_audit("nightly", args.end, "ok")


if __name__ == "__main__":
    main()
