from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional

import yaml

from gbdp.bronze.cache import ResponseCache
from gbdp.bronze.writer import BronzeWriter
from gbdp.connectors.indy import IndyLocalConnector
from gbdp.connectors.kbo import KboLocalConnector
from gbdp.connectors.lmb import LmbLocalConnector
from gbdp.connectors.mlb_statcast import MlbStatcastConnector
from gbdp.connectors.mlb_statsapi import MlbStatsApiConnector
from gbdp.connectors.npb_spaia import NpbSpaiaConnector
from gbdp.connectors.retrosheet import RetrosheetLocalConnector
from gbdp.identity.resolver import resolve_identity
from gbdp.quality.checks import run_quality_checks
from gbdp.quality.metrics import write_run_audit
from gbdp.silver.indy import normalize_indy
from gbdp.silver.local_boxscore import normalize_local_boxscore
from gbdp.silver.mlb import normalize_mlb
from gbdp.silver.npb import normalize_npb
from gbdp.silver.retrosheet import normalize_retrosheet
from gbdp.gold.publish import publish_gold
from gbdp.utils.io import bronze_root, gold_root, ensure_dir, path_exists, write_parquet_table


@dataclass
class StageResult:
    stage: str
    status: str
    started_at_utc: str
    ended_at_utc: str
    details: Optional[str] = None


def run_pipeline(
    start: str,
    end: str,
    sources_path: str,
    force: bool,
    stages: Optional[List[str]] = None,
    retries: int = 0,
    retry_delay_s: float = 1.0,
    chunk: str | None = None,
    leagues: Optional[List[str]] = None,
) -> List[StageResult]:
    cfg = _load_yaml(Path(sources_path))
    root = gold_root()
    writer = BronzeWriter(bronze_root())
    cache = ResponseCache(bronze_root() / "cache")

    available = _pipeline_stages()
    ordered = stages or list(available.keys())
    results: List[StageResult] = []
    windows = _build_windows(start, end, chunk)
    for stage in ordered:
        if stage not in available:
            raise ValueError(f"Unknown stage: {stage}")
        for win_start, win_end in windows:
            started = _now()
            status = "ok"
            details = None
            attempt = 0
            while True:
                try:
                    available[stage](win_start, win_end, cfg, writer, cache, force, leagues)
                    break
                except Exception as exc:
                    attempt += 1
                    if attempt > retries:
                        status = "error"
                        details = str(exc)
                        break
                    import time

                    time.sleep(retry_delay_s)
            ended = _now()
            results.append(StageResult(stage, status, started, ended, details))
    _write_stage_audit(root, end, results, force)
    return results


def _pipeline_stages() -> Dict[str, Callable]:
    return {
        "fetch_schedules": _stage_fetch_schedules,
        "fetch_rosters": _stage_fetch_rosters,
        "fetch_transactions": _stage_fetch_transactions,
        "fetch_games": _stage_fetch_games,
        "fetch_pbp": _stage_fetch_pbp,
        "fetch_pitches": _stage_fetch_pitches,
        "fetch_npb_stats": _stage_fetch_npb_stats,
        "silver_normalize": _stage_silver,
        "identity_resolve": _stage_identity,
        "gold_publish": _stage_gold,
        "quality_checks": _stage_quality,
        "audit_report": _stage_audit,
        "register_uc": _stage_register_uc,
    }


def _stage_fetch_schedules(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "mlb"):
        mlb = MlbStatsApiConnector(writer, cache, cfg["mlb_statsapi"]["base_url"])
        for p in mlb.list_partitions(start, end, "schedule"):
            payload = mlb.fetch_partition(p)
            mlb.write_bronze(payload, mlb.parse_payload(payload), force=force)
    if _allowed(leagues, "npb"):
        npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
        for p in npb.list_partitions(start, end, "schedules"):
            payload = npb.fetch_partition(p)
            npb.write_bronze(payload, npb.parse_payload(payload), force=force)


def _stage_fetch_rosters(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "mlb"):
        mlb = MlbStatsApiConnector(writer, cache, cfg["mlb_statsapi"]["base_url"])
        for p in mlb.list_partitions(start, end, "rosters"):
            payload = mlb.fetch_partition(p)
            mlb.write_bronze(payload, mlb.parse_payload(payload), force=force)
    if _allowed(leagues, "npb"):
        npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
        for p in npb.list_partitions(start, end, "rosters"):
            payload = npb.fetch_partition(p)
            npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    if "kbo_local" in cfg and _allowed(leagues, "kbo"):
        kbo = KboLocalConnector(writer, cache)
        for p in kbo.list_partitions(start, end, "rosters"):
            payload = kbo.fetch_partition(p)
            kbo.write_bronze(payload, kbo.parse_payload(payload), force=force)
    if "lmb_local" in cfg and _allowed(leagues, "lmb"):
        lmb = LmbLocalConnector(writer, cache)
        for p in lmb.list_partitions(start, end, "rosters"):
            payload = lmb.fetch_partition(p)
            lmb.write_bronze(payload, lmb.parse_payload(payload), force=force)


def _stage_fetch_transactions(start, end, cfg, writer, cache, force, leagues=None):
    if not _allowed(leagues, "mlb"):
        return
    mlb = MlbStatsApiConnector(writer, cache, cfg["mlb_statsapi"]["base_url"])
    for p in mlb.list_partitions(start, end, "transactions"):
        payload = mlb.fetch_partition(p)
        mlb.write_bronze(payload, mlb.parse_payload(payload), force=force)


def _stage_fetch_games(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "mlb"):
        mlb = MlbStatsApiConnector(writer, cache, cfg["mlb_statsapi"]["base_url"])
        for p in mlb.list_partitions(start, end, "schedule"):
            payload = mlb.fetch_partition(p)
            mlb.write_bronze(payload, mlb.parse_payload(payload), force=force)
    if _allowed(leagues, "npb"):
        npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
        for p in npb.list_partitions(start, end, "games"):
            payload = npb.fetch_partition(p)
            npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    if "retrosheet_local" in cfg and _allowed(leagues, "mlb"):
        retro = RetrosheetLocalConnector(writer, cache)
        for p in retro.list_partitions(start, end, "gameinfo"):
            payload = retro.fetch_partition(p)
            retro.write_bronze(payload, retro.parse_payload(payload), force=force)
    if "kbo_local" in cfg and _allowed(leagues, "kbo"):
        kbo = KboLocalConnector(writer, cache)
        for entity in ["games", "boxscore_batting", "boxscore_pitching"]:
            for p in kbo.list_partitions(start, end, entity):
                payload = kbo.fetch_partition(p)
                kbo.write_bronze(payload, kbo.parse_payload(payload), force=force)
    if "lmb_local" in cfg and _allowed(leagues, "lmb"):
        lmb = LmbLocalConnector(writer, cache)
        for entity in ["games", "boxscore_batting", "boxscore_pitching"]:
            for p in lmb.list_partitions(start, end, entity):
                payload = lmb.fetch_partition(p)
                lmb.write_bronze(payload, lmb.parse_payload(payload), force=force)


def _stage_fetch_pbp(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "npb"):
        npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
        for p in npb.list_partitions(start, end, "game_pbp"):
            payload = npb.fetch_partition(p)
            npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    if "retrosheet_local" in cfg and _allowed(leagues, "mlb"):
        retro = RetrosheetLocalConnector(writer, cache)
        for p in retro.list_partitions(start, end, "plays"):
            payload = retro.fetch_partition(p)
            retro.write_bronze(payload, retro.parse_payload(payload), force=force)


def _stage_fetch_pitches(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "mlb"):
        mlb_statcast = MlbStatcastConnector(writer, cache, cfg["mlb_statcast"]["base_url"])
        for p in mlb_statcast.list_partitions(start, end, "pitches"):
            payload = mlb_statcast.fetch_partition(p)
            mlb_statcast.write_bronze(payload, mlb_statcast.parse_payload(payload), force=force)
    if _allowed(leagues, "npb"):
        npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
        for p in npb.list_partitions(start, end, "game_pitches"):
            payload = npb.fetch_partition(p)
            npb.write_bronze(payload, npb.parse_payload(payload), force=force)


def _stage_fetch_npb_stats(start, end, cfg, writer, cache, force, leagues=None):
    if not _allowed(leagues, "npb"):
        return
    npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
    for p in npb.list_partitions(start, end, "standings"):
        payload = npb.fetch_partition(p)
        npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    for p in npb.list_partitions(start, end, "player_batting_saber"):
        payload = npb.fetch_partition(p)
        npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    for p in npb.list_partitions(start, end, "player_pitching_saber"):
        payload = npb.fetch_partition(p)
        npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    for p in npb.list_partitions(start, end, "player_stats_by_year"):
        payload = npb.fetch_partition(p)
        npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    for p in npb.list_partitions(start, end, "player_stats_by_month"):
        payload = npb.fetch_partition(p)
        npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    for p in npb.list_partitions(start, end, "player_stats_by_game"):
        payload = npb.fetch_partition(p)
        npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    for p in npb.list_partitions(start, end, "player_hitting_career"):
        payload = npb.fetch_partition(p)
        npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    for p in npb.list_partitions(start, end, "game_batter_stats"):
        payload = npb.fetch_partition(p)
        npb.write_bronze(payload, npb.parse_payload(payload), force=force)
    for p in npb.list_partitions(start, end, "game_pitcher_stats"):
        payload = npb.fetch_partition(p)
        npb.write_bronze(payload, npb.parse_payload(payload), force=force)


def _stage_silver(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "mlb"):
        normalize_mlb("games", start, end, force=force)
        normalize_mlb("rosters", start, end, force=force)
        normalize_mlb("transactions", start, end, force=force)
        normalize_mlb("pitches", start, end, force=force)
    if _allowed(leagues, "npb"):
        normalize_npb("games", start, end, force=force)
        normalize_npb("rosters", start, end, force=force)
        normalize_npb("game_pbp", start, end, force=force)
        normalize_npb("standings", start, end, force=force)
        normalize_npb("game_batter_stats", start, end, force=force)
        normalize_npb("game_pitcher_stats", start, end, force=force)
        normalize_npb("player_batting_saber", start, end, force=force)
        normalize_npb("player_pitching_saber", start, end, force=force)
        normalize_npb("player_stats_by_year", start, end, force=force)
        normalize_npb("player_stats_by_month", start, end, force=force)
        normalize_npb("player_stats_by_game", start, end, force=force)
        normalize_npb("player_hitting_career", start, end, force=force)
    if "indy_local" in cfg:
        normalize_indy("games", start, end, force=force)
        normalize_indy("rosters", start, end, force=force)
        normalize_indy("boxscore_batting", start, end, force=force)
        normalize_indy("boxscore_pitching", start, end, force=force)
    if "kbo_local" in cfg and _allowed(leagues, "kbo"):
        for entity in ["games", "rosters", "boxscore_batting", "boxscore_pitching"]:
            normalize_local_boxscore("kbo_local", entity, start, end, force=force)
    if "lmb_local" in cfg and _allowed(leagues, "lmb"):
        for entity in ["games", "rosters", "boxscore_batting", "boxscore_pitching"]:
            normalize_local_boxscore("lmb_local", entity, start, end, force=force)
    if "retrosheet_local" in cfg:
        normalize_retrosheet("allplayers", start, end, force=force)
        normalize_retrosheet("gameinfo", start, end, force=force)
        normalize_retrosheet("teamstats", start, end, force=force)
        normalize_retrosheet("batting", start, end, force=force)
        normalize_retrosheet("pitching", start, end, force=force)
        normalize_retrosheet("fielding", start, end, force=force)
        normalize_retrosheet("plays", start, end, force=force)


def _stage_identity(start, end, cfg, writer, cache, force, leagues=None):
    resolve_identity(start, end, force=force)


def _stage_gold(start, end, cfg, writer, cache, force, leagues=None):
    publish_gold(start, end, force=force)


def _stage_quality(start, end, cfg, writer, cache, force, leagues=None):
    run_quality_checks(start, end, force=force)


def _stage_audit(start, end, cfg, writer, cache, force, leagues=None):
    write_run_audit("nightly", end, "ok", force=force)


def _stage_register_uc(start, end, cfg, writer, cache, force, leagues=None):
    from gbdp.catalog.uc import register_uc_tables_from_env

    register_uc_tables_from_env()


def _allowed(leagues: Optional[List[str]], league: str) -> bool:
    if not leagues:
        return True
    return league.lower() in {l.lower() for l in leagues}


def _write_stage_audit(root: Path, dt: str, results: List[StageResult], force: bool) -> None:
    out_dir = root / "audit_stage_runs" / f"dt={dt}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if path_exists(out_path) and not force:
        return
    rows = [r.__dict__ for r in results]
    import pyarrow as pa
    table = pa.Table.from_pylist(rows)
    write_parquet_table(table, out_path, force=True)


def _load_yaml(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_windows(start: str, end: str, chunk: str | None) -> List[tuple[str, str]]:
    if not chunk:
        return [(start, end)]
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    windows: List[tuple[str, str]] = []
    if chunk == "month":
        cur = date(start_d.year, start_d.month, 1)
        while cur <= end_d:
            next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
            win_start = cur if cur >= start_d else start_d
            win_end = (next_month - timedelta(days=1))
            if win_end > end_d:
                win_end = end_d
            windows.append((win_start.isoformat(), win_end.isoformat()))
            cur = next_month
        return windows
    raise ValueError(f"Unsupported chunk: {chunk}")
