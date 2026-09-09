from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional

import os
import time
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
from gbdp.utils.logging import get_logger

logger = get_logger("gbdp.pipeline")


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
    total_steps = len(ordered) * len(windows)
    logger.info(
        "pipeline start: %d stage(s) x %d window(s) = %d step(s), range=%s..%s, leagues=%s",
        len(ordered), len(windows), total_steps, start, end, leagues or "all",
    )
    step = 0
    pipeline_t0 = time.monotonic()
    for stage in ordered:
        if stage not in available:
            raise ValueError(f"Unknown stage: {stage}")
        for win_start, win_end in windows:
            step += 1
            started = _now()
            status = "ok"
            details = None
            attempt = 0
            t0 = time.monotonic()
            logger.info("[%d/%d] stage=%s window=%s..%s starting", step, total_steps, stage, win_start, win_end)
            while True:
                try:
                    available[stage](win_start, win_end, cfg, writer, cache, force, leagues)
                    break
                except Exception as exc:
                    attempt += 1
                    if attempt > retries:
                        status = "error"
                        details = str(exc)
                        logger.error(
                            "[%d/%d] stage=%s window=%s..%s FAILED: %s",
                            step, total_steps, stage, win_start, win_end, details,
                        )
                        break
                    logger.warning(
                        "[%d/%d] stage=%s window=%s..%s attempt %d failed, retrying in %.1fs: %s",
                        step, total_steps, stage, win_start, win_end, attempt, retry_delay_s, exc,
                    )
                    time.sleep(retry_delay_s)
            ended = _now()
            if status == "ok":
                logger.info(
                    "[%d/%d] stage=%s window=%s..%s done (%.1fs)",
                    step, total_steps, stage, win_start, win_end, time.monotonic() - t0,
                )
            results.append(StageResult(stage, status, started, ended, details))
    logger.info(
        "pipeline finished in %.1fs (%d/%d steps ok)",
        time.monotonic() - pipeline_t0,
        sum(1 for r in results if r.status == "ok"),
        len(results),
    )
    _write_stage_audit(root, end, results, force)
    failures = [r for r in results if r.status != "ok"]
    if failures:
        details = "; ".join(f"{r.stage}:{r.details}" for r in failures if r.details)
        raise RuntimeError(f"Pipeline failed stages: {[r.stage for r in failures]} {details}")
    return results


def _pipeline_stages() -> Dict[str, Callable]:
    return {
        "fetch_schedules": _stage_fetch_schedules,
        "fetch_rosters": _stage_fetch_rosters,
        "fetch_transactions": _stage_fetch_transactions,
        "fetch_games": _stage_fetch_games,
        "fetch_pbp": _stage_fetch_pbp,
        "fetch_pitches": _stage_fetch_pitches,
        "fetch_boxscores": _stage_fetch_boxscores,
        "fetch_npb_stats": _stage_fetch_npb_stats,
        "silver_normalize": _stage_silver,
        "identity_resolve": _stage_identity,
        "gold_publish": _stage_gold,
        "quality_checks": _stage_quality,
        "audit_report": _stage_audit,
        "register_uc": _stage_register_uc,
    }


def _run_partitions(connector, partitions, force: bool, label: str) -> None:
    total = len(partitions)
    if total == 0:
        logger.info("%s: no partitions to fetch", label)
        return
    logger.info("%s: fetching %d partition(s)", label, total)
    t0 = time.monotonic()
    for i, p in enumerate(partitions, 1):
        payload = connector.fetch_partition(p)
        connector.write_bronze(payload, connector.parse_payload(payload), force=force)
        if i == total or i % 10 == 0:
            logger.info("%s: %d/%d partition(s) fetched (%.1fs elapsed)", label, i, total, time.monotonic() - t0)
    logger.info("%s: done in %.1fs", label, time.monotonic() - t0)


def _run_step(label: str, fn: Callable[[], None]) -> None:
    t0 = time.monotonic()
    logger.info("%s: start", label)
    fn()
    logger.info("%s: done (%.1fs)", label, time.monotonic() - t0)


def _stage_fetch_schedules(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "mlb"):
        mlb = MlbStatsApiConnector(writer, cache, cfg["mlb_statsapi"]["base_url"])
        _run_partitions(mlb, mlb.list_partitions(start, end, "schedule"), force, "fetch_schedules:mlb_statsapi:schedule")
    if _allowed(leagues, "npb"):
        npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
        _run_partitions(npb, npb.list_partitions(start, end, "schedules"), force, "fetch_schedules:npb_spaia:schedules")


def _stage_fetch_rosters(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "mlb"):
        mlb = MlbStatsApiConnector(writer, cache, cfg["mlb_statsapi"]["base_url"])
        _run_partitions(mlb, mlb.list_partitions(start, end, "rosters"), force, "fetch_rosters:mlb_statsapi:rosters")
    if _allowed(leagues, "npb"):
        npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
        _run_partitions(npb, npb.list_partitions(start, end, "rosters"), force, "fetch_rosters:npb_spaia:rosters")
    if "kbo_local" in cfg and _allowed(leagues, "kbo"):
        kbo = KboLocalConnector(writer, cache)
        _run_partitions(kbo, kbo.list_partitions(start, end, "rosters"), force, "fetch_rosters:kbo_local:rosters")
    if "lmb_local" in cfg and _allowed(leagues, "lmb"):
        lmb = LmbLocalConnector(writer, cache)
        _run_partitions(lmb, lmb.list_partitions(start, end, "rosters"), force, "fetch_rosters:lmb_local:rosters")


def _stage_fetch_transactions(start, end, cfg, writer, cache, force, leagues=None):
    if not _allowed(leagues, "mlb"):
        return
    mlb = MlbStatsApiConnector(writer, cache, cfg["mlb_statsapi"]["base_url"])
    _run_partitions(mlb, mlb.list_partitions(start, end, "transactions"), force, "fetch_transactions:mlb_statsapi:transactions")


def _stage_fetch_games(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "mlb"):
        mlb = MlbStatsApiConnector(writer, cache, cfg["mlb_statsapi"]["base_url"])
        _run_partitions(mlb, mlb.list_partitions(start, end, "schedule"), force, "fetch_games:mlb_statsapi:schedule")
    if _allowed(leagues, "npb"):
        npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
        _run_partitions(npb, npb.list_partitions(start, end, "games"), force, "fetch_games:npb_spaia:games")
    if "retrosheet_local" in cfg and _allowed(leagues, "mlb"):
        retro = RetrosheetLocalConnector(writer, cache)
        _run_partitions(retro, retro.list_partitions(start, end, "gameinfo"), force, "fetch_games:retrosheet_local:gameinfo")
    if "kbo_local" in cfg and _allowed(leagues, "kbo"):
        kbo = KboLocalConnector(writer, cache)
        for entity in ["games", "boxscore_batting", "boxscore_pitching"]:
            _run_partitions(kbo, kbo.list_partitions(start, end, entity), force, f"fetch_games:kbo_local:{entity}")
    if "lmb_local" in cfg and _allowed(leagues, "lmb"):
        lmb = LmbLocalConnector(writer, cache)
        for entity in ["games", "boxscore_batting", "boxscore_pitching"]:
            _run_partitions(lmb, lmb.list_partitions(start, end, entity), force, f"fetch_games:lmb_local:{entity}")


def _stage_fetch_pbp(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "npb"):
        npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
        _run_partitions(npb, npb.list_partitions(start, end, "game_pbp"), force, "fetch_pbp:npb_spaia:game_pbp")
    if "retrosheet_local" in cfg and _allowed(leagues, "mlb"):
        retro = RetrosheetLocalConnector(writer, cache)
        _run_partitions(retro, retro.list_partitions(start, end, "plays"), force, "fetch_pbp:retrosheet_local:plays")


def _stage_fetch_pitches(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "mlb"):
        mlb_statcast = MlbStatcastConnector(writer, cache, cfg["mlb_statcast"]["base_url"])
        _run_partitions(mlb_statcast, mlb_statcast.list_partitions(start, end, "pitches"), force, "fetch_pitches:mlb_statcast:pitches")
    if _allowed(leagues, "npb"):
        npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
        _run_partitions(npb, npb.list_partitions(start, end, "game_pitches"), force, "fetch_pitches:npb_spaia:game_pitches")


def _stage_fetch_boxscores(start, end, cfg, writer, cache, force, leagues=None):
    if not _allowed(leagues, "mlb"):
        return
    mlb = MlbStatsApiConnector(writer, cache, cfg["mlb_statsapi"]["base_url"])
    _run_partitions(mlb, mlb.list_partitions(start, end, "boxscore"), force, "fetch_boxscores:mlb_statsapi:boxscore")


def _stage_fetch_npb_stats(start, end, cfg, writer, cache, force, leagues=None):
    if not _allowed(leagues, "npb"):
        return
    npb = NpbSpaiaConnector(writer, cache, cfg["npb_spaia"]["base_url"])
    entities = [
        "standings",
        "player_batting_saber",
        "player_pitching_saber",
        "player_stats_by_year",
        "player_stats_by_month",
        "player_stats_by_game",
        "player_hitting_career",
        "game_batter_stats",
        "game_pitcher_stats",
    ]
    for entity in entities:
        _run_partitions(npb, npb.list_partitions(start, end, entity), force, f"fetch_npb_stats:npb_spaia:{entity}")


def _stage_silver(start, end, cfg, writer, cache, force, leagues=None):
    if _allowed(leagues, "mlb"):
        for entity in ["games", "rosters", "transactions", "pitches", "boxscore_batting", "boxscore_pitching"]:
            _run_step(f"silver_normalize:mlb:{entity}", lambda entity=entity: normalize_mlb(entity, start, end, force=force))
    if _allowed(leagues, "npb"):
        for entity in [
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
        ]:
            _run_step(f"silver_normalize:npb:{entity}", lambda entity=entity: normalize_npb(entity, start, end, force=force))
    if "indy_local" in cfg:
        for entity in ["games", "rosters", "boxscore_batting", "boxscore_pitching"]:
            _run_step(f"silver_normalize:indy:{entity}", lambda entity=entity: normalize_indy(entity, start, end, force=force))
    if "kbo_local" in cfg and _allowed(leagues, "kbo"):
        for entity in ["games", "rosters", "boxscore_batting", "boxscore_pitching"]:
            _run_step(
                f"silver_normalize:kbo_local:{entity}",
                lambda entity=entity: normalize_local_boxscore("kbo_local", entity, start, end, force=force),
            )
    if "lmb_local" in cfg and _allowed(leagues, "lmb"):
        for entity in ["games", "rosters", "boxscore_batting", "boxscore_pitching"]:
            _run_step(
                f"silver_normalize:lmb_local:{entity}",
                lambda entity=entity: normalize_local_boxscore("lmb_local", entity, start, end, force=force),
            )
    if "retrosheet_local" in cfg:
        for entity in ["allplayers", "gameinfo", "teamstats", "batting", "pitching", "fielding", "plays"]:
            _run_step(f"silver_normalize:retrosheet_local:{entity}", lambda entity=entity: normalize_retrosheet(entity, start, end, force=force))


def _stage_identity(start, end, cfg, writer, cache, force, leagues=None):
    _run_step(f"identity_resolve:{start}..{end}", lambda: resolve_identity(start, end, force=force))


def _stage_gold(start, end, cfg, writer, cache, force, leagues=None):
    _run_step(f"gold_publish:{start}..{end}", lambda: publish_gold(start, end, force=force))


def _stage_quality(start, end, cfg, writer, cache, force, leagues=None):
    _run_step(f"quality_checks:{start}..{end}", lambda: run_quality_checks(start, end, force=force))


def _stage_audit(start, end, cfg, writer, cache, force, leagues=None):
    _run_step(f"audit_report:{end}", lambda: write_run_audit("nightly", end, "ok", force=force))


def _stage_register_uc(start, end, cfg, writer, cache, force, leagues=None):
    def _do():
        mode = os.getenv("GBDP_UC_MODE", "managed").lower()
        if mode in {"managed", "serverless"}:
            from gbdp.catalog.managed_publish import publish_uc_managed_from_env

            publish_uc_managed_from_env(start, end)
            return
        from gbdp.catalog.uc import register_uc_tables_from_env

        register_uc_tables_from_env()

    _run_step(f"register_uc:{start}..{end}", _do)


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
