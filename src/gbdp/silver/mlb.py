from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List

from gbdp.silver.writer import write_parquet
from gbdp.utils.io import (
    bronze_root,
    silver_root,
    ensure_dir,
    include_partition_cols_silver,
    path_exists,
    read_parquet_rows,
    stable_json_dumps,
)
from gbdp.utils.time import daterange, parse_date


def normalize_mlb(
    entity: str, start: str, end: str, root: Path | None = None, force: bool = False
) -> List[Path]:
    root = root or silver_root()
    outputs: List[Path] = []
    for d in daterange(parse_date(start), parse_date(end)):
        if entity == "games":
            outputs.append(_normalize_games(root, d, force))
        elif entity == "rosters":
            outputs.append(_normalize_rosters(root, d, force))
        elif entity == "transactions":
            outputs.append(_normalize_transactions(root, d, force))
        elif entity == "pitches":
            outputs.append(_normalize_pitches(root, d, force))
        else:
            raise ValueError(f"Unsupported MLB silver entity: {entity}")
    return outputs


def _bronze_path(root: Path, source: str, entity: str, dt: date) -> Path:
    return bronze_root() / "parsed" / source / entity / f"dt={dt.isoformat()}"


def _silver_path(root: Path, source: str, entity: str, dt: date) -> Path:
    return root / source / entity / f"dt={dt.isoformat()}"


def _read_bronze(source: str, entity: str, dt: date, root: Path) -> List[Dict[str, Any]]:
    path = _bronze_path(root, source, entity, dt)
    if not path_exists(path):
        return []
    return read_parquet_rows(path)


def _normalize_games(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("mlb_statsapi", "schedule", dt, root)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for r in rows:
        dates = r.get("dates") or []
        if isinstance(dates, str):
            dates = _parse_json_maybe(dates) or []
        for date_block in dates:
            games = date_block.get("games", [])
            for g in games:
                row = {
                        "game_id": _as_str(g.get("gamePk")),
                        "game_date": g.get("gameDate"),
                        "game_type": g.get("gameType"),
                        "status": (g.get("status") or {}).get("detailedState"),
                        "home_team_id": _as_str((g.get("teams") or {}).get("home", {}).get("team", {}).get("id")),
                        "home_team_name": (g.get("teams") or {}).get("home", {}).get("team", {}).get("name"),
                        "away_team_id": _as_str((g.get("teams") or {}).get("away", {}).get("team", {}).get("id")),
                        "away_team_name": (g.get("teams") or {}).get("away", {}).get("team", {}).get("name"),
                        "home_score": _as_int((g.get("teams") or {}).get("home", {}).get("score")),
                        "away_score": _as_int((g.get("teams") or {}).get("away", {}).get("score")),
                        "venue": (g.get("venue") or {}).get("name"),
                    }
                if include_partition_cols:
                    row["dt"] = dt.isoformat()
                    row["source"] = "mlb_statsapi"
                normalized.append(row)
    out_dir = _silver_path(root, "mlb_statsapi", "games", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _normalize_rosters(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("mlb_statsapi", "rosters", dt, root)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for r in rows:
        teams = r.get("teams")
        if isinstance(teams, str):
            teams = _parse_json_maybe(teams)
        if isinstance(teams, dict):
            teams = teams.get("teams", [])
        for t in teams or []:
            team_id = _as_str(t.get("team_id"))
            payload = _parse_json_maybe(t.get("body_text")) or {}
            roster = payload.get("roster", []) if isinstance(payload, dict) else []
            for p in roster:
                person = p.get("person", {})
                row = {
                        "team_id": team_id,
                        "player_id": _as_str(person.get("id")),
                        "player_name": person.get("fullName"),
                        "position": (p.get("position") or {}).get("abbreviation"),
                        "status": p.get("status"),
                        "raw_json": stable_json_dumps(p),
                    }
                if include_partition_cols:
                    row["dt"] = dt.isoformat()
                    row["source"] = "mlb_statsapi"
                normalized.append(row)
    out_dir = _silver_path(root, "mlb_statsapi", "rosters", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _normalize_transactions(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("mlb_statsapi", "transactions", dt, root)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for r in rows:
        transactions = r.get("transactions") or []
        if isinstance(transactions, str):
            transactions = _parse_json_maybe(transactions) or []
        for t in transactions:
            row = {
                    "transaction_id": _as_str(t.get("id")),
                    "player_id": _as_str((t.get("person") or {}).get("id")),
                    "team_id": _as_str((t.get("toTeam") or {}).get("id") or (t.get("team") or {}).get("id")),
                    "type_code": t.get("typeCode"),
                    "type_desc": t.get("typeDesc"),
                    "date": t.get("date"),
                    "description": t.get("description"),
                    "raw_json": stable_json_dumps(t),
                }
            if include_partition_cols:
                row["dt"] = dt.isoformat()
                row["source"] = "mlb_statsapi"
            normalized.append(row)
    out_dir = _silver_path(root, "mlb_statsapi", "transactions", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _normalize_pitches(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("mlb_statcast", "pitches", dt, root)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for r in rows:
        row = {
                "game_date": r.get("game_date"),
                "game_pk": _as_str(r.get("game_pk")),
                "pitcher_id": _as_str(r.get("pitcher")),
                "batter_id": _as_str(r.get("batter")),
                "pitch_type": r.get("pitch_type"),
                "release_speed": _as_float(r.get("release_speed")),
                "plate_x": _as_float(r.get("plate_x")),
                "plate_z": _as_float(r.get("plate_z")),
                "description": r.get("description"),
                "events": r.get("events"),
                "balls": _as_int(r.get("balls")),
                "strikes": _as_int(r.get("strikes")),
                "outs_when_up": _as_int(r.get("outs_when_up")),
                "inning": _as_int(r.get("inning")),
                "inning_topbot": r.get("inning_topbot"),
                "on_1b": _as_str(r.get("on_1b")),
                "on_2b": _as_str(r.get("on_2b")),
                "on_3b": _as_str(r.get("on_3b")),
                "at_bat_number": _as_int(r.get("at_bat_number")),
                "pitch_number": _as_int(r.get("pitch_number")),
                "raw_json": stable_json_dumps(r),
            }
        if include_partition_cols:
            row["dt"] = dt.isoformat()
            row["source"] = "mlb_statcast"
        normalized.append(row)
    out_dir = _silver_path(root, "mlb_statcast", "pitches", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _parse_json_maybe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def _as_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
