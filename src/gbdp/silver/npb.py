from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pyarrow.dataset as ds

from gbdp.silver.writer import write_parquet
from gbdp.utils.io import bronze_root, silver_root, ensure_dir, stable_json_dumps, include_partition_cols_silver
from gbdp.utils.time import daterange, parse_date


def normalize_npb(
    entity: str, start: str, end: str, root: Path | None = None, force: bool = False
) -> List[Path]:
    root = root or silver_root()
    outputs: List[Path] = []
    for d in daterange(parse_date(start), parse_date(end)):
        if entity == "games":
            outputs.append(_normalize_games(root, d, force))
        elif entity == "rosters":
            outputs.append(_normalize_rosters(root, d, force))
        elif entity == "game_pbp":
            outputs.append(_normalize_game_pbp(root, d, force))
        elif entity == "standings":
            outputs.append(_normalize_standings(root, d, force))
        else:
            raise ValueError(f"Unsupported NPB silver entity: {entity}")
    return outputs


def _bronze_path(root: Path, entity: str, dt: date) -> Path:
    return bronze_root() / "parsed" / "npb_spaia" / entity / f"dt={dt.isoformat()}"


def _silver_path(root: Path, entity: str, dt: date) -> Path:
    return root / "npb_spaia" / entity / f"dt={dt.isoformat()}"


def _read_bronze(entity: str, dt: date, root: Path) -> List[Dict[str, Any]]:
    path = _bronze_path(root, entity, dt)
    if not path.exists():
        return []
    dataset = ds.dataset(path, format="parquet")
    table = dataset.to_table()
    return table.to_pylist()


def _normalize_games(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("games", dt, root)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for r in rows:
        game_date_raw = r.get("gameDate")
        game_date = _parse_yyyymmdd(game_date_raw)
        start_time = r.get("startTime")
        start_time_fmt = _hhmm_to_hhmm(start_time)
        row = {
                "game_id": _as_str(r.get("gameId")),
                "game_date": game_date,
                "start_time_local": start_time_fmt,
                "home_team_id": _as_str(r.get("homeTeamId")),
                "away_team_id": _as_str(r.get("visitorTeamId")),
                "home_team_name": r.get("homeTeamName"),
                "away_team_name": r.get("visitorTeamName"),
                "home_score": _as_int(r.get("homeTeamTotalRuns")),
                "away_score": _as_int(r.get("visitorTeamTotalRuns")),
                "game_state_id": _as_int(r.get("gameStateId")),
                "game_state_name": r.get("gameStateName"),
                "game_type_id": _as_int(r.get("gameTypeId")),
                "game_type_name": r.get("gameTypeName"),
                "inning": _as_int(r.get("inning")),
                "top_bottom": _as_int(r.get("topBottom")),
            }
        if include_partition_cols:
            row["dt"] = dt.isoformat()
            row["source"] = "npb_spaia"
        normalized.append(row)
    out_dir = _silver_path(root, "games", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _normalize_rosters(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("rosters", dt, root)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for r in rows:
        teams_raw = r.get("teams")
        teams = _parse_json_maybe(teams_raw)
        if isinstance(teams, dict):
            teams = teams.get("teams", [])
        for team_entry in teams or []:
            team_id = _as_str(team_entry.get("team_id"))
            body_text = team_entry.get("body_text")
            payload = _parse_json_maybe(body_text)
            players = _extract_player_list(payload)
            for p in players:
                row = {
                        "team_id": team_id,
                        "player_id": _as_str(
                            p.get("player_id")
                            or p.get("playerId")
                            or p.get("person_info_id")
                            or p.get("PersonInfoId")
                            or p.get("id")
                        ),
                        "player_name": p.get("name")
                        or p.get("playerName")
                        or p.get("PlayerName")
                        or p.get("nameFull")
                        or p.get("name_full"),
                        "position": p.get("position") or p.get("pos"),
                        "bats": p.get("bats"),
                        "throws": p.get("throws"),
                        "raw_json": stable_json_dumps(p),
                    }
                if include_partition_cols:
                    row["dt"] = dt.isoformat()
                    row["source"] = "npb_spaia"
                normalized.append(row)
    out_dir = _silver_path(root, "rosters", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _normalize_game_pbp(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("game_pbp", dt, root)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for r in rows:
        games_raw = r.get("games")
        games = _parse_json_maybe(games_raw)
        if isinstance(games, dict):
            games = games.get("games", [])
        for g in games or []:
            game_id = _as_str(g.get("game_id"))
            body_text = g.get("body_text")
            events = _parse_json_maybe(body_text)
            if isinstance(events, dict):
                events = events.get("data", [])
            for e in events or []:
                row = {
                        "game_id": game_id or _as_str(e.get("GameID") or e.get("gameId")),
                        "event_id": _as_str(e.get("ID") or e.get("id")),
                        "game_date": _parse_yyyymmdd(e.get("GameDate")),
                        "inning": _as_int(e.get("Inning")),
                        "top_bottom": _as_str(e.get("TB")),
                        "text_info_name": e.get("TextInfo_Name"),
                        "text_info_text": e.get("TextInfo_Bat_Text"),
                        "play_seq_no": _as_int(e.get("PlayInfo_SeqNo")),
                        "play_ab": _as_int(e.get("PlayInfo_AB")),
                        "play_player_id": _as_str(e.get("PlayInfo_PlayerID")),
                        "play_player_name": e.get("PlayInfo_PlayerName"),
                        "updated_at": e.get("UpdatedAt"),
                        "created_at": e.get("CreatedAt"),
                        "raw_event_json": stable_json_dumps(e),
                    }
                if include_partition_cols:
                    row["dt"] = dt.isoformat()
                    row["source"] = "npb_spaia"
                normalized.append(row)
    out_dir = _silver_path(root, "game_pbp", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _normalize_standings(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("standings", dt, root)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for r in rows:
        data = r.get("data") or r.get("standings") or r
        if isinstance(data, str):
            data = _parse_json_maybe(data) or []
        if isinstance(data, dict):
            data = data.get("data") or data.get("standings") or []
        for t in data or []:
            row = {
                "team_id": _as_str(t.get("TeamID") or t.get("team_id")),
                "team_name": t.get("TeamName") or t.get("team_name"),
                "w": _as_int(t.get("Win") or t.get("w")),
                "l": _as_int(t.get("Lose") or t.get("l")),
                "t": _as_int(t.get("Tie") or t.get("t")),
                "pct": t.get("WinRate") or t.get("pct"),
                "gb": t.get("GameBehind") or t.get("gb"),
            }
            if include_partition_cols:
                row["dt"] = dt.isoformat()
                row["source"] = "npb_spaia"
            normalized.append(row)
    out_dir = _silver_path(root, "standings", dt)
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


def _extract_player_list(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if isinstance(payload, dict):
        for key in ("players", "data", "roster", "list"):
            if key in payload and isinstance(payload[key], list):
                return [p for p in payload[key] if isinstance(p, dict)]
        return [payload] if payload else []
    return []


def _parse_yyyymmdd(value: Any) -> str | None:
    if not value:
        return None
    s = str(value)
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None


def _hhmm_to_hhmm(value: Any) -> str | None:
    if not value:
        return None
    s = str(value)
    if len(s) == 4 and s.isdigit():
        return f"{s[0:2]}:{s[2:4]}"
    return None


def _as_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
