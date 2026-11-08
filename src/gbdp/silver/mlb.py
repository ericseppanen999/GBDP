from __future__ import annotations

import json
import logging
import re
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
    read_bronze_rows,
    stable_json_dumps,
)
from gbdp.utils.time import daterange, parse_date

logger = logging.getLogger(__name__)


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
        elif entity == "boxscore_batting":
            outputs.append(_normalize_boxscore_batting(root, d, force))
        elif entity == "boxscore_pitching":
            outputs.append(_normalize_boxscore_pitching(root, d, force))
        elif entity == "player_bios":
            outputs.append(_normalize_player_bios(root, d, force))
        elif entity == "teams":
            outputs.append(_normalize_teams(root, d, force))
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
    return read_bronze_rows(path)


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
    # The roster API can list one player twice on a team (Soroka, Cubs,
    # 2026-06-19, byte-identical entries); one row per (team, player).
    seen: set = set()
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
                key = (team_id, _as_str(person.get("id")))
                if key in seen:
                    continue
                seen.add(key)
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
    # Savant's CSV can list the same pitch twice (seen Nov 2026: byte-identical
    # lines for balls in play). One row per (game_pk, at_bat_number,
    # pitch_number); exact repeats are dropped, conflicting ones are counted.
    seen: Dict[tuple, str] = {}
    exact_dupes = conflicting = 0
    for r in rows:
        # Bronze from before the type=details fix holds leaderboard rows, not
        # pitches. They have no game_pk and collapse onto a single pitch_id.
        if not r.get("game_pk"):
            continue
        key = (_as_str(r.get("game_pk")), _as_int(r.get("at_bat_number")), _as_int(r.get("pitch_number")))
        content = stable_json_dumps({k: v for k, v in r.items() if k != "ingested_at_utc"})
        if key in seen:
            if seen[key] == content:
                exact_dupes += 1
            else:
                conflicting += 1
            continue
        seen[key] = content
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
    if exact_dupes or conflicting:
        logger.warning(
            "silver mlb_statcast/pitches dt=%s: dropped %d repeated pitch rows (%d identical, %d conflicting; first kept)",
            dt.isoformat(), exact_dupes + conflicting, exact_dupes, conflicting,
        )
    out_dir = _silver_path(root, "mlb_statcast", "pitches", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _height_to_cm(value: Any) -> float | None:
    # The API reports height as text, e.g. 6' 3".
    match = re.match(r"\s*(\d+)'\s*(\d+)", str(value or ""))
    if not match:
        return None
    return round((int(match.group(1)) * 12 + int(match.group(2))) * 2.54, 1)


def _pounds_to_kg(value: Any) -> float | None:
    pounds = _as_float(value)
    return round(pounds * 0.45359237, 1) if pounds else None


def _normalize_player_bios(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("mlb_statsapi", "player_bios", dt, root)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for r in rows:
        people = r.get("people") or []
        if isinstance(people, str):
            people = _parse_json_maybe(people) or []
        for p in people:
            row = {
                    "player_id": _as_str(p.get("id")),
                    # The season list covers every player on a 40-man roster, not
                    # just today's 26 active, so identity and dim_player read
                    # players from here too.
                    "player_name": p.get("fullName"),
                    "position": (p.get("primaryPosition") or {}).get("abbreviation"),
                    "team_id": _as_str((p.get("currentTeam") or {}).get("id")),
                    "dob": p.get("birthDate"),
                    "height_cm": _height_to_cm(p.get("height")),
                    "weight_kg": _pounds_to_kg(p.get("weight")),
                    "bats": (p.get("batSide") or {}).get("code"),
                    "throws": (p.get("pitchHand") or {}).get("code"),
                    "birth_country": p.get("birthCountry"),
                }
            if include_partition_cols:
                row["dt"] = dt.isoformat()
                row["source"] = "mlb_statsapi"
            normalized.append(row)
    out_dir = _silver_path(root, "mlb_statsapi", "player_bios", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _normalize_teams(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("mlb_statsapi", "teams", dt, root)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    # The roster API can list one player twice on a team (Soroka, Cubs,
    # 2026-06-19, byte-identical entries); one row per (team, player).
    seen: set = set()
    for r in rows:
        teams = r.get("teams") or []
        if isinstance(teams, str):
            teams = _parse_json_maybe(teams) or []
        for t in teams:
            row = {
                "team_id": _as_str(t.get("id")),
                "team_name": t.get("name"),
                "team_abbrev": t.get("abbreviation"),
                "club_name": t.get("clubName"),
                "home_city": t.get("locationName"),
                "league_name": (t.get("league") or {}).get("name"),
                "division_name": (t.get("division") or {}).get("name"),
                "venue_name": (t.get("venue") or {}).get("name"),
                "first_year": _as_int(t.get("firstYearOfPlay")),
                "active": t.get("active"),
            }
            if include_partition_cols:
                row["dt"] = dt.isoformat()
                row["source"] = "mlb_statsapi"
            normalized.append(row)
    out_dir = _silver_path(root, "mlb_statsapi", "teams", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _iter_boxscore_player_stats(root: Path, dt: date, stat_key: str):
    """Yield (game_pk, team_id, player_id, player_name, stats_dict) for every
    player in every game's boxscore who has a non-empty `stat_key` stat block
    (i.e. actually batted or actually pitched in the game)."""
    rows = _read_bronze("mlb_statsapi", "boxscore", dt, root)
    for r in rows:
        games = r.get("games")
        if isinstance(games, str):
            games = _parse_json_maybe(games)
        if isinstance(games, dict):
            games = games.get("games", [])
        for game_entry in games or []:
            game_pk = game_entry.get("game_pk")
            box = _parse_json_maybe(game_entry.get("body_text")) or {}
            teams = box.get("teams", {}) if isinstance(box, dict) else {}
            for side in ("home", "away"):
                team = teams.get(side) or {}
                team_id = _as_str((team.get("team") or {}).get("id"))
                for p in (team.get("players") or {}).values():
                    stats = ((p.get("stats") or {}).get(stat_key)) or {}
                    if not stats:
                        continue
                    person = p.get("person") or {}
                    yield game_pk, team_id, _as_str(person.get("id")), stats, p


def _normalize_boxscore_batting(root: Path, dt: date, force: bool) -> Path:
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for game_pk, team_id, player_id, batting, p in _iter_boxscore_player_stats(root, dt, "batting"):
        if not batting.get("atBats") and not batting.get("plateAppearances"):
            continue  # entered the game but never had a plate appearance
        row = {
            "game_id": _as_str(game_pk),
            "team_id": team_id,
            "player_id": player_id,
            "ab": _as_int(batting.get("atBats")),
            "h": _as_int(batting.get("hits")),
            "2b": _as_int(batting.get("doubles")),
            "3b": _as_int(batting.get("triples")),
            "hr": _as_int(batting.get("homeRuns")),
            "bb": _as_int(batting.get("baseOnBalls")),
            "so": _as_int(batting.get("strikeOuts")),
            "rbi": _as_int(batting.get("rbi")),
            "r": _as_int(batting.get("runs")),
            "raw_json": stable_json_dumps(p),
        }
        if include_partition_cols:
            row["dt"] = dt.isoformat()
            row["source"] = "mlb_statsapi"
        normalized.append(row)
    out_dir = _silver_path(root, "mlb_statsapi", "boxscore_batting", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _normalize_boxscore_pitching(root: Path, dt: date, force: bool) -> Path:
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for game_pk, team_id, player_id, pitching, p in _iter_boxscore_player_stats(root, dt, "pitching"):
        if not pitching.get("inningsPitched"):
            continue  # in the pitching stats block but never actually appeared
        row = {
            "game_id": _as_str(game_pk),
            "team_id": team_id,
            "player_id": player_id,
            "ip": pitching.get("inningsPitched"),
            "h": _as_int(pitching.get("hits")),
            "r": _as_int(pitching.get("runs")),
            "er": _as_int(pitching.get("earnedRuns")),
            "bb": _as_int(pitching.get("baseOnBalls")),
            "so": _as_int(pitching.get("strikeOuts")),
            "hr": _as_int(pitching.get("homeRuns")),
            "raw_json": stable_json_dumps(p),
        }
        if include_partition_cols:
            row["dt"] = dt.isoformat()
            row["source"] = "mlb_statsapi"
        normalized.append(row)
    out_dir = _silver_path(root, "mlb_statsapi", "boxscore_pitching", dt)
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
