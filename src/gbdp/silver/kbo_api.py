from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from gbdp.silver.writer import write_parquet
from gbdp.utils.io import (
    bronze_root,
    ensure_dir,
    include_partition_cols_silver,
    path_exists,
    read_parquet_rows,
    silver_root,
    stable_json_dumps,
)
from gbdp.utils.time import daterange, parse_date


def normalize_kbo_api(entity: str, start: str, end: str, root: Path | None = None, force: bool = False) -> List[Path]:
    root = root or silver_root()
    outputs: List[Path] = []
    for d in daterange(parse_date(start), parse_date(end)):
        if entity == "games":
            outputs.append(_normalize_games(root, d, force))
        elif entity == "boxscore_batting":
            outputs.append(_normalize_boxscore_batting(root, d, force))
        elif entity == "boxscore_pitching":
            outputs.append(_normalize_boxscore_pitching(root, d, force))
        else:
            raise ValueError(f"Unsupported kbo_api silver entity: {entity}")
    return outputs


def _bronze_path(entity: str, dt: date) -> Path:
    return bronze_root() / "parsed" / "kbo_api" / entity / f"dt={dt.isoformat()}"


def _silver_path(root: Path, entity: str, dt: date) -> Path:
    return root / "kbo_api" / entity / f"dt={dt.isoformat()}"


def _read_bronze(entity: str, dt: date) -> List[Dict[str, Any]]:
    path = _bronze_path(entity, dt)
    if not path_exists(path):
        return []
    return read_parquet_rows(path)


def _as_str(v: Any) -> Any:
    return str(v) if v is not None else None


def _as_int(v: Any) -> Any:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _as_iso_date(yyyymmdd: Any) -> Any:
    s = _as_str(yyyymmdd)
    if not s or len(s) != 8:
        return s
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


_STATUS_BY_STATE = {"1": "Scheduled", "2": "In Progress", "3": "Final"}


def _normalize_games(root: Path, dt: date, force: bool) -> Path:
    rows = _read_bronze("games", dt)
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for r in rows:
        state = _as_str(r.get("GAME_STATE_SC"))
        status = _STATUS_BY_STATE.get(state, state)
        if _as_str(r.get("CANCEL_SC_ID")) not in (None, "0"):
            status = r.get("CANCEL_SC_NM") or "Cancelled"
        row = {
            "game_id": _as_str(r.get("G_ID")),
            "game_date": _as_iso_date(r.get("G_DT")),
            "home_team_id": _as_str(r.get("HOME_ID")),
            "home_team_name": r.get("HOME_NM"),
            "away_team_id": _as_str(r.get("AWAY_ID")),
            "away_team_name": r.get("AWAY_NM"),
            # T_SCORE_CN/B_SCORE_CN are named for top/bottom of the inning, i.e.
            # away/home batting -- NOT team names starting with T/B.
            "away_score": _as_int(r.get("T_SCORE_CN")),
            "home_score": _as_int(r.get("B_SCORE_CN")),
            "venue": r.get("S_NM"),
            "status": status,
            "sr_id": _as_str(r.get("SR_ID")),
            "raw_json": stable_json_dumps(r),
        }
        if include_partition_cols:
            row["dt"] = dt.isoformat()
            row["source"] = "kbo_api"
        normalized.append(row)
    out_dir = _silver_path(root, "games", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


def _iter_boxscore_games(dt: date):
    """Yield (game_id, away_team_id, home_team_id, boxscore_dict) for every
    game fetched on this date."""
    rows = _read_bronze("boxscore", dt)
    for r in rows:
        games = r.get("games")
        if isinstance(games, str):
            games = _parse_json_maybe(games)
        for g in games or []:
            box = _parse_json_maybe(g.get("body_text"))
            if isinstance(box, dict) and box.get("code") == "100":
                yield g.get("game_id"), g.get("away_team_id"), g.get("home_team_id"), box


def _parse_json_maybe(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None


def _grid_rows(table_json: str | None) -> List[List[str]]:
    """Flatten the site's generic {"rows": [{"row": [{"Text": ...}, ...]}]}
    grid-table JSON into a plain list of cell-text lists."""
    table = _parse_json_maybe(table_json) or {}
    out = []
    for r in table.get("rows") or []:
        out.append([c.get("Text") for c in (r.get("row") or [])])
    return out


def _normalize_boxscore_batting(root: Path, dt: date, force: bool) -> Path:
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for game_id, away_team_id, home_team_id, box in _iter_boxscore_games(dt):
        # arrHitter/arrPitcher carry no team identifier at all; confirmed
        # empirically (win/loss pitcher cross-referenced against the game
        # list) that index 0 is always the away team, index 1 the home team.
        team_ids = [away_team_id, home_team_id]
        for block_idx, team_block in enumerate(box.get("arrHitter") or []):
            team_id = team_ids[block_idx] if block_idx < len(team_ids) else None
            names = [row[-1] for row in _grid_rows(team_block.get("table1")) if row]
            # table3 has NO header labels in the API response; column order
            # confirmed against real game data (batting order/name from
            # table1 cross-referenced with a known home-run play) to be the
            # standard AB-R-H-RBI-AVG box score convention. 2B/3B/BB/SO/HR
            # are not broken out anywhere in this response -- left as None.
            stat_rows = _grid_rows(team_block.get("table3"))
            for i, stats in enumerate(stat_rows):
                if i >= len(names) or not names[i]:
                    continue
                if len(stats) < 4:
                    continue
                ab = _as_int(stats[0])
                if not ab:
                    continue  # did not appear at the plate
                row = {
                    "game_id": game_id,
                    "team_id": team_id,
                    # No numeric player ID is exposed anywhere in this response
                    # (checked the row's Id/Value metadata directly -- always
                    # null), so player_name is the only available source_id
                    # for identity resolution. Documented limitation, not an
                    # oversight: less robust than a real ID, but functional.
                    "player_name": names[i],
                    "ab": ab,
                    "r": _as_int(stats[1]),
                    "h": _as_int(stats[2]),
                    "rbi": _as_int(stats[3]),
                }
                if include_partition_cols:
                    row["dt"] = dt.isoformat()
                    row["source"] = "kbo_api"
                normalized.append(row)
    out_dir = _silver_path(root, "boxscore_batting", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)


# Pitcher table headers ARE labeled in the API response, so map by header
# text instead of hardcoding positions -- robust if KBO reorders columns.
_PITCHER_HEADER_MAP = {
    "선수명": "player_name",
    "이닝": "ip",
    "타수": "ab_against",
    "피안타": "h",
    "홈런": "hr",
    "4사구": "bb",
    "삼진": "so",
    "실점": "r",
    "자책": "er",
}


def _normalize_boxscore_pitching(root: Path, dt: date, force: bool) -> Path:
    normalized: List[Dict[str, Any]] = []
    include_partition_cols = include_partition_cols_silver()
    for game_id, away_team_id, home_team_id, box in _iter_boxscore_games(dt):
        team_ids = [away_team_id, home_team_id]
        for block_idx, team_block in enumerate(box.get("arrPitcher") or []):
            team_id = team_ids[block_idx] if block_idx < len(team_ids) else None
            table = _parse_json_maybe(team_block.get("table")) or {}
            header_rows = table.get("headers") or []
            if not header_rows:
                continue
            header_texts = [c.get("Text") for c in (header_rows[0].get("row") or [])]
            col_index = {
                _PITCHER_HEADER_MAP[h]: i
                for i, h in enumerate(header_texts)
                if h in _PITCHER_HEADER_MAP
            }
            if "player_name" not in col_index:
                continue
            for r in table.get("rows") or []:
                cells = [c.get("Text") for c in (r.get("row") or [])]
                name = cells[col_index["player_name"]] if col_index["player_name"] < len(cells) else None
                if not name:
                    continue
                row = {"game_id": game_id, "team_id": team_id, "player_name": name}
                for field, idx in col_index.items():
                    if field == "player_name" or idx >= len(cells):
                        continue
                    if field == "ip":
                        row[field] = cells[idx]
                    else:
                        row[field] = _as_int(cells[idx])
                if include_partition_cols:
                    row["dt"] = dt.isoformat()
                    row["source"] = "kbo_api"
                normalized.append(row)
    out_dir = _silver_path(root, "boxscore_pitching", dt)
    ensure_dir(out_dir)
    return write_parquet(normalized, out_dir / "part-00001.parquet", force=force)
