from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pyarrow.dataset as ds

from gbdp.silver.writer import write_parquet
from gbdp.utils.io import data_root, ensure_dir, include_partition_cols_silver
from gbdp.utils.time import daterange, parse_date


def normalize_retrosheet(entity: str, start: str, end: str, root: Path | None = None) -> List[Path]:
    root = root or data_root()
    outputs: List[Path] = []
    for d in daterange(parse_date(start), parse_date(end)):
        outputs.append(_normalize_entity(root, entity, d))
    return outputs


def _normalize_entity(root: Path, entity: str, dt: date) -> Path:
    path = root / "bronze" / "parsed" / "retrosheet_local" / entity / f"dt={dt.isoformat()}"
    rows: List[Dict[str, Any]] = []
    if path.exists():
        files = list(path.glob("*.parquet"))
        if files:
            rows = ds.dataset(files, format="parquet").to_table().to_pylist()

    if entity == "gameinfo":
        include_partition_cols = include_partition_cols_silver()
        out_rows = []
        for r in rows:
            row = {
                "game_id": r.get("gid"),
                "game_date": _to_date(r.get("date")),
                "home_team_id": r.get("hometeam"),
                "away_team_id": r.get("visteam"),
                "home_score": _as_int(r.get("hruns")),
                "away_score": _as_int(r.get("vruns")),
                "venue": r.get("site"),
                "status": "final" if r.get("hruns") and r.get("vruns") else "unknown",
            }
            if include_partition_cols:
                row["dt"] = dt.isoformat()
                row["source"] = "retrosheet_local"
            out_rows.append(row)
        return _write(root, "games", dt, out_rows)

    if entity == "allplayers":
        include_partition_cols = include_partition_cols_silver()
        out_rows = []
        for r in rows:
            row = {
                "player_id": r.get("id"),
                "player_name": f"{(r.get('first') or '').strip()} {(r.get('last') or '').strip()}".strip(),
                "bats": r.get("bat"),
                "throws": r.get("throw"),
                "team_id": r.get("team"),
                "season": r.get("season"),
            }
            if include_partition_cols:
                row["dt"] = dt.isoformat()
                row["source"] = "retrosheet_local"
            out_rows.append(row)
        return _write(root, "rosters", dt, out_rows)

    if entity == "batting":
        include_partition_cols = include_partition_cols_silver()
        out_rows = []
        for r in rows:
            row = {
                "game_id": r.get("gid"),
                "player_id": r.get("id"),
                "team_id": r.get("team"),
                "ab": _as_int(r.get("b_ab")),
                "h": _as_int(r.get("b_h")),
                "2b": _as_int(r.get("b_d")),
                "3b": _as_int(r.get("b_t")),
                "hr": _as_int(r.get("b_hr")),
                "bb": _as_int(r.get("b_w")),
                "so": _as_int(r.get("b_k")),
                "rbi": _as_int(r.get("b_rbi")),
                "r": _as_int(r.get("b_r")),
            }
            if include_partition_cols:
                row["dt"] = dt.isoformat()
                row["source"] = "retrosheet_local"
            out_rows.append(row)
        return _write(root, "boxscore_batting", dt, out_rows)

    if entity == "pitching":
        include_partition_cols = include_partition_cols_silver()
        out_rows = []
        for r in rows:
            row = {
                "game_id": r.get("gid"),
                "player_id": r.get("id"),
                "team_id": r.get("team"),
                "ip": _as_int(r.get("p_ipouts")),
                "h": _as_int(r.get("p_h")),
                "r": _as_int(r.get("p_r")),
                "er": _as_int(r.get("p_er")),
                "bb": _as_int(r.get("p_w")),
                "so": _as_int(r.get("p_k")),
                "hr": _as_int(r.get("p_hr")),
            }
            if include_partition_cols:
                row["dt"] = dt.isoformat()
                row["source"] = "retrosheet_local"
            out_rows.append(row)
        return _write(root, "boxscore_pitching", dt, out_rows)

    if entity == "plays":
        include_partition_cols = include_partition_cols_silver()
        out_rows = []
        for r in rows:
            row = {
                "game_id": r.get("gid"),
                "event": r.get("event"),
                "inning": _as_int(r.get("inning")),
                "top_bottom": r.get("top_bot"),
                "batting_team_id": r.get("batteam"),
                "pitching_team_id": r.get("pitteam"),
                "batter_id": r.get("batter"),
                "pitcher_id": r.get("pitcher"),
                "outs_pre": _as_int(r.get("outs_pre")),
                "outs_post": _as_int(r.get("outs_post")),
                "br1_pre": r.get("br1_pre"),
                "br2_pre": r.get("br2_pre"),
                "br3_pre": r.get("br3_pre"),
                "br1_post": r.get("br1_post"),
                "br2_post": r.get("br2_post"),
                "br3_post": r.get("br3_post"),
                "runs": _as_int(r.get("runs")),
                "rbi": _as_int(r.get("rbi")),
            }
            if include_partition_cols:
                row["dt"] = dt.isoformat()
                row["source"] = "retrosheet_local"
            out_rows.append(row)
        return _write(root, "game_pbp", dt, out_rows)

    if entity == "teamstats":
        return _write(root, "teamstats", dt, rows)

    if entity == "fielding":
        return _write(root, "fielding", dt, rows)

    raise ValueError(f"Unsupported retrosheet entity: {entity}")


def _write(root: Path, entity: str, dt: date, rows: List[Dict[str, Any]]) -> Path:
    out_dir = root / "silver" / "retrosheet_local" / entity / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    return write_parquet(rows, out_dir / "part-00001.parquet")


def _to_date(value: Any) -> str | None:
    if not value:
        return None
    s = str(value)
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return value


def _as_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
