from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pyarrow.dataset as ds

from gbdp.identity.manual_overrides import load_manual_overrides
from gbdp.identity.rules import player_match_key, team_match_key
from gbdp.utils.ids import ulid_from_key
from gbdp.utils.io import data_root, ensure_dir
from gbdp.utils.time import daterange, parse_date


def resolve_identity(start: str, end: str, root: Path | None = None, force: bool = False) -> List[Path]:
    root = root or data_root()
    outputs: List[Path] = []
    overrides = load_manual_overrides(Path("manual_entity_links.csv"))
    override_map = {
        (o["entity_type"], o["source"], o["source_id"]): o["canonical_id"] for o in overrides
    }
    for d in daterange(parse_date(start), parse_date(end)):
        outputs.append(_resolve_for_date(root, d, override_map, force))
        _write_merge_events(root, d, overrides, force)
    return outputs


def _resolve_for_date(root: Path, dt: date, overrides: Dict[tuple, str], force: bool) -> Path:
    records: List[Dict[str, Any]] = []
    records.extend(_collect_player_sources(root, dt))
    records.extend(_collect_team_sources(root, dt))

    bridge_rows: List[Dict[str, Any]] = []
    for r in records:
        entity_type = r["entity_type"]
        source = r["source"]
        source_id = r["source_id"]
        override = overrides.get((entity_type, source, source_id))
        if entity_type == "player":
            match = player_match_key(
                {
                    "source": source,
                    "source_player_id": source_id,
                    "name": r.get("name"),
                    "dob": r.get("dob"),
                    "team_id": r.get("team_id"),
                }
            )
        else:
            match = team_match_key(
                {
                    "source": source,
                    "source_team_id": source_id,
                    "name": r.get("name"),
                    "league_code": r.get("league_code"),
                }
            )
        canonical_key = match.canonical_key if match else f"{entity_type}:{source}:{source_id}"
        canonical_id = override or ulid_from_key(canonical_key, dt.isoformat())
        bridge_rows.append(
            {
                "entity_type": entity_type,
                "source": source,
                "source_id": source_id,
                "canonical_id": canonical_id,
                "first_seen_dt": dt.isoformat(),
                "last_seen_dt": dt.isoformat(),
                "confidence": match.confidence if match else 0.5,
                "match_method": match.method if match else "unknown",
                "manual_override": bool(override),
                "notes": None,
                "dt": dt.isoformat(),
            }
        )

    out_dir = root / "gold" / "bridge_source_ids" / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if out_path.exists() and not force:
        return out_path
    _write_parquet(bridge_rows, out_path)
    return out_path


def _collect_player_sources(root: Path, dt: date) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    # NPB rosters silver
    npb_rosters = _read_silver(root, "npb_spaia", "rosters", dt)
    for r in npb_rosters:
        source_id = r.get("player_id")
        if not source_id:
            continue
        rows.append(
            {
                "entity_type": "player",
                "source": "npb_spaia",
                "source_id": str(source_id),
                "name": r.get("player_name"),
                "dob": r.get("dob"),
                "team_id": r.get("team_id"),
                "league_code": "NPB",
            }
        )
    # MLB rosters silver
    mlb_rosters = _read_silver(root, "mlb_statsapi", "rosters", dt)
    for r in mlb_rosters:
        source_id = r.get("player_id")
        if not source_id:
            continue
        rows.append(
            {
                "entity_type": "player",
                "source": "mlb_statsapi",
                "source_id": str(source_id),
                "name": r.get("player_name"),
                "dob": r.get("dob"),
                "team_id": r.get("team_id"),
                "league_code": "MLB",
            }
        )
    # Indy rosters silver
    indy_rosters = _read_silver(root, "indy_local", "rosters", dt)
    for r in indy_rosters:
        source_id = r.get("player_id")
        if not source_id:
            continue
        rows.append(
            {
                "entity_type": "player",
                "source": "indy_local",
                "source_id": str(source_id),
                "name": r.get("player_name") or r.get("name"),
                "dob": r.get("dob"),
                "team_id": r.get("team_id"),
                "league_code": "INDY",
            }
        )
    # Retrosheet rosters silver
    retro_rosters = _read_silver(root, "retrosheet_local", "rosters", dt)
    for r in retro_rosters:
        source_id = r.get("player_id")
        if not source_id:
            continue
        rows.append(
            {
                "entity_type": "player",
                "source": "retrosheet_local",
                "source_id": str(source_id),
                "name": r.get("player_name"),
                "dob": None,
                "team_id": r.get("team_id"),
                "league_code": "MLB",
            }
        )
    return rows


def _collect_team_sources(root: Path, dt: date) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    npb_games = _read_silver(root, "npb_spaia", "games", dt)
    for r in npb_games:
        for team_id, team_name in [
            (r.get("home_team_id"), r.get("home_team_name")),
            (r.get("away_team_id"), r.get("away_team_name")),
        ]:
            if team_id:
                rows.append(
                    {
                        "entity_type": "team",
                        "source": "npb_spaia",
                        "source_id": str(team_id),
                        "name": team_name,
                        "league_code": "NPB",
                    }
                )
    mlb_games = _read_silver(root, "mlb_statsapi", "games", dt)
    for r in mlb_games:
        for team_id, team_name in [
            (r.get("home_team_id"), r.get("home_team_name")),
            (r.get("away_team_id"), r.get("away_team_name")),
        ]:
            if team_id:
                rows.append(
                    {
                        "entity_type": "team",
                        "source": "mlb_statsapi",
                        "source_id": str(team_id),
                        "name": team_name,
                        "league_code": "MLB",
                    }
                )
    indy_games = _read_silver(root, "indy_local", "games", dt)
    for r in indy_games:
        for team_id, team_name in [
            (r.get("home_team_id"), r.get("home_team_name")),
            (r.get("away_team_id"), r.get("away_team_name")),
        ]:
            if team_id:
                rows.append(
                    {
                        "entity_type": "team",
                        "source": "indy_local",
                        "source_id": str(team_id),
                        "name": team_name,
                        "league_code": "INDY",
                    }
                )
    retro_games = _read_silver(root, "retrosheet_local", "games", dt)
    for r in retro_games:
        for team_id, team_name in [
            (r.get("home_team_id"), r.get("home_team_id")),
            (r.get("away_team_id"), r.get("away_team_id")),
        ]:
            if team_id:
                rows.append(
                    {
                        "entity_type": "team",
                        "source": "retrosheet_local",
                        "source_id": str(team_id),
                        "name": team_name,
                        "league_code": "MLB",
                    }
                )
    return rows


def _read_silver(root: Path, source: str, entity: str, dt: date) -> List[Dict[str, Any]]:
    path = root / "silver" / source / entity / f"dt={dt.isoformat()}"
    if not path.exists():
        return []
    dataset = ds.dataset(path, format="parquet")
    return dataset.to_table().to_pylist()


def _write_parquet(rows: List[Dict[str, Any]], path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not rows:
        rows = [{"empty": True}]
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, use_dictionary=False)


def _write_merge_events(root: Path, dt: date, overrides: List[Dict[str, str]], force: bool) -> None:
    out_dir = root / "gold" / "merge_events" / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if out_path.exists() and not force:
        return
    rows: List[Dict[str, Any]] = []
    for o in overrides:
        rows.append(
            {
                "entity_type": o.get("entity_type"),
                "source": o.get("source"),
                "source_id": o.get("source_id"),
                "canonical_id": o.get("canonical_id"),
                "match_method": "manual_override",
                "notes": o.get("notes"),
                "dt": dt.isoformat(),
            }
        )
    _write_parquet(rows, out_path)
