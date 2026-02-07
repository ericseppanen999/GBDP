from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pyarrow.dataset as ds

from gbdp.utils.ids import ulid_from_key
from gbdp.utils.io import data_root, ensure_dir, stable_json_dumps, storage_format
from gbdp.utils.time import daterange, parse_date


def publish_gold(start: str, end: str, root: Path | None = None, force: bool = False) -> List[Path]:
    root = root or data_root()
    outputs: List[Path] = []
    for d in daterange(parse_date(start), parse_date(end)):
        outputs.extend(_publish_for_date(root, d, force))
    return outputs


def _publish_for_date(root: Path, dt: date, force: bool) -> List[Path]:
    outputs: List[Path] = []
    bridge = _read_gold_bridge(root, dt)

    outputs.append(_write_dim_league(root, dt, force))
    outputs.append(_write_dim_team(root, dt, bridge, force))
    outputs.append(_write_dim_player(root, dt, bridge, force))
    outputs.append(_write_dim_season(root, dt, force))

    outputs.append(_write_fact_game(root, dt, bridge, force))
    outputs.append(_write_fact_roster(root, dt, bridge, force))
    outputs.append(_write_fact_transaction(root, dt, bridge, force))
    outputs.append(_write_fact_pitch(root, dt, bridge, force))
    outputs.append(_write_fact_plate_appearance(root, dt, bridge, force))
    outputs.append(_write_fact_standings(root, dt, bridge, force))
    outputs.append(_write_fact_boxscore_batting(root, dt, bridge, force))
    outputs.append(_write_fact_boxscore_pitching(root, dt, bridge, force))
    outputs.append(_write_run_expectancy(root, dt, force))
    outputs.append(_write_breakout_candidates(root, dt, force))
    return outputs


def _read_gold_bridge(root: Path, dt: date) -> Dict[tuple, str]:
    path = root / "gold" / "bridge_source_ids" / f"dt={dt.isoformat()}"
    if not path.exists():
        return {}
    data = ds.dataset(path, format="parquet").to_table().to_pylist()
    return {(r["entity_type"], r["source"], r["source_id"]): r["canonical_id"] for r in data}


def _write_dim_league(root: Path, dt: date, force: bool) -> Path:
    leagues_path = Path("configs/leagues.yaml")
    if leagues_path.exists():
        import yaml

        with leagues_path.open("r", encoding="utf-8") as f:
            leagues = yaml.safe_load(f)["leagues"]
    else:
        leagues = {}
    rows = []
    for code, meta in leagues.items():
        rows.append(
            {
                "league_id": meta.get("league_id"),
                "league_code": code,
                "country": meta.get("country"),
                "level": meta.get("level"),
                "season_start_month": meta.get("season_start_month"),
                "season_end_month": meta.get("season_end_month"),
                "dt": dt.isoformat(),
            }
        )
    return _write_gold_table(root, "dim_league", dt, rows, force)


def _write_dim_team(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statsapi", "games", dt):
        rows.extend(_team_rows_from_game(r, "MLB", bridge, "mlb_statsapi"))
    for r in _read_silver(root, "npb_spaia", "games", dt):
        rows.extend(_team_rows_from_game(r, "NPB", bridge, "npb_spaia"))
    for r in _read_silver(root, "indy_local", "games", dt):
        rows.extend(_team_rows_from_game(r, "INDY", bridge, "indy_local"))
    for r in _read_silver(root, "retrosheet_local", "games", dt):
        rows.extend(_team_rows_from_game(r, "MLB", bridge, "retrosheet_local"))
    dedup = {(r["team_id"], r["team_name"]): r for r in rows}
    return _write_gold_table(root, "dim_team", dt, list(dedup.values()), force)


def _team_rows_from_game(row: Dict[str, Any], league_code: str, bridge: Dict[tuple, str], source: str):
    out = []
    for team_id, team_name, abbrev in [
        (row.get("home_team_id"), row.get("home_team_name"), None),
        (row.get("away_team_id"), row.get("away_team_name"), None),
    ]:
        if not team_id:
            continue
        canonical = bridge.get(("team", source, str(team_id))) or ulid_from_key(
            f"team:{source}:{team_id}", row.get("dt") or row.get("game_date") or ""
        )
        out.append(
            {
                "team_id": canonical,
                "league_id": _league_id_for_code(league_code),
                "team_name": team_name,
                "team_abbrev": abbrev,
                "home_city": None,
                "valid_from_dt": row.get("dt") or row.get("game_date") or "",
                "valid_to_dt": None,
                "is_current": True,
                "dt": row.get("dt") or row.get("game_date") or "",
                "source": source,
                "source_team_id": str(team_id),
            }
        )
    return out


def _write_dim_player(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statsapi", "rosters", dt):
        rows.append(_player_row(r, "MLB", bridge, "mlb_statsapi"))
    for r in _read_silver(root, "npb_spaia", "rosters", dt):
        rows.append(_player_row(r, "NPB", bridge, "npb_spaia"))
    for r in _read_silver(root, "indy_local", "rosters", dt):
        rows.append(_player_row(r, "INDY", bridge, "indy_local"))
    for r in _read_silver(root, "retrosheet_local", "rosters", dt):
        rows.append(_player_row(r, "MLB", bridge, "retrosheet_local"))
    dedup = {r["player_id"]: r for r in rows if r.get("player_id")}
    return _write_gold_table(root, "dim_player", dt, list(dedup.values()), force)


def _player_row(r: Dict[str, Any], league_code: str, bridge: Dict[tuple, str], source: str) -> Dict[str, Any]:
    source_id = r.get("player_id")
    canonical = bridge.get(("player", source, str(source_id))) or ulid_from_key(
        f"player:{source}:{source_id}", r.get("dt") or ""
    )
    return {
        "player_id": canonical,
        "primary_name": r.get("player_name"),
        "alternate_names": None,
        "dob": r.get("dob"),
        "bats": r.get("bats") or "UNK",
        "throws": r.get("throws") or "UNK",
        "height_cm": None,
        "weight_kg": None,
        "nationality": None,
        "primary_position": r.get("position"),
        "valid_from_dt": r.get("dt"),
        "valid_to_dt": None,
        "is_current": True,
        "dt": r.get("dt"),
        "source": source,
        "source_player_id": str(source_id) if source_id is not None else None,
        "league_id": _league_id_for_code(league_code),
    }


def _write_dim_season(root: Path, dt: date, force: bool) -> Path:
    year = dt.year
    rows = [
        {
            "season_id": ulid_from_key(f"season:MLB:{year}", dt.isoformat()),
            "league_id": _league_id_for_code("MLB"),
            "season_year": year,
            "season_type": "regular",
            "season_start_dt": f"{year}-03-01",
            "season_end_dt": f"{year}-11-30",
            "dt": dt.isoformat(),
        },
        {
            "season_id": ulid_from_key(f"season:NPB:{year}", dt.isoformat()),
            "league_id": _league_id_for_code("NPB"),
            "season_year": year,
            "season_type": "regular",
            "season_start_dt": f"{year}-03-01",
            "season_end_dt": f"{year}-11-30",
            "dt": dt.isoformat(),
        },
        {
            "season_id": ulid_from_key(f"season:INDY:{year}", dt.isoformat()),
            "league_id": _league_id_for_code("INDY"),
            "season_year": year,
            "season_type": "regular",
            "season_start_dt": f"{year}-04-01",
            "season_end_dt": f"{year}-10-31",
            "dt": dt.isoformat(),
        },
    ]
    return _write_gold_table(root, "dim_season", dt, rows, force)


def _write_fact_game(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    rows.extend(_fact_game_from_silver(root, dt, "mlb_statsapi", "MLB", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "npb_spaia", "NPB", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "indy_local", "INDY", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "retrosheet_local", "MLB", bridge))
    return _write_gold_table(root, "fact_game", dt, rows, force)


def _fact_game_from_silver(root: Path, dt: date, source: str, league_code: str, bridge: Dict[tuple, str]):
    rows = []
    for r in _read_silver(root, source, "games", dt):
        game_id = ulid_from_key(f"game:{source}:{r.get('game_id')}", dt.isoformat())
        home_team = bridge.get(("team", source, str(r.get("home_team_id"))))
        away_team = bridge.get(("team", source, str(r.get("away_team_id"))))
        rows.append(
            {
                "game_id": game_id,
                "league_id": _league_id_for_code(league_code),
                "season_id": ulid_from_key(f"season:{league_code}:{dt.year}", dt.isoformat()),
                "game_date": r.get("game_date") or dt.isoformat(),
                "game_start_ts_utc": r.get("game_date"),
                "home_team_id": home_team,
                "away_team_id": away_team,
                "venue": r.get("venue"),
                "status": r.get("status") or r.get("game_state_name") or "unknown",
                "home_score": r.get("home_score"),
                "away_score": r.get("away_score"),
                "dt": dt.isoformat(),
                "source": source,
                "source_game_id": r.get("game_id"),
            }
        )
    return rows


def _write_fact_roster(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statsapi", "rosters", dt):
        rows.append(_fact_roster_row(r, "mlb_statsapi", "MLB", bridge, dt))
    for r in _read_silver(root, "npb_spaia", "rosters", dt):
        rows.append(_fact_roster_row(r, "npb_spaia", "NPB", bridge, dt))
    for r in _read_silver(root, "indy_local", "rosters", dt):
        rows.append(_fact_roster_row(r, "indy_local", "INDY", bridge, dt))
    for r in _read_silver(root, "retrosheet_local", "rosters", dt):
        rows.append(_fact_roster_row(r, "retrosheet_local", "MLB", bridge, dt))
    return _write_gold_table(root, "fact_roster", dt, rows, force)


def _fact_roster_row(r: Dict[str, Any], source: str, league_code: str, bridge: Dict[tuple, str], dt: date):
    team = bridge.get(("team", source, str(r.get("team_id"))))
    player = bridge.get(("player", source, str(r.get("player_id"))))
    return {
        "team_id": team,
        "player_id": player,
        "season_id": ulid_from_key(f"season:{league_code}:{dt.year}", dt.isoformat()),
        "role": "player",
        "start_dt": dt.isoformat(),
        "end_dt": None,
        "dt": dt.isoformat(),
        "source": source,
    }


def _write_fact_transaction(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statsapi", "transactions", dt):
        rows.append(
            {
                "transaction_id": ulid_from_key(f"txn:{r.get('transaction_id')}", dt.isoformat()),
                "league_id": _league_id_for_code("MLB"),
                "team_id": bridge.get(("team", "mlb_statsapi", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "mlb_statsapi", str(r.get("player_id")))),
                "transaction_type": r.get("type_desc"),
                "effective_dt": r.get("date"),
                "details": r.get("description"),
                "dt": dt.isoformat(),
                "source": "mlb_statsapi",
            }
        )
    return _write_gold_table(root, "fact_transaction", dt, rows, force)


def _write_fact_pitch(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "mlb_statcast", "pitches", dt):
        game_pk = r.get("game_pk")
        pa_key = f"pa:mlb_statcast:{game_pk}:{r.get('at_bat_number') or ''}:{r.get('batter_id')}"
        pa_id = ulid_from_key(pa_key, dt.isoformat())
        rows.append(
            {
                "pitch_id": ulid_from_key(
                    f"pitch:{game_pk}:{r.get('inning')}:{r.get('outs_when_up')}:{r.get('pitcher_id')}:{r.get('batter_id')}:{r.get('description')}",
                    dt.isoformat(),
                ),
                "game_id": ulid_from_key(f"game:mlb_statsapi:{game_pk}", dt.isoformat()),
                "pa_id": pa_id,
                "pitch_number_in_pa": r.get("pitch_number"),
                "pitcher_id": bridge.get(("player", "mlb_statsapi", str(r.get("pitcher_id")))),
                "batter_id": bridge.get(("player", "mlb_statsapi", str(r.get("batter_id")))),
                "balls_before": r.get("balls"),
                "strikes_before": r.get("strikes"),
                "pitch_type": r.get("pitch_type"),
                "release_speed": r.get("release_speed"),
                "plate_x": r.get("plate_x"),
                "plate_z": r.get("plate_z"),
                "result": r.get("description"),
                "dt": dt.isoformat(),
            }
        )
    return _write_gold_table(root, "fact_pitch", dt, rows, force)


def _write_fact_plate_appearance(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    # MLB from statcast (dedup by pa_id)
    pa_map: Dict[str, Dict[str, Any]] = {}
    for r in _read_silver(root, "mlb_statcast", "pitches", dt):
        game_pk = r.get("game_pk")
        pa_key = f"pa:mlb_statcast:{game_pk}:{r.get('at_bat_number') or ''}:{r.get('batter_id')}"
        pa_id = ulid_from_key(pa_key, dt.isoformat())
        event_type = _map_event_type(r.get("events") or r.get("description"))
        if pa_id not in pa_map or event_type != "UNKNOWN":
            pa_map[pa_id] = {
                "pa_id": pa_id,
                "game_id": ulid_from_key(f"game:mlb_statsapi:{game_pk}", dt.isoformat()),
                "inning": r.get("inning"),
                "is_top_inning": r.get("inning_topbot") == "Top",
                "batting_team_id": None,
                "fielding_team_id": None,
                "batter_id": bridge.get(("player", "mlb_statsapi", str(r.get("batter_id")))),
                "pitcher_id": bridge.get(("player", "mlb_statsapi", str(r.get("pitcher_id")))),
                "event_type": event_type,
                "rbi": None,
                "runs_scored_on_play": None,
                "outs_on_play": r.get("outs_when_up"),
                "base_state_before": _base_state_from_statcast(r),
                "outs_before": r.get("outs_when_up"),
                "base_state_after": None,
                "outs_after": r.get("outs_when_up"),
                "dt": dt.isoformat(),
            }
    rows.extend(pa_map.values())
    # NPB from PBP (minimal)
    for r in _read_silver(root, "npb_spaia", "game_pbp", dt):
        rows.append(
            {
                "pa_id": ulid_from_key(f"pa:npb:{r.get('game_id')}:{r.get('event_id')}", dt.isoformat()),
                "game_id": ulid_from_key(f"game:npb_spaia:{r.get('game_id')}", dt.isoformat()),
                "inning": r.get("inning"),
                "is_top_inning": r.get("top_bottom") == "1",
                "batting_team_id": None,
                "fielding_team_id": None,
                "batter_id": bridge.get(("player", "npb_spaia", str(r.get("play_player_id")))),
                "pitcher_id": None,
                "event_type": "UNKNOWN",
                "rbi": None,
                "runs_scored_on_play": None,
                "outs_on_play": None,
                "base_state_before": None,
                "outs_before": None,
                "base_state_after": None,
                "outs_after": None,
                "dt": dt.isoformat(),
            }
        )
    # Retrosheet plays
    for r in _read_silver(root, "retrosheet_local", "game_pbp", dt):
        event_type = _map_retrosheet_event(r)
        base_before = _base_state_from_br(r.get("br1_pre"), r.get("br2_pre"), r.get("br3_pre"))
        base_after = _base_state_from_br(r.get("br1_post"), r.get("br2_post"), r.get("br3_post"))
        outs_before = r.get("outs_pre")
        outs_after = r.get("outs_post")
        outs_on_play = None
        if outs_before is not None and outs_after is not None:
            outs_on_play = outs_after - outs_before
        rows.append(
            {
                "pa_id": ulid_from_key(f"pa:retrosheet:{r.get('game_id')}:{r.get('event_id') or r.get('pn') or ''}", dt.isoformat()),
                "game_id": ulid_from_key(f"game:retrosheet_local:{r.get('game_id')}", dt.isoformat()),
                "inning": r.get("inning"),
                "is_top_inning": str(r.get("top_bottom")) in {"0", "Top", "top"},
                "batting_team_id": bridge.get(("team", "retrosheet_local", str(r.get("batting_team_id")))),
                "fielding_team_id": bridge.get(("team", "retrosheet_local", str(r.get("pitching_team_id")))),
                "batter_id": bridge.get(("player", "retrosheet_local", str(r.get("batter_id")))),
                "pitcher_id": bridge.get(("player", "retrosheet_local", str(r.get("pitcher_id")))),
                "event_type": event_type,
                "rbi": r.get("rbi"),
                "runs_scored_on_play": r.get("runs"),
                "outs_on_play": outs_on_play,
                "base_state_before": base_before,
                "outs_before": outs_before,
                "base_state_after": base_after,
                "outs_after": outs_after,
                "dt": dt.isoformat(),
            }
        )
    return _write_gold_table(root, "fact_plate_appearance", dt, rows, force)


def _write_fact_standings(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "npb_spaia", "standings", dt):
        rows.append(
            {
                "league_id": _league_id_for_code("NPB"),
                "season_id": ulid_from_key(f"season:NPB:{dt.year}", dt.isoformat()),
                "team_id": bridge.get(("team", "npb_spaia", str(r.get("team_id")))),
                "w": r.get("w"),
                "l": r.get("l"),
                "t": r.get("t"),
                "pct": r.get("pct"),
                "gb": r.get("gb"),
                "dt": dt.isoformat(),
            }
        )
    return _write_gold_table(root, "fact_standings", dt, rows, force)


def _write_fact_boxscore_batting(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "indy_local", "boxscore_batting", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:indy:{r.get('game_id')}", dt.isoformat()),
                "team_id": bridge.get(("team", "indy_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "indy_local", str(r.get("player_id")))),
                "ab": r.get("ab"),
                "h": r.get("h"),
                "2b": r.get("2b"),
                "3b": r.get("3b"),
                "hr": r.get("hr"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "rbi": r.get("rbi"),
                "r": r.get("r"),
                "dt": dt.isoformat(),
            }
        )
    for r in _read_silver(root, "retrosheet_local", "boxscore_batting", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:retrosheet:{r.get('game_id')}", dt.isoformat()),
                "team_id": bridge.get(("team", "retrosheet_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "retrosheet_local", str(r.get("player_id")))),
                "ab": r.get("ab"),
                "h": r.get("h"),
                "2b": r.get("2b"),
                "3b": r.get("3b"),
                "hr": r.get("hr"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "rbi": r.get("rbi"),
                "r": r.get("r"),
                "dt": dt.isoformat(),
            }
        )
    return _write_gold_table(root, "fact_boxscore_batting", dt, rows, force)


def _write_fact_boxscore_pitching(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    for r in _read_silver(root, "indy_local", "boxscore_pitching", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:indy:{r.get('game_id')}", dt.isoformat()),
                "team_id": bridge.get(("team", "indy_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "indy_local", str(r.get("player_id")))),
                "ip": r.get("ip"),
                "h": r.get("h"),
                "r": r.get("r"),
                "er": r.get("er"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "hr": r.get("hr"),
                "dt": dt.isoformat(),
            }
        )
    for r in _read_silver(root, "retrosheet_local", "boxscore_pitching", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:retrosheet:{r.get('game_id')}", dt.isoformat()),
                "team_id": bridge.get(("team", "retrosheet_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "retrosheet_local", str(r.get("player_id")))),
                "ip": r.get("ip"),
                "h": r.get("h"),
                "r": r.get("r"),
                "er": r.get("er"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "hr": r.get("hr"),
                "dt": dt.isoformat(),
            }
        )
    return _write_gold_table(root, "fact_boxscore_pitching", dt, rows, force)


def _write_run_expectancy(root: Path, dt: date, force: bool) -> Path:
    # Compute RE by base_state_before + outs_before using available runs_scored_on_play
    import duckdb
    con = duckdb.connect()
    path = root / "gold" / "fact_plate_appearance" / f"dt={dt.isoformat()}"
    if not path.exists():
        return _write_gold_table(root, "run_expectancy", dt, [], force)
    con.execute(f"CREATE OR REPLACE VIEW pa AS SELECT * FROM read_parquet('{path}/*.parquet')")
    rows = con.execute(
        """
        SELECT
          base_state_before,
          outs_before,
          AVG(COALESCE(runs_scored_on_play, 0)) AS exp_runs
        FROM pa
        WHERE base_state_before IS NOT NULL AND outs_before IS NOT NULL
        GROUP BY 1,2
        """
    ).fetchall()
    out_rows = [
        {"base_state": r[0], "outs": r[1], "exp_runs": float(r[2]), "dt": dt.isoformat()}
        for r in rows
    ]
    return _write_gold_table(root, "run_expectancy", dt, out_rows, force)


def _write_breakout_candidates(root: Path, dt: date, force: bool) -> Path:
    # Simple heuristic: top 20 HR on dt from boxscore_batting
    import duckdb
    con = duckdb.connect()
    path = root / "gold" / "fact_boxscore_batting" / f"dt={dt.isoformat()}"
    if not path.exists():
        return _write_gold_table(root, "breakout_candidates", dt, [], force)
    con.execute(f"CREATE OR REPLACE VIEW b AS SELECT * FROM read_parquet('{path}/*.parquet')")
    rows = con.execute(
        """
        SELECT player_id, SUM(hr) as hr, SUM(h) as h, SUM(ab) as ab
        FROM b
        WHERE player_id IS NOT NULL
        GROUP BY 1
        ORDER BY hr DESC, h DESC
        LIMIT 20
        """
    ).fetchall()
    out_rows = [
        {"player_id": r[0], "hr": int(r[1] or 0), "h": int(r[2] or 0), "ab": int(r[3] or 0), "dt": dt.isoformat()}
        for r in rows
    ]
    return _write_gold_table(root, "breakout_candidates", dt, out_rows, force)


def _map_event_type(value: Any) -> str:
    if not value:
        return "UNKNOWN"
    v = str(value).upper()
    if "STRIKEOUT" in v or v == "K":
        return "K"
    if "WALK" in v or v == "BB":
        return "BB"
    if "HBP" in v:
        return "HBP"
    if "HOME_RUN" in v or v == "HR":
        return "HR"
    if v in {"SINGLE", "1B"}:
        return "1B"
    if v in {"DOUBLE", "2B"}:
        return "2B"
    if v in {"TRIPLE", "3B"}:
        return "3B"
    if "ERROR" in v:
        return "ERROR"
    if "OUT" in v:
        return "OUT"
    return "UNKNOWN"


def _map_retrosheet_event(r: Dict[str, Any]) -> str:
    # Use explicit flags from plays.csv if present
    def _is_one(key: str) -> bool:
        v = r.get(key)
        return str(v) == "1"

    if _is_one("hr"):
        return "HR"
    if _is_one("triple"):
        return "3B"
    if _is_one("double"):
        return "2B"
    if _is_one("single"):
        return "1B"
    if _is_one("walk"):
        return "BB"
    if _is_one("hbp"):
        return "HBP"
    if _is_one("k"):
        return "K"
    if _is_one("roe"):
        return "ERROR"
    if _is_one("fc"):
        return "FC"
    if _is_one("sf"):
        return "SAC"
    if _is_one("sh"):
        return "SAC"
    if _is_one("gdp"):
        return "DP"
    if _is_one("tp"):
        return "TP"
    if _is_one("othout") or _is_one("noout"):
        return "OUT"
    return "UNKNOWN"


def _base_state_from_statcast(r: Dict[str, Any]) -> int | None:
    on_1b = r.get("on_1b")
    on_2b = r.get("on_2b")
    on_3b = r.get("on_3b")
    try:
        b1 = 1 if on_1b not in (None, "") else 0
        b2 = 1 if on_2b not in (None, "") else 0
        b3 = 1 if on_3b not in (None, "") else 0
        return b1 + (b2 * 2) + (b3 * 4)
    except Exception:
        return None


def _base_state_from_br(br1: Any, br2: Any, br3: Any) -> int | None:
    def _present(v: Any) -> int:
        if v in (None, "", "0", "NA"):
            return 0
        return 1
    return _present(br1) + (_present(br2) * 2) + (_present(br3) * 4)


def _write_gold_table(root: Path, table: str, dt: date, rows: List[Dict[str, Any]], force: bool) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    out_dir = root / "gold" / table / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if out_path.exists() and not force:
        return out_path
    if not rows:
        rows = [{"empty": True}]
    fmt = storage_format()
    if fmt == "parquet":
        table_data = pa.Table.from_pylist(rows)
        pq.write_table(table_data, out_path, use_dictionary=False)
    elif fmt == "delta":
        _write_delta(rows, out_dir)
    else:
        raise ValueError(f"Unsupported storage format: {fmt}")
    return out_path


def _write_delta(rows: List[Dict[str, Any]], out_dir: Path) -> None:
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta writes") from exc
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("overwrite").save(str(out_dir))


def _read_silver(root: Path, source: str, entity: str, dt: date) -> List[Dict[str, Any]]:
    path = root / "silver" / source / entity / f"dt={dt.isoformat()}"
    if not path.exists():
        return []
    fmt = storage_format()
    if fmt == "parquet":
        return ds.dataset(path, format="parquet").to_table().to_pylist()
    if fmt == "delta":
        try:
            from pyspark.sql import SparkSession
        except Exception as exc:
            raise RuntimeError("pyspark is required for delta reads") from exc
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.format("delta").load(str(path))
        return [row.asDict() for row in df.collect()]
    raise ValueError(f"Unsupported storage format: {fmt}")


def _league_id_for_code(code: str) -> str | None:
    leagues_path = Path("configs/leagues.yaml")
    if not leagues_path.exists():
        return None
    import yaml

    with leagues_path.open("r", encoding="utf-8") as f:
        leagues = yaml.safe_load(f)["leagues"]
    meta = leagues.get(code, {})
    return meta.get("league_id")
