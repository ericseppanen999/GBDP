from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

from gbdp.utils.ids import ulid_from_key
from gbdp.utils.io import (
    ensure_dir,
    gold_root,
    list_dir,
    path_exists,
    read_parquet_rows,
    silver_root,
    spark_path,
    stable_json_dumps,
    storage_format,
    write_parquet_table,
)
from gbdp.utils.time import utc_now
from gbdp.utils.time import daterange, parse_date


def publish_gold(start: str, end: str, root: Path | None = None, force: bool = False) -> List[Path]:
    root = root or gold_root()
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
    outputs.append(_write_feature_player_rolling_30d(root, dt, force))
    outputs.append(_write_fact_contract(root, dt, force))
    return outputs


def _read_gold_bridge(root: Path, dt: date) -> Dict[tuple, str]:
    path = root / "bridge_source_ids" / f"dt={dt.isoformat()}"
    if not path_exists(path):
        return {}
    fmt = storage_format()
    if fmt == "parquet":
        data = read_parquet_rows(path)
    elif fmt == "delta":
        try:
            from pyspark.sql import SparkSession
        except Exception as exc:
            raise RuntimeError("pyspark is required for delta reads") from exc
        spark = SparkSession.builder.getOrCreate()
        data = [row.asDict() for row in spark.read.format("delta").load(spark_path(path)).collect()]
    else:
        data = []
    out: Dict[tuple, str] = {}
    for r in data:
        entity_type = r.get("entity_type")
        source = r.get("source")
        source_id = r.get("source_id")
        canonical_id = r.get("canonical_id")
        if not (entity_type and source and source_id and canonical_id):
            continue
        out[(entity_type, source, source_id)] = canonical_id
    return out


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
    for r in _read_silver(root, "kbo_local", "games", dt):
        rows.extend(_team_rows_from_game(r, "KBO", bridge, "kbo_local"))
    for r in _read_silver(root, "lmb_local", "games", dt):
        rows.extend(_team_rows_from_game(r, "LMB", bridge, "lmb_local"))
    for r in _read_silver(root, "retrosheet_local", "games", dt):
        rows.extend(_team_rows_from_game(r, "MLB", bridge, "retrosheet_local"))
    dedup = {(r["team_id"], r["team_name"]): r for r in rows}
    scd_rows = _apply_scd2(
        root, "dim_team", dt, list(dedup.values()), key_field="team_id", attr_fields=["team_name", "team_abbrev", "home_city", "league_id"]
    )
    return _write_gold_table(root, "dim_team", dt, scd_rows, force)


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
    for r in _read_silver(root, "kbo_local", "rosters", dt):
        rows.append(_player_row(r, "KBO", bridge, "kbo_local"))
    for r in _read_silver(root, "lmb_local", "rosters", dt):
        rows.append(_player_row(r, "LMB", bridge, "lmb_local"))
    for r in _read_silver(root, "retrosheet_local", "rosters", dt):
        rows.append(_player_row(r, "MLB", bridge, "retrosheet_local"))
    dedup = {r["player_id"]: r for r in rows if r.get("player_id")}
    scd_rows = _apply_scd2(
        root, "dim_player", dt, list(dedup.values()), key_field="player_id", attr_fields=["primary_name", "bats", "throws", "primary_position", "league_id"]
    )
    return _write_gold_table(root, "dim_player", dt, scd_rows, force)


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
        {
            "season_id": ulid_from_key(f"season:KBO:{year}", dt.isoformat()),
            "league_id": _league_id_for_code("KBO"),
            "season_year": year,
            "season_type": "regular",
            "season_start_dt": f"{year}-03-01",
            "season_end_dt": f"{year}-10-31",
            "dt": dt.isoformat(),
        },
        {
            "season_id": ulid_from_key(f"season:LMB:{year}", dt.isoformat()),
            "league_id": _league_id_for_code("LMB"),
            "season_year": year,
            "season_type": "regular",
            "season_start_dt": f"{year}-03-01",
            "season_end_dt": f"{year}-09-30",
            "dt": dt.isoformat(),
        },
    ]
    return _write_gold_table(root, "dim_season", dt, rows, force)


def _write_fact_game(root: Path, dt: date, bridge: Dict[tuple, str], force: bool) -> Path:
    rows: List[Dict[str, Any]] = []
    rows.extend(_fact_game_from_silver(root, dt, "mlb_statsapi", "MLB", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "npb_spaia", "NPB", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "indy_local", "INDY", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "kbo_local", "KBO", bridge))
    rows.extend(_fact_game_from_silver(root, dt, "lmb_local", "LMB", bridge))
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
    for r in _read_silver(root, "kbo_local", "rosters", dt):
        rows.append(_fact_roster_row(r, "kbo_local", "KBO", bridge, dt))
    for r in _read_silver(root, "lmb_local", "rosters", dt):
        rows.append(_fact_roster_row(r, "lmb_local", "LMB", bridge, dt))
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
                "source": "mlb_statcast",
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
                "source": "mlb_statcast",
            }
    rows.extend(pa_map.values())
    # NPB from PBP (minimal)
    npb_rows = _read_silver(root, "npb_spaia", "game_pbp", dt)
    rows.extend(_reconstruct_npb_pa(npb_rows, dt, bridge))
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
                "source": "retrosheet_local",
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
                "source": "npb_spaia",
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
                "source": "indy_local",
            }
        )
    for r in _read_silver(root, "kbo_local", "boxscore_batting", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:kbo:{r.get('game_id')}", dt.isoformat()),
                "team_id": bridge.get(("team", "kbo_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "kbo_local", str(r.get("player_id")))),
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
                "source": "kbo_local",
            }
        )
    for r in _read_silver(root, "lmb_local", "boxscore_batting", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:lmb:{r.get('game_id')}", dt.isoformat()),
                "team_id": bridge.get(("team", "lmb_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "lmb_local", str(r.get("player_id")))),
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
                "source": "lmb_local",
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
                "source": "retrosheet_local",
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
                "source": "indy_local",
            }
        )
    for r in _read_silver(root, "kbo_local", "boxscore_pitching", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:kbo:{r.get('game_id')}", dt.isoformat()),
                "team_id": bridge.get(("team", "kbo_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "kbo_local", str(r.get("player_id")))),
                "ip": r.get("ip"),
                "h": r.get("h"),
                "r": r.get("r"),
                "er": r.get("er"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "hr": r.get("hr"),
                "dt": dt.isoformat(),
                "source": "kbo_local",
            }
        )
    for r in _read_silver(root, "lmb_local", "boxscore_pitching", dt):
        rows.append(
            {
                "game_id": ulid_from_key(f"game:lmb:{r.get('game_id')}", dt.isoformat()),
                "team_id": bridge.get(("team", "lmb_local", str(r.get("team_id")))),
                "player_id": bridge.get(("player", "lmb_local", str(r.get("player_id")))),
                "ip": r.get("ip"),
                "h": r.get("h"),
                "r": r.get("r"),
                "er": r.get("er"),
                "bb": r.get("bb"),
                "so": r.get("so"),
                "hr": r.get("hr"),
                "dt": dt.isoformat(),
                "source": "lmb_local",
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
                "source": "retrosheet_local",
            }
        )
    return _write_gold_table(root, "fact_boxscore_pitching", dt, rows, force)


def _write_run_expectancy(root: Path, dt: date, force: bool) -> Path:
    # Compute RE by base_state_before + outs_before using available runs_scored_on_play
    fmt = storage_format()
    path = root / "fact_plate_appearance" / f"dt={dt.isoformat()}"
    if not path_exists(path):
        return _write_gold_table(root, "run_expectancy", dt, [], force)
    if fmt == "parquet":
        import duckdb
        con = duckdb.connect()
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
    else:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = SparkSession.builder.getOrCreate()
        df = spark.read.format("delta").load(spark_path(path))
        df = df.where("base_state_before IS NOT NULL AND outs_before IS NOT NULL")
        rows = [
            (r["base_state_before"], r["outs_before"], r["exp_runs"])
            for r in df.groupBy("base_state_before", "outs_before")
            .agg(F.avg(F.coalesce("runs_scored_on_play", F.lit(0))).alias("exp_runs"))
            .collect()
        ]
    out_rows = [
        {"base_state": r[0], "outs": r[1], "exp_runs": float(r[2]), "dt": dt.isoformat()}
        for r in rows
    ]
    return _write_gold_table(root, "run_expectancy", dt, out_rows, force)


def _write_breakout_candidates(root: Path, dt: date, force: bool) -> Path:
    # Simple heuristic: top 20 HR on dt from boxscore_batting
    fmt = storage_format()
    path = root / "fact_boxscore_batting" / f"dt={dt.isoformat()}"
    if not path_exists(path):
        return _write_gold_table(root, "breakout_candidates", dt, [], force)
    if fmt == "parquet":
        import duckdb
        con = duckdb.connect()
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
    else:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = SparkSession.builder.getOrCreate()
        df = spark.read.format("delta").load(spark_path(path))
        rows = [
            (r["player_id"], r["hr"], r["h"], r["ab"])
            for r in df.where("player_id IS NOT NULL")
            .groupBy("player_id")
            .agg(F.sum("hr").alias("hr"), F.sum("h").alias("h"), F.sum("ab").alias("ab"))
            .orderBy(F.col("hr").desc(), F.col("h").desc())
            .limit(20)
            .collect()
        ]
    out_rows = [
        {"player_id": r[0], "hr": int(r[1] or 0), "h": int(r[2] or 0), "ab": int(r[3] or 0), "dt": dt.isoformat()}
        for r in rows
    ]
    return _write_gold_table(root, "breakout_candidates", dt, out_rows, force)


def _write_feature_player_rolling_30d(root: Path, dt: date, force: bool) -> Path:
    # Rolling 30d from fact_boxscore_batting across leagues
    fmt = storage_format()
    if fmt == "parquet":
        import duckdb
        con = duckdb.connect()
        paths = []
        for offset in range(0, 30):
            d = (dt - timedelta(days=offset)).isoformat()
            path = root / "fact_boxscore_batting" / f"dt={d}"
            if path_exists(path):
                paths.append(f"{path}/*.parquet")
        if not paths:
            return _write_gold_table(root, "feature_player_rolling_30d", dt, [], force)
        paths_sql = ", ".join(f"'{p}'" for p in paths)
        con.execute(f"CREATE OR REPLACE VIEW b AS SELECT * FROM read_parquet([{paths_sql}])")
        rows = con.execute(
            """
            SELECT
              player_id,
              SUM(ab) AS ab,
              SUM(h) AS h,
              SUM(hr) AS hr,
              SUM(rbi) AS rbi,
              SUM(bb) AS bb,
              SUM(so) AS so
            FROM b
            WHERE player_id IS NOT NULL
            GROUP BY 1
            """
        ).fetchall()
        out_rows = [
            {"player_id": r[0], "ab": int(r[1] or 0), "h": int(r[2] or 0), "hr": int(r[3] or 0),
             "rbi": int(r[4] or 0), "bb": int(r[5] or 0), "so": int(r[6] or 0), "window_days": 30, "dt": dt.isoformat()}
            for r in rows
        ]
        return _write_gold_table(root, "feature_player_rolling_30d", dt, out_rows, force)
    else:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.getOrCreate()
        start_dt = (dt - timedelta(days=29)).isoformat()
        dfs = []
        for offset in range(0, 30):
            d = (dt - timedelta(days=offset)).isoformat()
            path = root / "fact_boxscore_batting" / f"dt={d}"
            if path_exists(path):
                dfs.append(spark.read.format("delta").load(spark_path(path)))
        if not dfs:
            return _write_gold_table(root, "feature_player_rolling_30d", dt, [], force)
        df = dfs[0]
        for part in dfs[1:]:
            df = df.unionByName(part, allowMissingColumns=True)
        df = df.where((F.col("dt") >= start_dt) & (F.col("dt") <= dt.isoformat()))
        agg = (
            df.where("player_id IS NOT NULL")
            .groupBy("player_id")
            .agg(
                F.sum("ab").alias("ab"),
                F.sum("h").alias("h"),
                F.sum("hr").alias("hr"),
                F.sum("rbi").alias("rbi"),
                F.sum("bb").alias("bb"),
                F.sum("so").alias("so"),
            )
        )
        out_rows = [
            {
                "player_id": r["player_id"],
                "ab": int(r["ab"] or 0),
                "h": int(r["h"] or 0),
                "hr": int(r["hr"] or 0),
                "rbi": int(r["rbi"] or 0),
                "bb": int(r["bb"] or 0),
                "so": int(r["so"] or 0),
                "window_days": 30,
                "dt": dt.isoformat(),
            }
            for r in agg.collect()
        ]
        return _write_gold_table(root, "feature_player_rolling_30d", dt, out_rows, force)


def _write_fact_contract(root: Path, dt: date, force: bool) -> Path:
    # Placeholder until contract sources are integrated
    return _write_gold_table(root, "fact_contract", dt, [], force)


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


def _reconstruct_npb_pa(rows: List[Dict[str, Any]], dt: date, bridge: Dict[tuple, str]) -> List[Dict[str, Any]]:
    # Best-effort base/out reconstruction using text info; falls back to UNKNOWN
    by_game: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        gid = str(r.get("game_id") or "")
        by_game.setdefault(gid, []).append(r)
    out: List[Dict[str, Any]] = []
    for gid, items in by_game.items():
        items.sort(key=lambda x: (x.get("inning") or 0, x.get("play_seq_no") or x.get("event_id") or 0))
        bases = [0, 0, 0]
        outs = 0
        last_inning = None
        last_top = None
        for r in items:
            inning = r.get("inning")
            top = r.get("top_bottom")
            if (inning, top) != (last_inning, last_top):
                bases = [0, 0, 0]
                outs = 0
                last_inning, last_top = inning, top
            text = (r.get("text_info_text") or r.get("text_info_name") or "").lower()
            event_type, outs_on_play, bases_after = _npb_infer_event(text, bases)
            base_before = _bases_to_state(bases)
            outs_before = outs
            outs = min(3, outs + outs_on_play)
            base_after = _bases_to_state(bases_after)
            outs_after = outs
            bases = bases_after
            out.append(
                {
                    "pa_id": ulid_from_key(f"pa:npb:{gid}:{r.get('event_id') or r.get('play_seq_no')}", dt.isoformat()),
                    "game_id": ulid_from_key(f"game:npb_spaia:{gid}", dt.isoformat()),
                    "inning": inning,
                    "is_top_inning": str(top) == "1",
                    "batting_team_id": None,
                    "fielding_team_id": None,
                    "batter_id": bridge.get(("player", "npb_spaia", str(r.get("play_player_id")))),
                    "pitcher_id": None,
                    "event_type": event_type,
                    "rbi": None,
                    "runs_scored_on_play": None,
                    "outs_on_play": outs_on_play,
                    "base_state_before": base_before,
                    "outs_before": outs_before,
                    "base_state_after": base_after,
                    "outs_after": outs_after,
                    "dt": dt.isoformat(),
                    "source": "npb_spaia",
                }
            )
    return out


def _npb_infer_event(text: str, bases: List[int]) -> tuple[str, int, List[int]]:
    # bases: [on1, on2, on3]
    t = text or ""
    if "三重殺" in t or "triple play" in t:
        return "TP", 3, [0, 0, 0]
    if "併殺" in t or "double play" in t:
        return "DP", 2, [0, 0, 0]
    if "本塁打" in t or "ホームラン" in t or "home run" in t:
        return "HR", 0, [0, 0, 0]
    if "三塁打" in t or "triple" in t:
        return "3B", 0, [0, 0, 1]
    if "二塁打" in t or "double" in t:
        return "2B", 0, _advance_bases(bases, 2)
    if "安打" in t or "ヒット" in t or "single" in t:
        return "1B", 0, _advance_bases(bases, 1)
    if "四球" in t or "死球" in t or "walk" in t or "hbp" in t:
        return "BB", 0, _advance_bases(bases, 1)
    if "犠打" in t or "犠飛" in t or "sacrifice" in t:
        return "SAC", 1, bases
    if "アウト" in t or "out" in t or "フライ" in t or "ゴロ" in t:
        return "OUT", 1, bases
    return "UNKNOWN", 0, bases


def _advance_bases(bases: List[int], bases_taken: int) -> List[int]:
    # Simplified forced advance
    b1, b2, b3 = bases
    if bases_taken == 1:
        if b1:
            if b2:
                b3 = 1
            b2 = 1
        b1 = 1
    elif bases_taken == 2:
        if b2:
            b3 = 1
        if b1:
            b3 = 1
        b2 = 1
        b1 = 0
    return [b1, b2, b3]


def _bases_to_state(bases: List[int]) -> int:
    return (1 if bases[0] else 0) + (2 if bases[1] else 0) + (4 if bases[2] else 0)


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
    out_dir = root / table / f"dt={dt.isoformat()}"
    ensure_dir(out_dir)
    out_path = out_dir / "part-00001.parquet"
    if path_exists(out_path) and not force:
        return out_path
    if not rows:
        rows = [{"empty": True}]
    now = utc_now().isoformat()
    for r in rows:
        if "dt" not in r:
            r["dt"] = dt.isoformat()
        if "ingested_at_utc" not in r:
            r["ingested_at_utc"] = now
    fmt = storage_format()
    if fmt == "parquet":
        import pyarrow as pa
        table_data = pa.Table.from_pylist(rows)
        write_parquet_table(table_data, out_path, force=True)
    elif fmt == "delta":
        _write_delta(rows, out_dir, table)
    else:
        raise ValueError(f"Unsupported storage format: {fmt}")
    return out_path


def _write_delta(rows: List[Dict[str, Any]], out_dir: Path, table: str) -> None:
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta writes") from exc
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(rows)
    key_cols = _gold_primary_keys().get(table)
    if not key_cols:
        df.write.format("delta").mode("overwrite").save(spark_path(out_dir))
        return
    try:
        from delta.tables import DeltaTable
    except Exception:
        df.write.format("delta").mode("overwrite").save(spark_path(out_dir))
        return
    path = spark_path(out_dir)
    if not _delta_exists(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        return
    delta_table = DeltaTable.forPath(spark, path)
    cond = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])
    (
        delta_table.alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _delta_exists(spark, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False


def _gold_primary_keys() -> Dict[str, List[str]]:
    return {
        "dim_league": ["league_id", "dt"],
        "dim_team": ["team_id", "valid_from_dt"],
        "dim_player": ["player_id", "valid_from_dt"],
        "dim_season": ["season_id"],
        "fact_game": ["game_id", "dt"],
        "fact_roster": ["team_id", "player_id", "season_id", "dt"],
        "fact_transaction": ["transaction_id"],
        "fact_contract": ["contract_id", "dt"],
        "fact_plate_appearance": ["pa_id", "dt"],
        "fact_pitch": ["pitch_id", "dt"],
        "fact_boxscore_batting": ["game_id", "player_id", "dt"],
        "fact_boxscore_pitching": ["game_id", "player_id", "dt"],
        "fact_standings": ["team_id", "season_id", "dt"],
        "run_expectancy": ["base_state", "outs", "dt"],
        "breakout_candidates": ["player_id", "dt"],
        "feature_player_rolling_30d": ["player_id", "dt"],
    }


def _read_silver(root: Path, source: str, entity: str, dt: date) -> List[Dict[str, Any]]:
    path = silver_root() / source / entity / f"dt={dt.isoformat()}"
    if not path_exists(path):
        return []
    fmt = storage_format()
    if fmt == "parquet":
        return read_parquet_rows(path)
    if fmt == "delta":
        try:
            from pyspark.sql import SparkSession
        except Exception as exc:
            raise RuntimeError("pyspark is required for delta reads") from exc
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.format("delta").load(spark_path(path))
        return [row.asDict() for row in df.collect()]
    raise ValueError(f"Unsupported storage format: {fmt}")


def _apply_scd2(
    root: Path,
    table: str,
    dt: date,
    new_rows: List[Dict[str, Any]],
    key_field: str,
    attr_fields: List[str],
) -> List[Dict[str, Any]]:
    prev_dt = _latest_dt_before(root, table, dt)
    if not prev_dt:
        for r in new_rows:
            r["valid_from_dt"] = dt.isoformat()
            r["valid_to_dt"] = None
            r["is_current"] = True
        return new_rows
    prev_rows = _read_gold_snapshot(root, table, prev_dt)
    return _scd2_merge(prev_rows, new_rows, key_field, attr_fields, dt, prev_dt)


def _attrs_equal(a: Dict[str, Any], b: Dict[str, Any], fields: List[str]) -> bool:
    for f in fields:
        if a.get(f) != b.get(f):
            return False
    return True


def _scd2_merge(
    prev_rows: List[Dict[str, Any]],
    new_rows: List[Dict[str, Any]],
    key_field: str,
    attr_fields: List[str],
    dt: date,
    prev_dt: date,
) -> List[Dict[str, Any]]:
    prev_by_key: Dict[str, Dict[str, Any]] = {str(r.get(key_field)): r for r in prev_rows if r.get(key_field)}
    out_rows: List[Dict[str, Any]] = []
    seen_keys = set()
    prev_dt_str = prev_dt.isoformat()
    for r in new_rows:
        key = str(r.get(key_field))
        seen_keys.add(key)
        prev = prev_by_key.get(key)
        if not prev:
            r["valid_from_dt"] = dt.isoformat()
            r["valid_to_dt"] = None
            r["is_current"] = True
            out_rows.append(r)
            continue
        if _attrs_equal(prev, r, attr_fields):
            r["valid_from_dt"] = prev.get("valid_from_dt") or prev_dt_str
            r["valid_to_dt"] = None
            r["is_current"] = True
            out_rows.append(r)
        else:
            prev_closed = dict(prev)
            prev_closed["valid_to_dt"] = (dt - timedelta(days=1)).isoformat()
            prev_closed["is_current"] = False
            out_rows.append(prev_closed)
            r["valid_from_dt"] = dt.isoformat()
            r["valid_to_dt"] = None
            r["is_current"] = True
            out_rows.append(r)
    for key, prev in prev_by_key.items():
        if key in seen_keys:
            continue
        prev_closed = dict(prev)
        prev_closed["valid_to_dt"] = (dt - timedelta(days=1)).isoformat()
        prev_closed["is_current"] = False
        out_rows.append(prev_closed)
    return out_rows


def _latest_dt_before(root: Path, table: str, dt: date) -> date | None:
    base = root / table
    if not path_exists(base):
        return None
    candidates: List[date] = []
    for part in list_dir(base, dirs_only=True):
        name = part.name
        if not name.startswith("dt="):
            continue
        try:
            d = date.fromisoformat(name.split("=", 1)[1])
        except Exception:
            continue
        if d < dt:
            candidates.append(d)
    if not candidates:
        return None
    return sorted(candidates)[-1]


def _read_gold_snapshot(root: Path, table: str, dt: date) -> List[Dict[str, Any]]:
    path = root / table / f"dt={dt.isoformat()}"
    if not path_exists(path):
        return []
    fmt = storage_format()
    if fmt == "parquet":
        return read_parquet_rows(path)
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise RuntimeError("pyspark is required for delta reads") from exc
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("delta").load(spark_path(path))
    return [row.asDict() for row in df.collect()]


def _league_id_for_code(code: str) -> str | None:
    leagues_path = Path("configs/leagues.yaml")
    if not leagues_path.exists():
        return None
    import yaml

    with leagues_path.open("r", encoding="utf-8") as f:
        leagues = yaml.safe_load(f)["leagues"]
    meta = leagues.get(code, {})
    return meta.get("league_id")
