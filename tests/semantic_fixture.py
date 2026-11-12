"""A small silver layer, run through the real identity and gold code.

Every gold table the semantic layer describes is produced from this by
`gbdp.identity.resolver.resolve_identity` and `gbdp.gold.publish.publish_gold`,
so the schema tests check the YAML against what the code really writes, and the
metric tests run against real gold, not hand-written parquet.

The numbers are small enough to check by hand. On 2026-05-01, MLB game 1001
(BOS at NYY, 3-0): one complete bottom 1st for NYY batter 1 against BOS pitcher
2, then a game-ending triple play in the top 2nd.

    PA  state before   event       runs  rest of inning
    1   ---, 0 out     single      0     3
    2   1--, 0 out     home run    2     3
    3   ---, 0 out     strikeout   0     1
    4   ---, 1 out     walk        0     1
    5   1--, 1 out     field out   0     1
    6   1--, 2 out     double      0     1     (runner to third)
    7   -23, 2 out     TRUNCATED   1     1     (wild pitch scores one, then caught stealing home)
    8   12-, 0 out     triple play 0     0     (top 2nd, BOS batter 3 against NYY pitcher 4)

Batter 1, PAs 1-6 (PA 7 is TRUNCATED, not a PA): 6 PA, 5 AB, 3 H, 1 HR, 1 BB,
1 K, 7 TB: AVG .600, OBP .667, SLG 1.400. RE24 for PAs 1-7 sums to +1.0 (3 runs
scored from a 2.0-run start).
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

DAY1 = date(2026, 5, 1)
DAY2 = date(2026, 5, 2)

NYY, BOS = "147", "111"
NPB_HOME, NPB_AWAY = "11", "6"


def _write(silver: Path, source: str, entity: str, day: date, rows: List[Dict[str, Any]]) -> None:
    part = silver / source / entity / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), part / "part-00001.parquet")


def _pitch(ab, n, inning, half, outs, pitch_type, speed, desc, events=None, bases=(None, None, None),
           score=(0, 0), batter="1", pitcher="2", balls=0, strikes=0):
    on_1b, on_2b, on_3b = bases
    return {
        "game_pk": "1001", "at_bat_number": ab, "pitch_number": n, "inning": inning, "inning_topbot": half,
        "outs_when_up": outs, "on_1b": on_1b, "on_2b": on_2b, "on_3b": on_3b, "events": events,
        "description": desc, "pitch_type": pitch_type, "release_speed": speed, "plate_x": 0.1, "plate_z": 2.5,
        "balls": balls, "strikes": strikes, "batter_id": batter, "pitcher_id": pitcher,
        "raw_json": json.dumps({"bat_score": score[0], "post_bat_score": score[1], **_savant_outcome(events, desc)}),
    }


# Savant's per-PA fields on a PA-ending pitch (feature_contact_daily / feature_pitch_daily read them).
_WOBA = {"walk": 0.69, "hit_by_pitch": 0.72, "single": 0.88, "double": 1.25, "triple": 1.58, "home_run": 2.03}


def _savant_outcome(events, desc) -> Dict[str, Any]:
    if not events or events in ("caught_stealing_2b", "truncated_pa"):
        return {}
    out: Dict[str, Any] = {"woba_denom": "1", "woba_value": str(_WOBA.get(events, 0.0))}
    if desc == "hit_into_play":
        out.update(estimated_woba_using_speedangle="0.4", bb_type="line_drive",
                   babip_value="1" if events in ("single", "double", "triple") else "0")
    return out


def _statcast_pitches() -> List[Dict[str, Any]]:
    on1, on23, on12 = ("x", None, None), (None, "x", "x"), ("x", "x", None)
    return [
        # PA 1: single
        _pitch(1, 1, 1, "Bot", 0, "FF", 95.0, "ball"),
        _pitch(1, 2, 1, "Bot", 0, "SL", 85.0, "hit_into_play", "single", balls=1),
        # PA 2: two-run homer
        _pitch(2, 1, 1, "Bot", 0, "FF", 97.0, "hit_into_play", "home_run", bases=on1, score=(0, 2)),
        # PA 3: strikeout (two whiffs and a foul)
        _pitch(3, 1, 1, "Bot", 0, "SL", 86.0, "swinging_strike", score=(2, 2)),
        _pitch(3, 2, 1, "Bot", 0, "FF", 96.0, "foul", strikes=1, score=(2, 2)),
        _pitch(3, 3, 1, "Bot", 0, "SL", 84.0, "swinging_strike_blocked", "strikeout", strikes=2, score=(2, 2)),
        # PA 4: walk
        _pitch(4, 1, 1, "Bot", 1, "FF", 94.0, "ball", score=(2, 2)),
        _pitch(4, 2, 1, "Bot", 1, "FF", 93.0, "ball", balls=1, score=(2, 2)),
        _pitch(4, 3, 1, "Bot", 1, "FF", 95.0, "ball", balls=2, score=(2, 2)),
        _pitch(4, 4, 1, "Bot", 1, "FF", 96.0, "ball", "walk", balls=3, score=(2, 2)),
        # PA 5: field out, runner holds
        _pitch(5, 1, 1, "Bot", 1, "CH", 88.0, "hit_into_play", "field_out", bases=on1, score=(2, 2)),
        # PA 6: double, runner to third
        _pitch(6, 1, 1, "Bot", 2, "FF", 95.0, "called_strike", bases=on1, score=(2, 2)),
        _pitch(6, 2, 1, "Bot", 2, "FF", 96.0, "hit_into_play", "double", bases=on1, strikes=1, score=(2, 2)),
        # PA 7: a wild pitch scores the runner from third, then the runner from
        # second is caught stealing home for the third out: a truncated PA.
        _pitch(7, 1, 1, "Bot", 2, "SL", 85.0, "ball", "truncated_pa", bases=on23, score=(2, 3)),
        # PA 8: game-ending triple play, top 2nd
        _pitch(8, 1, 2, "Top", 0, "FF", 92.0, "hit_into_play", "triple_play", bases=on12,
               batter="3", pitcher="4"),
    ]


def _npb_event(event_id, ab, sec, text, inning=1, top="1"):
    return {"game_id": "n1", "inning": inning, "top_bottom": top, "play_ab": ab, "play_seq_no": ab,
            "event_id": str(event_id), "play_player_id": "np1", "text_info_text": text,
            "raw_event_json": json.dumps({"TextInfo_Bat_Text_Sec": sec})}


def _npb_pbp() -> List[Dict[str, Any]]:
    # Top 1st, the away team (広) bats: single, two-run homer, ground out,
    # ground out, then the inning ends on a caught stealing (TRUNCATED).
    return [
        _npb_event(1, 1, "14", "試合開始 1回表 西0-0広　広島は1番名原からの攻撃"),
        _npb_event(2, 1, "6", "＜1番：名原＞無死走者なし"),
        _npb_event(3, 1, "1", "2球目:レフトへのヒットを放つ 一塁"),
        _npb_event(4, 2, "6", "＜2番：菊池＞無死一塁"),
        _npb_event(5, 2, "1", "1球目:カウント0-0から左中間への2ランホームラン 西 0-2 広 "),
        _npb_event(6, 3, "6", "＜3番：小園＞無死走者なし"),
        _npb_event(7, 3, "1", "1球目:サードゴロ 1アウト"),
        _npb_event(8, 4, "6", "＜4番：坂倉＞一死走者なし"),
        _npb_event(9, 4, "1", "3球目:フォアボールを選ぶ 一塁"),
        _npb_event(10, 5, "6", "＜5番：末包＞一死一塁"),
        _npb_event(11, 5, "1", "1球目:サードゴロ 2アウト"),
        _npb_event(12, 6, "6", "＜6番：大盛＞二死二塁"),
        _npb_event(13, 6, "3", "二塁走者坂倉:盗塁失敗 3アウト"),
    ]


def build_silver(silver: Path) -> None:
    """Every (source, entity) that gold/publish.py and identity read, on two days."""
    d1, d2 = DAY1, DAY2

    # --- MLB Stats API -----------------------------------------------------
    game_1001 = {"game_id": "1001", "game_date": d1.isoformat(), "home_team_id": NYY, "home_team_name": "New York Yankees",
                 "away_team_id": BOS, "away_team_name": "Boston Red Sox", "venue": "Yankee Stadium",
                 "status": "Final", "game_type": "R", "home_score": 3, "away_score": 0}
    _write(silver, "mlb_statsapi", "games", d1, [game_1001])
    _write(silver, "mlb_statsapi", "games", d2, [
        # 1001 shows up again on the next day's schedule: it must count once.
        game_1001,
        {"game_id": "1002", "game_date": d2.isoformat(), "home_team_id": BOS, "home_team_name": "Boston Red Sox",
         "away_team_id": NYY, "away_team_name": "New York Yankees", "venue": "Fenway Park",
         "status": "Final", "game_type": "R", "home_score": 4, "away_score": 5},
        # An All-Star game (A): not part of either team's regular season.
        {"game_id": "1004", "game_date": d2.isoformat(), "home_team_id": NYY, "home_team_name": "New York Yankees",
         "away_team_id": BOS, "away_team_name": "Boston Red Sox", "venue": "Yankee Stadium",
         "status": "Final", "game_type": "A", "home_score": 9, "away_score": 1},
        {"game_id": "1003", "game_date": d2.isoformat(), "home_team_id": BOS, "home_team_name": "Boston Red Sox",
         "away_team_id": NYY, "away_team_name": "New York Yankees", "venue": "Fenway Park",
         "status": "Postponed", "game_type": "R", "home_score": None, "away_score": None},
    ])
    teams = [{"team_id": NYY, "team_name": "New York Yankees", "team_abbrev": "NYY", "home_city": "Bronx"},
             {"team_id": BOS, "team_name": "Boston Red Sox", "team_abbrev": "BOS", "home_city": "Boston"}]
    rosters = [{"team_id": NYY, "player_id": "1", "player_name": "Aaron Batter", "position": "RF"},
               {"team_id": BOS, "player_id": "2", "player_name": "Pat Pitcher", "position": "P"},
               {"team_id": BOS, "player_id": "3", "player_name": "Bo Hitter", "position": "SS"},
               {"team_id": NYY, "player_id": "4", "player_name": "Quinn Arm", "position": "P"}]
    bios = [{"player_id": "1", "player_name": "Aaron Batter", "dob": "1992-04-26", "bats": "R", "throws": "R",
             "height_cm": 200.7, "weight_kg": 127.0, "birth_country": "USA"},
            # On the season list but not an active roster that day.
            {"player_id": "5", "player_name": "Rex Reliever", "dob": "1995-01-01", "bats": "L", "throws": "L",
             "height_cm": 190.0, "weight_kg": 95.0, "birth_country": "Japan"}]
    for d in (d1, d2):
        _write(silver, "mlb_statsapi", "teams", d, teams)
        _write(silver, "mlb_statsapi", "rosters", d, rosters)
        _write(silver, "mlb_statsapi", "player_bios", d, bios)
    _write(silver, "mlb_statsapi", "transactions", d1, [
        {"transaction_id": "t1", "team_id": NYY, "player_id": "4", "type_desc": "Recalled",
         "date": d1.isoformat(), "description": "Recalled Quinn Arm."}])

    def bat(pid, team, ab, h, d2b, d3b, hr, bb, so, rbi, r, game="1001"):
        return {"game_id": game, "team_id": team, "player_id": pid, "ab": ab, "h": h, "2b": d2b, "3b": d3b,
                "hr": hr, "bb": bb, "so": so, "rbi": rbi, "r": r}

    _write(silver, "mlb_statsapi", "boxscore_batting", d1, [
        bat("1", NYY, 5, 3, 1, 0, 1, 1, 1, 3, 2), bat("3", BOS, 4, 1, 0, 0, 0, 0, 2, 0, 0)])
    _write(silver, "mlb_statsapi", "boxscore_batting", d2, [bat("1", NYY, 4, 1, 0, 1, 0, 0, 2, 1, 1, game="1002")])

    def pit(pid, team, ip, h, r, er, bb, so, hr, game="1001"):
        return {"game_id": game, "team_id": team, "player_id": pid, "ip": ip, "h": h, "r": r, "er": er,
                "bb": bb, "so": so, "hr": hr}

    _write(silver, "mlb_statsapi", "boxscore_pitching", d1, [
        pit("2", BOS, "6.2", 5, 3, 2, 1, 7, 1), pit("5", BOS, "0.1", 0, 0, 0, 0, 1, 0),
        pit("4", NYY, "9.0", 2, 0, 0, 1, 10, 0)])
    _write(silver, "mlb_statsapi", "boxscore_pitching", d2, [pit("2", BOS, "5.1", 6, 3, 3, 2, 4, 0, game="1002")])

    # --- Statcast ------------------------------------------------------------
    _write(silver, "mlb_statcast", "pitches", d1, _statcast_pitches())

    # --- NPB (SPAIA) -----------------------------------------------------------
    _write(silver, "npb_spaia", "games", d2, [
        {"game_id": "n1", "game_date": d2.isoformat(), "home_team_id": NPB_HOME, "home_team_name": "西武",
         "away_team_id": NPB_AWAY, "away_team_name": "広島", "home_score": 2, "away_score": 2,
         "game_state_name": "試合終了", "game_type_name": "セ・パ交流戦"},
        # Postseason, and a game type no rule knows (other).
        {"game_id": "n2", "game_date": d2.isoformat(), "home_team_id": NPB_HOME, "home_team_name": "西武",
         "away_team_id": NPB_AWAY, "away_team_name": "広島", "home_score": 3, "away_score": 1,
         "game_state_name": "試合終了", "game_type_name": "クライマックスシリーズ"},
        # The exact SPAIA strings: a called-off league game (never counts),
        # a Japan Series game and a Pacific League game.
        {"game_id": "n4", "game_date": d2.isoformat(), "home_team_id": NPB_HOME, "home_team_name": "西武",
         "away_team_id": NPB_AWAY, "away_team_name": "広島", "home_score": 0, "away_score": 0,
         "game_state_name": "試合中止", "game_type_name": "セ・リーグ公式戦"},
        {"game_id": "n5", "game_date": d2.isoformat(), "home_team_id": NPB_HOME, "home_team_name": "西武",
         "away_team_id": NPB_AWAY, "away_team_name": "広島", "home_score": 2, "away_score": 1,
         "game_state_name": "試合終了", "game_type_name": "日本シリーズ"},
        {"game_id": "n6", "game_date": d2.isoformat(), "home_team_id": NPB_HOME, "home_team_name": "西武",
         "away_team_id": NPB_AWAY, "away_team_name": "広島", "home_score": 1, "away_score": 0,
         "game_state_name": "試合終了", "game_type_name": "パ・リーグ公式戦"},
        {"game_id": "n3", "game_date": d2.isoformat(), "home_team_id": NPB_HOME, "home_team_name": "西武",
         "away_team_id": NPB_AWAY, "away_team_name": "広島", "home_score": 0, "away_score": 4,
         "game_state_name": "試合終了", "game_type_name": "ファーム交流戦"}])
    _write(silver, "npb_spaia", "rosters", d2, [
        {"team_id": NPB_AWAY, "player_id": "np1", "player_name": "名原　典彦", "position": "OF", "role": "player",
         "raw_json": json.dumps({"PlayerNameK": "ナハラ　ノリヒコ"})},
        {"team_id": NPB_AWAY, "player_id": "np9", "player_name": "監督", "position": None, "role": "manager",
         "raw_json": "{}"}])
    _write(silver, "npb_spaia", "game_pbp", d2, _npb_pbp())
    _write(silver, "npb_spaia", "standings", d2, [
        {"team_id": NPB_HOME, "w": 20, "l": 10, "t": 1, "pct": ".667", "gb": "-"}])

    # --- KBO (official API) ------------------------------------------------------
    _write(silver, "kbo_api", "games", d1, [
        {"game_id": "k1", "game_date": d1.isoformat(), "home_team_id": "LG", "home_team_name": "LG",
         "away_team_id": "OB", "away_team_name": "두산", "home_score": 6, "away_score": 1, "status": "Final"}])
    _write(silver, "kbo_api", "boxscore_batting", d1, [
        {"game_id": "k1", "team_id": "LG", "player_name": "김철수", "ab": 4, "h": 2, "rbi": 1, "r": 1}])
    _write(silver, "kbo_api", "boxscore_pitching", d1, [
        {"game_id": "k1", "team_id": "LG", "player_name": "박투수", "ip": "5 2/3", "h": 4, "r": 1, "er": 1,
         "bb": 2, "so": 5, "hr": 0},
        {"game_id": "k1", "team_id": "LG", "player_name": "이불펜", "ip": "1/3", "h": 0, "r": 0, "er": 0,
         "bb": 0, "so": 1, "hr": 0}])

    # --- Retrosheet (one unfinished half-inning, so it stays out of RE) ---------
    _write(silver, "retrosheet_local", "game_pbp", d1, [
        {"game_id": "NYA202605010", "event_id": 1, "inning": 1, "top_bottom": 0, "batting_team_id": "BOS",
         "pitching_team_id": "NYA", "batter_id": "r1", "pitcher_id": "r2", "walk": 1, "rbi": 0, "runs": 0,
         "outs_pre": 0, "outs_post": 0, "br1_pre": None, "br2_pre": None, "br3_pre": None,
         "br1_post": "r1", "br2_post": None, "br3_post": None}])


def build_ml_config(tmp: Path) -> Path:
    """configs/ml.yaml with thresholds a two-day fixture can meet, and a small
    hand-set model for breakout_candidates to score with (so these tests don't
    depend on the committed, trained artifact). GBDP_ML_CONFIG must point at
    tmp/ml.yaml."""
    import yaml

    from gbdp.ml import breakout, model
    from gbdp.ml.config import parse

    raw = yaml.safe_load(Path("configs/ml.yaml").read_text(encoding="utf-8"))
    raw["breakout"].update(min_baseline_pa=1, min_label_pa=1)
    raw["model"]["artifact"] = str(tmp / "model.json")
    path = tmp / "ml.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    n = len(breakout.MODEL_INPUTS)
    art = model.build_artifact(parse(raw), [0.0] * n, [1.0] * n, -1.0, [-1.0] + [0.0] * (n - 1), 1.0,
                               {"note": "test fixture"}, {})
    model.write_artifact(tmp / "model.json", art)
    return path


def build_gold(tmp: Path) -> Path:
    """Silver -> identity -> gold for both days. Env vars must point at tmp/silver and tmp/gold."""
    from gbdp.gold.publish import publish_gold
    from gbdp.identity.resolver import resolve_identity

    silver, gold = tmp / "silver", tmp / "gold"
    gold.mkdir(parents=True, exist_ok=True)
    build_ml_config(tmp)
    build_silver(silver)
    resolve_identity(DAY1.isoformat(), DAY2.isoformat(), root=gold, force=True)
    publish_gold(DAY1.isoformat(), DAY2.isoformat(), root=gold, force=True)
    build_insights(tmp, gold)
    return gold


def build_insights(tmp: Path, gold: Path) -> None:
    """insight_candidates as of DAY2, with thresholds low enough for two days
    of fixture data (configs/insights.yaml needs a real season)."""
    import os

    import yaml

    from gbdp.insights.publish import insights_config, publish_insights
    from gbdp.insights.statcast_features import publish_statcast_features

    publish_statcast_features(DAY1.isoformat(), DAY2.isoformat(), root=gold, force=True)
    cfg = insights_config(Path(__file__).resolve().parent.parent / "configs" / "insights.yaml")
    cfg["hitting_streak"]["min_games"] = 1
    cfg["change_vs_baseline"].update(min_recent_pa=1, min_baseline_pa=1, min_abs_z=0)
    cfg["league_month_shift"].update(min_month_pa=1, min_abs_z=0)
    cfg["cross_league_gap"].update(min_season_pa=1, min_abs_z=0)
    path = tmp / "insights.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    saved = os.environ.get("GBDP_INSIGHTS_CONFIG")
    os.environ["GBDP_INSIGHTS_CONFIG"] = str(path)
    try:
        publish_insights(DAY2.isoformat(), DAY2.isoformat(), root=gold, force=True)
    finally:
        if saved is None:
            os.environ.pop("GBDP_INSIGHTS_CONFIG", None)
        else:
            os.environ["GBDP_INSIGHTS_CONFIG"] = saved
