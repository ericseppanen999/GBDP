"""Every metric in configs/semantic_layer.yaml, run against fixture gold.

The gold comes from the real identity and gold code (tests/semantic_fixture.py,
whose docstring has the hand-worked numbers used here).
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from gbdp.semantic import layer as semantic
from gbdp.semantic.compile import Filter, MetricRequest, compile_request
from gbdp.semantic.layer import SemanticLayerError
from gbdp.semantic.render import databricks_target, duckdb_target
from gbdp.serve import db

import semantic_fixture as fx

D1, D2 = fx.DAY1.isoformat(), fx.DAY2.isoformat()


def run(metrics, dimensions=(), filters=(), **kw):
    req = MetricRequest(metrics=list(metrics), dimensions=list(dimensions),
                        filters=[Filter(*f) for f in filters], **kw)
    return db.compute_metrics(req)["rows"]


def one(metrics, filters=(), **kw):
    rows = run(metrics, filters=filters, **kw)
    assert len(rows) == 1, rows
    return rows[0]


SL = semantic.load()
BY_VIEW = {mv: [m for m, x in SL.metrics.items() if x.metric_view == mv] for mv in SL.metric_views}


@pytest.mark.parametrize("metric_view", sorted(SL.metric_views))
def test_every_metric_runs_by_every_dimension(semantic_gold, metric_view):
    metrics = BY_VIEW[metric_view]
    assert metrics, f"{metric_view} has no metrics"
    total = run(metrics)
    assert len(total) == 1 and set(total[0]) == set(metrics)
    for dim in SL.metric_views[metric_view].dimensions:
        rows = run(metrics, [dim.name])
        assert rows, f"{metric_view} by {dim.name} returned nothing"


def test_every_metric_is_covered_by_a_check():
    # Each metric has a hand-checked value somewhere in this file.
    import inspect
    import sys

    src = inspect.getsource(sys.modules[__name__])
    missing = [m for m in SL.metrics if f'"{m}"' not in src]
    assert not missing, f"no hand-checked value for: {missing}"


# --- batting, from plate appearances ------------------------------------------

BATTER_1 = [("batter_name", "eq", "Aaron Batter")]


def test_batting_line_by_hand(semantic_gold):
    row = one(["pa", "ab", "hits", "doubles", "triples", "home_runs", "walks", "hit_by_pitch", "strikeouts",
               "total_bases", "avg", "obp", "slg", "ops", "bb_pct", "k_pct", "sac_flies", "sac_bunts"], BATTER_1)
    assert (row["pa"], row["ab"], row["hits"], row["doubles"], row["triples"], row["home_runs"]) == (6, 5, 3, 1, 0, 1)
    assert (row["walks"], row["hit_by_pitch"], row["strikeouts"], row["total_bases"]) == (1, 0, 1, 7)
    assert (row["sac_flies"], row["sac_bunts"]) == (0, 0)  # SF/SH by hand: test_semantic_views
    assert row["avg"] == pytest.approx(0.600)
    assert row["obp"] == pytest.approx(4 / 6)
    assert row["slg"] == pytest.approx(1.400)
    assert row["ops"] == pytest.approx(4 / 6 + 1.4)
    assert row["bb_pct"] == pytest.approx(1 / 6) and row["k_pct"] == pytest.approx(1 / 6)


def test_truncated_rows_are_not_plate_appearances_but_carry_runs(semantic_gold):
    # Batter 1 was up for 7 rows; the 7th is TRUNCATED (caught stealing home
    # after a wild pitch scored a run). It is not a PA, but its run counts.
    by_event = {r["event_type"]: r for r in run(["pa", "runs_on_play", "re24"], ["event_type"], BATTER_1)}
    assert by_event["TRUNCATED"]["pa"] == 0
    assert by_event["TRUNCATED"]["runs_on_play"] == 1
    assert by_event["TRUNCATED"]["re24"] == pytest.approx(0.0)  # 1 run, from a 1.0 state to the 3rd out
    row = one(["pa", "k_pct", "bb_pct", "avg", "runs_on_play", "re24"], BATTER_1)
    assert row["pa"] == 6  # 7 rows
    assert row["k_pct"] == pytest.approx(1 / 6)  # would be 1/7 if TRUNCATED counted
    assert row["runs_on_play"] == 3  # 2 (homer) + 1 (on the TRUNCATED row)
    # NPB too: 6 rows in the half-inning, the caught stealing is TRUNCATED.
    npb = one(["pa", "ab", "hits", "home_runs", "walks", "runs_on_play"], [("league_code", "eq", "NPB")])
    assert (npb["pa"], npb["ab"], npb["hits"], npb["home_runs"], npb["walks"], npb["runs_on_play"]) == (5, 4, 2, 1, 1, 2)


def test_re24_sums_to_runs_minus_starting_expectancy(semantic_gold):
    # 3 runs scored in the bottom 1st from a 2.0-run start: +1.0.
    assert one(["re24"], BATTER_1)["re24"] == pytest.approx(1.0)
    by_event = {r["event_type"]: r["re24"] for r in run(["re24"], ["event_type"], BATTER_1)}
    assert by_event["1B"] == pytest.approx(1.0)  # 2.0 -> 3.0
    assert by_event["HR"] == pytest.approx(1.0)  # 3.0 -> 2.0, +2 runs
    assert by_event["K"] == pytest.approx(-1.0)  # 2.0 -> 1.0


def test_batting_dimensions_player_team_league_season_dates(semantic_gold):
    rows = {r["team_name"]: r for r in run(["pa"], ["team_name"], [("league_code", "eq", "MLB"),
                                                                     ("data_source", "eq", "mlb_statcast")])}
    assert rows["New York Yankees"]["pa"] == 6 and rows["Boston Red Sox"]["pa"] == 1
    assert one(["pa"], [("season_year", "eq", 2026), ("league_code", "eq", "MLB"),
                         ("data_source", "eq", "mlb_statcast")])["pa"] == 7
    assert run(["pa"], filters=[("season_year", "eq", 2025)])[0]["pa"] is None
    assert one(["pa"], start=D2, end=D2)["pa"] == 5  # only NPB played on day 2
    assert one(["pa"], BATTER_1, start=D2)["pa"] is None
    assert {r["batter_name"] for r in run(["pa"], ["batter_name"], [("pitcher_name", "eq", "Pat Pitcher")])} == {"Aaron Batter"}


def test_filter_values_are_data_not_sql(semantic_gold):
    assert one(["pa"], [("batter_name", "eq", "x' OR '1'='1")])["pa"] is None
    assert one(["pa"], [("batter_name", "in", ["Aaron Batter", "Bo Hitter"])])["pa"] == 7


# --- batting, from box scores ------------------------------------------------------

def test_box_score_batting(semantic_gold):
    row = one(["rbi", "batter_runs", "box_games", "box_avg"], [("player_name", "eq", "Aaron Batter")])
    assert (row["rbi"], row["batter_runs"], row["box_games"]) == (4, 3, 2)
    assert row["box_avg"] == pytest.approx(4 / 9)  # 3-for-5, then 1-for-4
    kbo = one(["box_avg", "rbi"], [("league_code", "eq", "KBO")])
    assert (kbo["box_avg"], kbo["rbi"]) == (0.5, 1)


# --- pitching ------------------------------------------------------------------------

def test_ip_strings_add_as_thirds(semantic_gold):
    # "6.2" + "0.1" = 7.0 innings (20 + 1 outs), not 6.3.
    row = one(["outs_recorded", "ip", "ip_display", "earned_runs", "era", "whip", "k_per_9", "bb_per_9"],
              [("team_name", "eq", "Boston Red Sox")], start=D1, end=D1)
    assert (row["outs_recorded"], row["ip_display"]) == (21, "7.0")
    assert row["ip"] == pytest.approx(7.0)
    assert row["era"] == pytest.approx(9 * 2 / 7)
    assert row["whip"] == pytest.approx((5 + 1) / 7)
    assert row["k_per_9"] == pytest.approx(9 * 8 / 7) and row["bb_per_9"] == pytest.approx(9 / 7)
    # kbo_api writes "5 2/3" and "1/3": 6.0 innings.
    kbo = one(["outs_recorded", "ip_display", "era"], [("league_code", "eq", "KBO")])
    assert (kbo["outs_recorded"], kbo["ip_display"]) == (18, "6.0") and kbo["era"] == pytest.approx(1.5)


def test_pitcher_season_and_date_range(semantic_gold):
    season = one(["ip_display", "games_pitched", "era", "whip", "hits_allowed", "pitcher_strikeouts",
                  "pitcher_walks"], [("pitcher_name", "eq", "Pat Pitcher")])
    # "6.2" + "5.1" = 12.0 innings, 5 earned runs.
    assert (season["ip_display"], season["games_pitched"]) == ("12.0", 2)
    assert season["era"] == pytest.approx(3.75) and season["whip"] == pytest.approx(14 / 12)
    assert (season["hits_allowed"], season["pitcher_strikeouts"], season["pitcher_walks"]) == (11, 11, 3)
    day1 = one(["ip_display", "era"], [("pitcher_name", "eq", "Pat Pitcher")], start=D1, end=D1)
    assert day1["ip_display"] == "6.2" and day1["era"] == pytest.approx(9 * 2 / (20 / 3))


def test_season_list_player_is_a_dimension_row(semantic_gold):
    # Pitcher 5 is on the season player list but no active roster.
    assert one(["ip_display"], [("pitcher_name", "eq", "Rex Reliever")])["ip_display"] == "0.1"


# --- teams -----------------------------------------------------------------------------

def test_team_records_count_each_game_once(semantic_gold):
    rows = {r["team_abbrev"]: r for r in run(
        ["games_played", "wins", "losses", "ties", "win_pct", "runs_scored", "runs_allowed", "run_differential"],
        ["team_abbrev"], [("league_code", "eq", "MLB")])}
    # Game 1001 is listed on both days and 1003 was postponed: 2 games each.
    nyy, bos = rows["NYY"], rows["BOS"]
    assert (nyy["games_played"], nyy["wins"], nyy["losses"], nyy["win_pct"]) == (2, 2, 0, 1.0)
    assert (nyy["runs_scored"], nyy["runs_allowed"], nyy["run_differential"]) == (8, 4, 4)
    assert (bos["wins"], bos["losses"], bos["run_differential"]) == (0, 2, -4)
    # 1001 counts on the day it was first final.
    assert one(["wins"], [("team_abbrev", "eq", "NYY")], start=D1, end=D1)["wins"] == 1
    # NPB regular season: the 2-2 interleague tie and a 1-0 Pacific League
    # game; the 試合中止 (called off) game never counts.
    npb = run(["games_played", "ties", "win_pct"], ["team_name"], [("league_code", "eq", "NPB")])
    assert sorted((r["games_played"], r["ties"], r["win_pct"]) for r in npb) == [(2, 1, 0.0), (2, 1, 1.0)]


def test_team_records_default_to_the_regular_season(semantic_gold):
    # The All-Star game (MLB A) and NPB's Climax Series and unmapped game type
    # are left out by default, and each can be asked for by season_phase.
    phases = {(r["league_code"], r["season_phase"]): r["games_played"] for r in run(
        ["games_played"], ["league_code", "season_phase"],
        [("season_phase", "in", ["regular", "postseason", "allstar", "preseason", "exhibition", "other", "unknown"])])}
    assert phases == {("MLB", "regular"): 4, ("MLB", "allstar"): 2, ("NPB", "regular"): 4,
                      ("NPB", "postseason"): 4, ("NPB", "other"): 2, ("KBO", "unknown"): 2}
    by_default = {r["league_code"]: r["games_played"] for r in run(["games_played"], ["league_code"])}
    assert by_default == {"MLB": 4, "NPB": 4, "KBO": 2}  # regular, plus unknown (KBO has no game type)
    allstar = one(["wins", "runs_scored"], [("team_abbrev", "eq", "NYY"), ("season_phase", "eq", "allstar")])
    assert (allstar["wins"], allstar["runs_scored"]) == (1, 9)
    types = {(r["game_type"], r["season_phase"]) for r in run(
        ["games_played"], ["game_type", "season_phase"], [("season_phase", "ne", "nothing")])}
    assert types == {("R", "regular"), ("A", "allstar"), ("セ・パ交流戦", "regular"), ("パ・リーグ公式戦", "regular"),
                     ("クライマックスシリーズ", "postseason"), ("日本シリーズ", "postseason"),
                     ("ファーム交流戦", "other"), (None, "unknown")}
    # The called-off game (試合中止, a セ・リーグ公式戦) is in no phase at all.
    assert ("セ・リーグ公式戦", "regular") not in types
    # The default is listed with the metric.
    assert SL.metric_summary("wins")["default_filters"] == [
        {"dimension": "season_phase", "op": "in", "value": ["regular", "unknown"]}]


def test_season_phase_rules_cover_npb_league_games():
    from gbdp.semantic.render import duckdb_target, render
    import duckdb

    case = render("{{season_phase:t:s}}", SL, duckdb_target())
    con = duckdb.connect()
    got = {t: con.execute(f"SELECT {case} FROM (SELECT ? AS t, ? AS s)", [t, src]).fetchone()[0]
           for t, src in [("セ・リーグ公式戦", "npb_spaia"), ("パ・リーグ公式戦", "npb_spaia"), ("日本シリーズ", "npb_spaia"),
                          ("W", "mlb_statsapi"), ("F", "mlb_statsapi"), ("S", "mlb_statsapi"),
                          ("SMBC日本シリーズ2026", "npb_spaia"), ("マイナビオールスターゲーム", "npb_spaia"),
                          ("オープン戦", "npb_spaia"), ("R", "npb_spaia"), ("Z", "mlb_statsapi")]}
    assert got == {"セ・リーグ公式戦": "regular", "パ・リーグ公式戦": "regular", "日本シリーズ": "postseason",
                   "W": "postseason", "F": "postseason", "S": "preseason", "R": "other", "Z": "other",
                   "SMBC日本シリーズ2026": "postseason", "マイナビオールスターゲーム": "allstar", "オープン戦": "preseason"}


# --- pitches ---------------------------------------------------------------------------

def test_velocity_by_pitch_type_and_whiff_rate(semantic_gold):
    by_type = {r["pitch_type_name"]: r for r in run(
        ["pitches", "avg_velocity", "max_velocity", "swings", "whiffs", "whiff_rate"], ["pitch_type_name"],
        [("pitcher_name", "eq", "Pat Pitcher")])}
    ff, sl = by_type["4-seam fastball"], by_type["Slider"]
    assert ff["pitches"] == 9 and ff["avg_velocity"] == pytest.approx(857 / 9) and ff["max_velocity"] == 97.0
    assert sl["pitches"] == 4 and sl["avg_velocity"] == pytest.approx(85.0)
    total = one(["swings", "whiffs", "whiff_rate"], [("pitcher_name", "eq", "Pat Pitcher")])
    # 4 balls in play + swinging_strike + foul + swinging_strike_blocked; 2 missed.
    assert (total["swings"], total["whiffs"]) == (7, 2) and total["whiff_rate"] == pytest.approx(2 / 7)
    assert one(["pitches"], [("team_name", "eq", "Boston Red Sox")])["pitches"] == 14


# --- run expectancy --------------------------------------------------------------------

def test_run_expectancy_matrix_over_a_date_range(semantic_gold):
    rows = {(r["league_code"], r["runners"], r["outs"]): r for r in run(
        ["exp_runs", "re_pa_count"], ["league_code", "runners", "outs"])}
    assert rows[("MLB", "Bases empty", 0)]["exp_runs"] == pytest.approx(2.0)  # PAs 1 and 3: 3 and 1 runs
    assert rows[("MLB", "Bases empty", 0)]["re_pa_count"] == 2
    assert rows[("MLB", "2nd & 3rd", 2)]["exp_runs"] == pytest.approx(1.0)
    assert ("NPB", "Bases empty", 0) not in {k for k in rows if rows[k]["re_pa_count"] is None}
    assert run(["re_pa_count"], ["league_code"], start=D2, end=D2) == [{"league_code": "NPB", "re_pa_count": 6}]


# --- requests the layer refuses ---------------------------------------------------------

def test_bad_requests_are_refused(semantic_gold):
    with pytest.raises(SemanticLayerError, match="different metric views"):
        run(["avg", "era"])
    with pytest.raises(SemanticLayerError, match="no dimension"):
        run(["avg"], ["pitch_type"])
    with pytest.raises(SemanticLayerError, match="unknown metric"):
        run(["batting_average"])
    with pytest.raises(SemanticLayerError, match="not a valid int"):
        run(["avg"], filters=[("season_year", "eq", "2026; DROP TABLE x")])
    with pytest.raises(SemanticLayerError, match="unknown filter op"):
        run(["avg"], filters=[("league_code", "like", "M%")])
    with pytest.raises(SemanticLayerError, match="isn't an output column"):
        run(["avg"], order_by=[("1; DROP", "desc")])


def test_having_order_and_limit(semantic_gold):
    rows = run(["pa", "home_runs"], ["batter_name"], having=[("pa", "gte", 2)],
               order_by=[("home_runs", "desc"), ("pa", "desc")], limit=5)
    assert [r["pa"] for r in rows] == [6, 5]


# --- the same metric SQL for Databricks ----------------------------------------------

def test_databricks_compile_inlines_views_and_uses_uc_names():
    q = compile_request(SL, MetricRequest(metrics=["box_avg"], dimensions=["team_name"],
                                          filters=[Filter("league_code", "eq", "KBO")]),
                        databricks_target(inline_views=True), param_style="literal")
    assert q.sql.startswith("WITH ") and "v_batting_line AS (" in q.sql
    assert "gbdp.gold_gbdp.fact_boxscore_batting" in q.sql and "b.c_2b AS doubles" in q.sql
    assert "source.league_code = 'KBO'" in q.sql and "{{" not in q.sql


def test_inline_compile_runs_on_duckdb_too(semantic_gold):
    # The dashboard's form (views as CTEs over the bare tables) gives the same answer.
    import dataclasses

    target = dataclasses.replace(duckdb_target(), inline_views=True)
    q = compile_request(SL, MetricRequest(metrics=["avg", "re24"], filters=[Filter("batter_name", "eq", "Aaron Batter")]),
                        target, param_style="literal")
    con = db._duckdb_connection()
    assert con.execute(q.sql).fetchall() == [(pytest.approx(0.6), pytest.approx(1.0))]


# --- API and MCP -----------------------------------------------------------------

def test_api_lists_describes_and_computes_metrics(semantic_gold):
    from gbdp.serve.api import app

    client = TestClient(app)
    names = {m["name"] for m in client.get("/metrics").json()}
    assert {"avg", "obp", "slg", "ops", "pa", "home_runs", "bb_pct", "k_pct", "ip", "era", "whip", "k_per_9",
            "bb_per_9", "runs_scored", "runs_allowed", "wins", "run_differential", "avg_velocity", "whiff_rate",
            "re24"} <= names
    detail = client.get("/metrics/era").json()
    assert detail["base_tables"] == ["fact_boxscore_pitching", "dim_player", "dim_team"]
    assert client.get("/metrics/nope").status_code == 404
    body = {"metrics": ["avg", "pa"], "dimensions": ["batter_name"],
            "filters": [{"dimension": "league_code", "op": "eq", "value": "MLB"},
                        {"dimension": "data_source", "value": "mlb_statcast"}],
            "start": D1, "end": D1, "order_by": [{"column": "pa", "direction": "desc"}]}
    out = client.post("/metrics/query", json=body).json()
    assert out["rows"][0] == {"batter_name": "Aaron Batter", "avg": pytest.approx(0.6), "pa": 6}
    assert "FROM v_plate_appearance AS source" in out["sql"]
    assert client.post("/metrics/query", json={"metrics": ["avg", "era"]}).status_code == 400
    assert client.post("/metrics/query", json={"metrics": []}).status_code == 422
    # /query still refuses writes, and can read the semantic views.
    assert client.post("/query", json={"sql": "DROP VIEW v_plate_appearance"}).status_code == 400
    rows = client.post("/query", json={"sql": "SELECT count(*) AS n FROM v_team_game"}).json()
    assert rows == [{"n": 18}]  # 9 finished games (3 MLB, 5 NPB, 1 KBO), two sides each


def test_metric_query_takes_the_api_token_and_hides_internal_errors(semantic_gold, monkeypatch):
    from gbdp.serve import api
    from gbdp.serve.api import app

    client = TestClient(app)
    body = {"metrics": ["pa"]}
    monkeypatch.setenv("GBDP_API_TOKEN", "s3cret")
    assert client.post("/metrics/query", json=body).status_code == 401
    assert client.post("/metrics/query", json=body, headers={"Authorization": "Bearer s3cret"}).status_code == 200
    monkeypatch.delenv("GBDP_API_TOKEN")
    too_many = {"metrics": ["pa"],
                "filters": [{"dimension": "batter_id", "op": "in", "value": [str(i) for i in range(1001)]}]}
    r = client.post("/metrics/query", json=too_many)
    assert r.status_code == 400 and "limit is 1000" in r.json()["detail"]

    def boom(request):
        raise RuntimeError("IO Error: C:/secret/path/gold not found")

    monkeypatch.setattr(api, "compute_metrics", boom)
    r = client.post("/metrics/query", json=body)
    assert r.status_code == 500 and "secret" not in r.text


def test_mcp_compute_metric(semantic_gold):
    from gbdp.mcp_server import compute_metric

    out = compute_metric(["ip_display", "era"], ["team_name"], [{"dimension": "team_name", "value": "Boston Red Sox"}],
                         start=D1, end=D1)
    assert out["rows"] == [{"team_name": "Boston Red Sox", "ip_display": "7.0", "era": pytest.approx(18 / 7)}]
    assert "error" in compute_metric(["avg", "era"])
    assert "error" in compute_metric(["avg"], filters=[{"op": "eq"}])


def test_season_phase_rules_cover_retrosheet_and_kbo_codes():
    # SL-019: gold writes Retrosheet's gametype as statsapi codes (plus P for
    # its pre-1950 championship series) and kbo_api's series as R/E/A/P.
    from gbdp.gold import publish
    from gbdp.semantic.render import duckdb_target, render
    import duckdb

    case = render("{{season_phase:t:s}}", SL, duckdb_target())
    con = duckdb.connect()

    def phase(t, src):
        return con.execute(f"SELECT {case} FROM (SELECT ? AS t, ? AS s)", [t, src]).fetchone()[0]

    retro = {"R": "regular", "E": "exhibition", "A": "allstar", "F": "postseason", "D": "postseason",
             "L": "postseason", "W": "postseason", "P": "postseason"}
    assert {t: phase(t, "retrosheet_local") for t in retro} == retro
    kbo = {"R": "regular", "E": "exhibition", "A": "allstar", "P": "postseason"}
    assert {t: phase(t, "kbo_api") for t in kbo} == kbo
    # Every code gold writes for these sources is covered.
    from gbdp.silver import retrosheet

    assert set(publish._KBO_SERIES.values()) <= set(kbo)
    assert set(retrosheet._RETRO_GAME_TYPES.values()) <= set(retro)
    # P is not an MLB statsapi code.
    assert phase("P", "mlb_statsapi") == "other"


def test_season_phase_maps_every_confirmed_npb_game_type():
    # The distinct gameTypeName values in silver npb_spaia/games, 2026-05-13
    # to 11-05, exactly as fact_game.game_type carries them (the Climax Series
    # names have U+3000 ideographic spaces), with their distinct game counts.
    from gbdp.semantic.render import duckdb_target, render
    import duckdb

    games = {
        "セ・リーグ公式戦": (285, "regular"),
        "パ・リーグ公式戦": (272, "regular"),
        "セ・パ交流戦": (112, "regular"),  # interleague is regular season
        "オールスターゲーム": (2, "allstar"),
        "セ・リーグ　クライマックスシリーズ　ファーストステージ": (2, "postseason"),
        "セ・リーグ　クライマックスシリーズ　ファイナルステージ": (4, "postseason"),
        "パ・リーグ　クライマックスシリーズ　ファーストステージ": (3, "postseason"),
        "パ・リーグ　クライマックスシリーズ　ファイナルステージ": (3, "postseason"),
        "日本シリーズ": (4, "postseason"),
    }
    case = render("{{season_phase:t:s}}", SL, duckdb_target())
    con = duckdb.connect()
    totals: dict = {}
    for name, (n, expected) in games.items():
        got = con.execute(f"SELECT {case} FROM (SELECT ? AS t, 'npb_spaia' AS s)", [name]).fetchone()[0]
        assert got == expected, name
        totals[got] = totals.get(got, 0) + n
    assert totals == {"regular": 669, "allstar": 2, "postseason": 16}
