"""Cloud mode: the FastAPI app on the Databricks backend, with the HTTP layer mocked.

No test here touches the network. A fake `requests.request` stands in for the
workspace; every statement the API sends is recorded and checked.
"""
from __future__ import annotations

import json as jsonlib

import pytest
import requests
from fastapi.testclient import TestClient

from gbdp.serve import db, warehouse as wh
from gbdp.serve.api import app

MLB = "018f1e98-0f71-7c10-bf86-8f0c1b6b2a01"
HOST = "https://example.cloud.databricks.com"


def _col(c):
    name, type_name = c if isinstance(c, tuple) else (c, "STRING")
    return {"name": name, "type_name": type_name}


def _ok(columns, rows, **extra):
    return {
        "statement_id": "st-1",
        "status": {"state": "SUCCEEDED"},
        "manifest": {"schema": {"columns": [_col(c) for c in columns]}},
        "result": {"data_array": rows, **extra},
    }


class FakeWorkspace:
    """Records each request and answers from a queue (empty result by default)."""

    def __init__(self):
        self.calls = []
        self.responses = []

    def __call__(self, method, url, json=None, headers=None, timeout=None):
        self.calls.append({"method": method, "url": url, "json": json, "headers": headers})
        r = self.responses.pop(0) if self.responses else _ok([], [])
        if isinstance(r, requests.Response):
            return r
        resp = requests.Response()
        resp.status_code = 200
        resp._content = jsonlib.dumps(r).encode()
        return resp

    @property
    def statements(self):
        return [c for c in self.calls if c["method"] == "POST" and c["url"].endswith("/api/2.0/sql/statements")]

    @property
    def last(self):
        return self.statements[-1]["json"]


@pytest.fixture
def ws(monkeypatch):
    fake = FakeWorkspace()
    monkeypatch.setattr(requests, "request", fake)
    monkeypatch.setenv("GBDP_SERVE_BACKEND", "databricks")
    monkeypatch.setenv("DATABRICKS_HOST", HOST + "/")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
    for name in ("GBDP_WAREHOUSE_ID", "GBDP_SERVE_CATALOG", "GBDP_SERVE_SCHEMA", "GBDP_SERVE_UC_MODE",
                 "GBDP_API_TOKEN", "GBDP_SERVE_TIMEOUT_S"):
        monkeypatch.delenv(name, raising=False)
    db._CACHE.clear()
    db.set_warehouse(None)
    yield fake
    db.set_warehouse(None)
    db._CACHE.clear()


@pytest.fixture
def client(ws):
    return TestClient(app)


def _params(body):
    return {p["name"]: (p["value"], p["type"]) for p in body.get("parameters", [])}


# ---- transport and settings -------------------------------------------------

def test_request_goes_to_the_statement_api_with_a_bearer_token(ws, client):
    ws.responses.append(_ok(["league_id", "league_code"], [[MLB, "MLB"]]))
    assert client.get("/leagues").json() == [{"league_id": MLB, "league_code": "MLB"}]
    call = ws.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == HOST + "/api/2.0/sql/statements"
    assert call["headers"] == {"Authorization": "Bearer dapi-test-token"}
    body = call["json"]
    assert body["warehouse_id"] == "a9d3d2589a30ab67"
    assert (body["catalog"], body["schema"]) == ("gbdp", "gold_gbdp")
    assert body["row_limit"] == 1000
    assert "dapi-test-token" not in str(body)


def test_warehouse_catalog_and_schema_are_configurable(ws, client, monkeypatch):
    monkeypatch.setenv("GBDP_WAREHOUSE_ID", "w-other")
    monkeypatch.setenv("GBDP_SERVE_CATALOG", "scratch")
    monkeypatch.setenv("GBDP_SERVE_SCHEMA", "gold_test")
    client.get("/leagues")
    assert ws.last["warehouse_id"] == "w-other"
    assert (ws.last["catalog"], ws.last["schema"]) == ("scratch", "gold_test")
    assert "`scratch`.`gold_test`.`dim_league`" in ws.last["statement"]


def test_missing_credentials_is_a_clear_503(ws, client, monkeypatch):
    monkeypatch.delenv("DATABRICKS_TOKEN")
    r = client.get("/leagues")
    assert r.status_code == 503
    assert "DATABRICKS_TOKEN" in r.json()["detail"]
    assert ws.calls == []


def test_bad_catalog_name_is_refused(ws, client, monkeypatch):
    monkeypatch.setenv("GBDP_SERVE_CATALOG", "gbdp`; DROP")
    assert client.get("/leagues").status_code == 503
    assert ws.calls == []


def test_unknown_backend_is_refused(ws, client, monkeypatch):
    monkeypatch.setenv("GBDP_SERVE_BACKEND", "oracle")
    assert client.get("/leagues").status_code == 503


def test_health_needs_no_warehouse(ws, client):
    assert client.get("/health").json() == {"status": "ok"}
    assert ws.calls == []


def test_http_error_becomes_a_502_without_the_token(ws, client):
    bad = requests.Response()
    bad.status_code = 403
    bad._content = b'{"error_code": "PERMISSION_DENIED", "message": "no access to warehouse"}'
    ws.responses.append(bad)
    r = client.get("/leagues")
    assert r.status_code == 502
    assert "no access to warehouse" in r.json()["detail"]
    assert "dapi-test-token" not in r.text


def test_network_failure_is_a_502(ws, client, monkeypatch):
    def boom(*a, **k):
        raise requests.ConnectionError("dns failure with secret details")

    monkeypatch.setattr(requests, "request", boom)
    r = client.get("/leagues")
    assert r.status_code == 502
    assert "ConnectionError" in r.json()["detail"]
    assert "secret details" not in r.text


def test_statement_errors_surface_the_warehouse_message(ws, client):
    ws.responses.append({"statement_id": "s", "status": {"state": "FAILED", "error": {
        "error_code": "BAD_REQUEST", "message": "[UNRESOLVED_COLUMN] `foo` cannot be resolved"}}})
    r = client.get("/breakout-candidates")
    assert r.status_code == 502
    assert "UNRESOLVED_COLUMN" in r.json()["detail"]


def test_missing_table_reads_as_empty_like_local(ws, client):
    ws.responses.append({"statement_id": "s", "status": {"state": "FAILED", "error": {
        "message": "[TABLE_OR_VIEW_NOT_FOUND] The table gbdp.gold_gbdp.dim_league cannot be found"}}})
    assert client.get("/leagues").json() == []


def test_slow_statement_is_polled_and_paged(ws, client, monkeypatch):
    monkeypatch.setattr(wh.time, "sleep", lambda s: None)
    ws.responses.append({"statement_id": "st-9", "status": {"state": "PENDING"}})
    ws.responses.append({"statement_id": "st-9", "status": {"state": "RUNNING"}})
    ws.responses.append(_ok(["league_code"], [["MLB"]],
                            next_chunk_internal_link="/api/2.0/sql/statements/st-9/result/chunks/1"))
    ws.responses.append({"data_array": [["NPB"]]})
    assert [r["league_code"] for r in client.get("/leagues").json()] == ["MLB", "NPB"]
    assert [(c["method"], c["url"].removeprefix(HOST)) for c in ws.calls] == [
        ("POST", "/api/2.0/sql/statements"),
        ("GET", "/api/2.0/sql/statements/st-9"),
        ("GET", "/api/2.0/sql/statements/st-9"),
        ("GET", "/api/2.0/sql/statements/st-9/result/chunks/1"),
    ]


def test_statement_that_never_finishes_is_cancelled(ws, client, monkeypatch):
    monkeypatch.setenv("GBDP_SERVE_TIMEOUT_S", "0")
    monkeypatch.setattr(wh.time, "sleep", lambda s: None)
    ws.responses.append({"statement_id": "st-slow", "status": {"state": "RUNNING"}})
    r = client.get("/leagues")
    assert r.status_code == 502 and "cancelled" in r.json()["detail"]
    assert ws.calls[-1]["url"].endswith("/api/2.0/sql/statements/st-slow/cancel")


# ---- value mapping ----------------------------------------------------------

def test_cells_are_typed_from_the_manifest(ws, client):
    ws.responses.append(_ok(
        [("game_id", "STRING"), ("home_score", "INT"), ("ops", "DOUBLE"), ("is_current", "BOOLEAN"),
         ("game_date", "DATE"), ("tags", "ARRAY"), ("note", "STRING"), ("avg", "DECIMAL")],
        [["g1", "5", "0.912", "true", "2026-06-10", '["a","b"]', None, "0.250"]],
    ))
    assert client.get("/games?date=2026-06-10").json() == [{
        "game_id": "g1", "home_score": 5, "ops": 0.912, "is_current": True,
        "game_date": "2026-06-10", "tags": ["a", "b"], "note": None, "avg": 0.25,
    }]


# ---- every read endpoint ----------------------------------------------------

def test_leagues_latest_snapshot(ws, client):
    client.get("/leagues")
    assert ws.last["statement"] == (
        "SELECT * FROM `gbdp`.`gold_gbdp`.`dim_league` "
        "WHERE `dt` = (SELECT max(`dt`) FROM `gbdp`.`gold_gbdp`.`dim_league`) LIMIT 1000")
    assert "parameters" not in ws.last


def test_leagues_as_of(ws, client):
    client.get("/leagues?as_of=2026-06-10")
    assert ws.last["statement"].endswith("WHERE `dt` = :p0 LIMIT 1000")
    assert _params(ws.last) == {"p0": ("2026-06-10", "STRING")}


def test_teams_filters(ws, client):
    client.get("/teams?league=MLB&as_of=2026-06-10")
    assert "`is_current` = :p1 AND `league_id` = :p2" in ws.last["statement"]
    assert _params(ws.last) == {"p0": ("2026-06-10", "STRING"), "p1": ("true", "BOOLEAN"), "p2": (MLB, "STRING")}


def test_players_search_is_bound_not_pasted(ws, client):
    client.get("/players", params={"query": "d'Arnaud' OR 1=1", "league": "MLB"})
    body = ws.last
    assert "Arnaud" not in body["statement"] and "OR 1=1" not in body["statement"]
    assert "locate(:p2, lower(`primary_name`)) > 0" in body["statement"]
    p = _params(body)
    assert p["p2"] == ("d'arnaud' or 1=1", "STRING")
    assert p["p0"] == ("true", "BOOLEAN")


def test_games_by_date_and_league(ws, client):
    ws.responses.append(_ok(["game_id", "league_id"], [["g1", MLB]]))
    assert client.get("/games?date=2026-06-10&league=MLB").json() == [{"game_id": "g1", "league_id": MLB}]
    assert "`dt` = :p0 AND `league_id` = :p1" in ws.last["statement"]
    assert _params(ws.last) == {"p0": ("2026-06-10", "STRING"), "p1": (MLB, "STRING")}


def test_game_by_id_searches_every_snapshot(ws, client):
    ws.responses.append(_ok(["game_id", "dt"], [["g1", "2026-06-10"]]))
    assert client.get("/games/g1").json() == [{"game_id": "g1", "dt": "2026-06-10"}]
    assert "`dt`" not in ws.last["statement"]
    assert "WHERE `game_id` = :p0" in ws.last["statement"]
    assert _params(ws.last) == {"p0": ("g1", "STRING")}


def test_game_by_id_as_of_and_404(ws, client):
    assert client.get("/games/nope?as_of=2026-06-10").status_code == 404
    assert "`dt` = :p0 AND `game_id` = :p1" in ws.last["statement"]


def test_player_stats_default_and_rolling(ws, client):
    ws.responses.append(_ok([("player_id", "STRING"), ("h", "INT")], [["p1", "2"]]))
    assert client.get("/players/p1/stats").json() == [{"player_id": "p1", "h": 2}]
    assert "`fact_boxscore_batting`" in ws.last["statement"] and "max(" not in ws.last["statement"]
    client.get("/players/p1/stats?window=rolling_30d")
    assert "`feature_player_rolling_30d` WHERE `dt` = (SELECT max(`dt`)" in ws.last["statement"]
    assert "`player_id` = :p0" in ws.last["statement"]
    client.get("/players/p1/stats?as_of=2026-06-10")
    assert "`dt` = :p0 AND `player_id` = :p1" in ws.last["statement"]


def test_run_expectancy_season_returns_last_snapshot(ws, client):
    ws.responses.append(_ok(
        [("league_id", "STRING"), ("season_year", "INT"), ("base_state", "INT"), ("outs", "INT"), ("dt", "STRING")],
        [[MLB, "2026", "0", "0", "2026-06-09"], [MLB, "2026", "0", "0", "2026-06-10"],
         [MLB, "2026", "1", "0", "2026-06-10"]],
    ))
    rows = client.get("/run-expectancy?league=MLB&season=2026").json()
    assert [r["dt"] for r in rows] == ["2026-06-10", "2026-06-10"]
    assert rows[0]["season_year"] == 2026
    assert _params(ws.last) == {"p0": (MLB, "STRING"), "p1": ("2026", "BIGINT")}
    assert ws.last["row_limit"] == 100_000 and "LIMIT 100000" in ws.last["statement"]


def test_run_expectancy_without_season_reads_the_latest(ws, client):
    client.get("/run-expectancy")
    assert "max(`dt`)" in ws.last["statement"]


def test_breakout_candidates_selects_columns_by_name(ws, client):
    ws.responses.append(_ok([("player_id", "STRING"), ("hr", "LONG")], [["p1", "3"]]))
    assert client.get("/breakout-candidates").json() == [{"player_id": "p1", "hr": 3}]
    from gbdp.serve import api

    assert ws.last["statement"].startswith(
        "SELECT " + ", ".join(f"`{c}`" for c in api.BREAKOUT_COLUMNS) + " FROM `gbdp`.`gold_gbdp`.`breakout_candidates`")
    assert "SELECT *" not in ws.last["statement"]


def test_managed_column_names_map_back(ws, client):
    ws.responses.append(_ok([("player_id", "STRING"), ("c_2b", "INT")], [["p1", "1"]]))
    assert client.get("/players/p1/stats").json() == [{"player_id": "p1", "2b": 1}]


def test_responses_are_cached_by_url_and_as_of(ws, client):
    client.get("/leagues")
    client.get("/leagues")
    assert len(ws.statements) == 1
    client.get("/leagues?as_of=2026-06-10")
    client.get("/leagues?as_of=2026-06-10")
    assert len(ws.statements) == 2
    client.get("/teams")
    assert len(ws.statements) == 3


def test_schema_and_metric_catalog_need_no_warehouse(ws, client):
    assert "tables" in client.get("/schema").json()
    assert client.get("/metrics").json()
    assert client.get("/metrics/avg").status_code == 200
    assert ws.calls == []


# ---- /query -----------------------------------------------------------------

def test_query_wraps_limits_and_runs_on_the_warehouse(ws, client):
    ws.responses.append(_ok([("n", "LONG")], [["7"]]))
    r = client.post("/query", json={"sql": "SELECT count(*) AS n FROM fact_game;", "limit": 5})
    assert r.json() == [{"n": 7}]
    body = ws.last
    assert body["statement"] == "SELECT * FROM (\nSELECT count(*) AS n FROM fact_game\n) AS _gbdp_query_result LIMIT 5"
    assert body["row_limit"] == 5
    assert (body["catalog"], body["schema"]) == ("gbdp", "gold_gbdp")  # bare names resolve here


def test_query_trailing_comment_cannot_swallow_the_wrapper(ws, client):
    client.post("/query", json={"sql": "SELECT 1 -- note"})
    assert "-- note\n) AS _gbdp_query_result" in ws.last["statement"]


@pytest.mark.parametrize("sql", [
    "DROP TABLE fact_game",
    "SELECT 1; DELETE FROM fact_game",
    "INSERT INTO fact_game SELECT * FROM fact_game",
    "WITH x AS (SELECT 1) DELETE FROM fact_game",
    "SELECT * FROM read_files('/Volumes/x')",
    "SELECT * FROM parquet.`/Volumes/gbdp/x`",
    "SELECT * FROM other_catalog.other_schema.t",
    "SELECT * FROM system.access.audit",
    "SELECT * FROM gbdp.bronze_gbdp.raw",
    "SELECT * FROM information_schema.tables",
    "SET spark.sql.x = 1",
    # Two-part schema.table resolves inside the current catalog.
    "SELECT * FROM bronze_gbdp.kbo_api_games",
    "SELECT * FROM silver_gbdp.npb_spaia_rosters g JOIN fact_game f ON f.game_id = g.game_id",
    # Warehouse functions that reach outside the tables.
    "SELECT secret('gbdp', 'token')",
    "SELECT try_secret('gbdp', 'token') AS t FROM fact_game",
    "SELECT http_request(conn => 'x', method => 'GET', path => '/')",
    "SELECT ai_query('endpoint', 'hi')",
    "SELECT reflect('java.lang.System', 'getenv', 'DATABRICKS_TOKEN')",
    "SELECT * FROM list('/Volumes/gbdp')",
])
def test_query_refuses_writes_and_other_schemas(ws, client, sql):
    r = client.post("/query", json={"sql": sql})
    assert r.status_code == 400
    assert ws.calls == []


def test_query_alias_dot_column_is_still_allowed(ws, client):
    sql = "SELECT g.game_id, pa.event_type FROM fact_game g JOIN fact_plate_appearance pa ON pa.game_id = g.game_id"
    assert client.post("/query", json={"sql": sql}).status_code == 200


def test_query_may_name_the_gold_schema_explicitly(ws, client):
    assert client.post("/query", json={"sql": "SELECT * FROM gbdp.gold_gbdp.fact_game"}).status_code == 200


def test_query_inlines_views_as_ctes_with_their_dependencies(ws, client):
    sl = db.semantic_layer()
    view = "v_plate_appearance"
    client.post("/query", json={"sql": f"SELECT * FROM {view} LIMIT 3"})
    sql = ws.last["statement"]
    assert sql.startswith(f"WITH {view} AS (")
    assert "v_pitch AS (" not in sql and "v_team_game AS (" not in sql  # only the views it names
    assert "gbdp.gold_gbdp." in sql  # the view SQL reads the UC tables
    assert "{{" not in sql
    assert sql.rstrip().endswith("LIMIT 1000")


def test_query_without_views_has_no_with_prefix(ws, client):
    client.post("/query", json={"sql": "SELECT * FROM dim_team"})
    assert ws.last["statement"].startswith("SELECT * FROM (")


def test_query_failure_is_a_400_with_the_message(ws, client):
    ws.responses.append({"statement_id": "s", "status": {"state": "FAILED", "error": {"message": "syntax error near x"}}})
    r = client.post("/query", json={"sql": "SELECT x FROM"})
    assert r.status_code == 400 and "syntax error" in r.json()["detail"]


def test_query_requires_the_bearer_token_when_set(ws, client, monkeypatch):
    monkeypatch.setenv("GBDP_API_TOKEN", "s3cret")
    assert client.post("/query", json={"sql": "SELECT 1"}).status_code == 401
    assert ws.calls == []
    ok = client.post("/query", json={"sql": "SELECT 1"}, headers={"Authorization": "Bearer s3cret"})
    assert ok.status_code == 200


# ---- /metrics/query ---------------------------------------------------------

def test_metric_query_compiles_for_databricks(ws, client):
    ws.responses.append(_ok([("league_code", "STRING"), ("avg", "DOUBLE")], [["MLB", "0.248"]]))
    r = client.post("/metrics/query", json={
        "metrics": ["avg"], "dimensions": ["league_code"],
        "filters": [{"dimension": "league_code", "value": "MLB"}],
        "start": "2026-05-01", "end": "2026-05-31", "limit": 10,
    })
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["rows"] == [{"league_code": "MLB", "avg": 0.248}]
    assert out["columns"] == ["league_code", "avg"]
    body = ws.last
    assert body["statement"] == out["sql"]
    assert "gbdp.gold_gbdp." in body["statement"] and ":p0" in body["statement"]
    main = body["statement"].split("\nSELECT\n", 1)[1]  # after the inlined view CTEs
    assert "?" not in main and "'MLB'" not in main and "'2026-05-01'" not in main
    assert "catalog" not in body  # tables are fully qualified
    assert body["row_limit"] == 10
    values = [v for v, _ in _params(body).values()]
    assert "MLB" in values and "2026-05-01" in values and "2026-05-31" in values
    assert [p["name"] for p in body["parameters"]] == [f"p{i}" for i in range(len(body["parameters"]))]
    assert body["statement"].startswith("WITH ")  # views inlined as CTEs, not required in UC


def test_metric_query_unknown_metric_is_400_without_a_request(ws, client):
    r = client.post("/metrics/query", json={"metrics": ["no_such_metric"]})
    assert r.status_code == 400 and ws.calls == []


def test_metric_query_types_numeric_parameters(ws, client):
    client.post("/metrics/query", json={
        "metrics": ["avg"], "dimensions": ["batter_name"],
        "having": [{"metric": "avg", "op": "gt", "value": 0.3}],
    })
    assert ("0.3", "DOUBLE") in _params(ws.last).values()


def test_metric_query_warehouse_failure_is_a_502_with_the_message(ws, client):
    ws.responses.append({"statement_id": "s", "status": {"state": "FAILED", "error": {
        "message": "[UNRESOLVED_COLUMN] runs_to_end_of_inning cannot be resolved"}}})
    r = client.post("/metrics/query", json={"metrics": ["avg"]})
    assert r.status_code == 502 and "UNRESOLVED_COLUMN" in r.json()["detail"]
