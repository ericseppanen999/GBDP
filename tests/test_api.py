"""Exercises the real FastAPI app against a small parquet gold layer."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from gbdp.serve import db
from gbdp.serve.api import app

MLB = "018f1e98-0f71-7c10-bf86-8f0c1b6b2a01"


def _write(root, table, dt, rows):
    part = root / table / f"dt={dt}"
    part.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{**r, "dt": dt} for r in rows]), part / "part-00001.parquet")


@pytest.fixture
def client(tmp_path, monkeypatch):
    gold = tmp_path / "gold"
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(gold))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    monkeypatch.delenv("GBDP_API_TOKEN", raising=False)
    db._CACHE.clear()
    _write(gold, "dim_league", "2025-04-02", [{"league_id": MLB, "league_code": "MLB"}])
    player = {"league_id": MLB, "player_id": "p1", "primary_name": "Shohei Ohtani"}
    _write(gold, "dim_player", "2025-04-01", [{**player, "is_current": True}])
    _write(
        gold,
        "dim_player",
        "2025-04-02",
        [
            {**player, "is_current": True},
            {**player, "player_id": "p2", "primary_name": "Travis d'Arnaud", "is_current": True},
            {**player, "player_id": "p3", "primary_name": "Old Name", "is_current": False},
        ],
    )
    _write(gold, "fact_game", "2025-04-01", [{"game_id": "g1", "league_id": MLB}])
    _write(gold, "fact_game", "2025-04-02", [{"game_id": "g2", "league_id": MLB}])
    _write(gold, "fact_boxscore_batting", "2025-04-01", [{"player_id": "p1", "h": 1}])
    _write(gold, "fact_boxscore_batting", "2025-04-02", [{"player_id": "p1", "h": 2}])
    (tmp_path / "secret.txt").write_text("hunter2", encoding="utf-8")
    return TestClient(app)


def test_endpoints_default_to_latest_snapshot(client):
    assert len(client.get("/leagues").json()) == 1
    assert [g["game_id"] for g in client.get("/games").json()] == ["g2"]


def test_as_of_selects_a_snapshot(client):
    assert [g["game_id"] for g in client.get("/games?as_of=2025-04-01").json()] == ["g1"]
    assert client.get("/games?as_of=2020-01-01").json() == []


def test_games_date_selects_that_day(client):
    assert [g["game_id"] for g in client.get("/games?date=2025-04-01").json()] == ["g1"]


def test_players_search_excludes_closed_versions(client):
    assert [p["player_id"] for p in client.get("/players?query=OHTANI&league=MLB").json()] == ["p1"]
    assert client.get("/players?query=old").json() == []


def test_player_search_treats_input_as_data(client):
    assert [p["player_id"] for p in client.get("/players", params={"query": "d'arnaud"}).json()] == ["p2"]
    assert client.get("/players", params={"query": "x' OR '1'='1"}).json() == []
    assert client.get("/players", params={"query": "%"}).json() == []


def test_as_of_must_be_a_date(client):
    assert client.get("/leagues", params={"as_of": "../../secret"}).status_code == 422


def test_unknown_league_is_404(client):
    assert client.get("/players?league=XYZ").status_code == 404


def test_game_by_id_searches_all_snapshots(client):
    assert client.get("/games/g1").json()[0]["game_id"] == "g1"
    assert client.get("/games/nope").status_code == 404


def test_player_stats_returns_box_score_lines(client):
    assert sorted(r["h"] for r in client.get("/players/p1/stats").json()) == [1, 2]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM read_text('../secret.txt')",
        "SELECT * FROM read_csv('../secret.txt')",
        "SELECT * FROM glob('*')",
        "SELECT * FROM parquet.`/etc/passwd`",
    ],
)
def test_query_blocks_file_reads_in_the_text_guard(client, sql):
    response = client.post("/query", json={"sql": sql})
    assert response.status_code == 400
    assert "hunter2" not in response.text


def test_duckdb_connection_cannot_read_outside_gold(client, tmp_path):
    # Skips the text guard: DuckDB also reads a bare string as a table.
    path = str(tmp_path / "secret.txt").replace("\\", "/")
    response = client.post("/query", json={"sql": f"SELECT * FROM '{path}'"})
    assert response.status_code == 400
    assert "hunter2" not in response.text


def test_query_still_reads_gold_and_allows_replace_function(client):
    assert client.post("/query", json={"sql": "SELECT count(*) n FROM fact_game"}).json() == [{"n": 2}]
    assert client.post("/query", json={"sql": "SELECT replace('a', 'a', 'b') x"}).json() == [{"x": "b"}]


def test_query_token_is_enforced_when_set(client, monkeypatch):
    monkeypatch.setenv("GBDP_API_TOKEN", "s3cret")
    body = {"sql": "SELECT 1 AS x"}
    assert client.post("/query", json=body).status_code == 401
    assert client.post("/query", json=body, headers={"Authorization": "Bearer nope"}).status_code == 401
    assert client.post("/query", json=body, headers={"Authorization": "Bearer s3cret"}).status_code == 200
