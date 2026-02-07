from __future__ import annotations

from fastapi import FastAPI, Query

from gbdp.serve.db import query

app = FastAPI(title="GBDP API", version="0.1.0")


def _league_id(code: str | None) -> str | None:
    if not code:
        return None
    import yaml
    from pathlib import Path

    path = Path("configs/leagues.yaml")
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        leagues = yaml.safe_load(f).get("leagues", {})
    meta = leagues.get(code.upper())
    return meta.get("league_id") if meta else None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/leagues")
def leagues(as_of: str | None = None):
    return query("dim_league", dt=as_of)


@app.get("/teams")
def teams(league: str | None = None, season: int | None = None, as_of: str | None = None):
    where = []
    if league:
        league_id = _league_id(league)
        if league_id:
            where.append(f"league_id = '{league_id}'")
    return query("dim_team", dt=as_of, where=" AND ".join(where) if where else None)


@app.get("/players")
def players(query_text: str | None = Query(None, alias="query"), league: str | None = None, as_of: str | None = None):
    where = []
    if query_text:
        where.append(f"lower(primary_name) LIKE '%{query_text.lower()}%'")
    if league:
        league_id = _league_id(league)
        if league_id:
            where.append(f"league_id = '{league_id}'")
    return query("dim_player", dt=as_of, where=" AND ".join(where) if where else None)


@app.get("/games")
def games(league: str | None = None, date: str | None = None, as_of: str | None = None):
    where = []
    if date:
        where.append(f"game_date LIKE '{date}%'")
    if league:
        league_id = _league_id(league)
        if league_id:
            where.append(f"league_id = '{league_id}'")
    return query("fact_game", dt=as_of, where=" AND ".join(where) if where else None)


@app.get("/games/{game_id}")
def game_by_id(game_id: str, as_of: str | None = None):
    return query("fact_game", dt=as_of, where=f"game_id = '{game_id}'")


@app.get("/players/{player_id}/stats")
def player_stats(player_id: str, window: str | None = None, as_of: str | None = None):
    return query("fact_pitch", dt=as_of, where=f"batter_id = '{player_id}'")


@app.get("/run-expectancy")
def run_expectancy(league: str | None = None, season: int | None = None, as_of: str | None = None):
    return query("run_expectancy", dt=as_of)


@app.get("/breakout-candidates")
def breakout_candidates(league: str | None = None, season: int | None = None, as_of: str | None = None):
    return query("breakout_candidates", dt=as_of)
