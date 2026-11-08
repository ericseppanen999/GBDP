from __future__ import annotations

import hmac
import os
from datetime import date as Date
from typing import Any, List, Literal, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field

from gbdp.semantic.compile import FILTER_OPS, Filter as MetricFilter, MetricRequest
from gbdp.semantic.layer import SemanticLayerError
from gbdp.serve.db import (
    Filter, UnsafeSqlError, compute_metrics, load_semantic_layer, query, run_sql, semantic_layer,
)

app = FastAPI(title="GBDP API", version="0.1.0")


def _league_id(code: str | None) -> str | None:
    if not code:
        return None
    import yaml
    from pathlib import Path
    from gbdp.utils.io import path_exists

    path = Path("configs/leagues.yaml")
    if not path_exists(path):
        return None
    with path.open("r", encoding="utf-8") as f:
        leagues = yaml.safe_load(f).get("leagues", {})
    meta = leagues.get(code.upper())
    return meta.get("league_id") if meta else None


def _dt(value: Date | None) -> str | None:
    return value.isoformat() if value else None


def _league_filter(code: str | None) -> List[Filter]:
    if not code:
        return []
    league_id = _league_id(code)
    if not league_id:
        raise HTTPException(status_code=404, detail=f"unknown league '{code}'")
    return [("league_id", "eq", league_id)]


def _require_token(authorization: str | None = Header(None)) -> None:
    # /query takes free-form SQL, so it can require a token. Set GBDP_API_TOKEN
    # before exposing the server beyond localhost.
    token = os.getenv("GBDP_API_TOKEN")
    if token and not hmac.compare_digest(authorization or "", f"Bearer {token}"):
        raise HTTPException(status_code=401, detail="invalid or missing bearer token")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/leagues")
def leagues(as_of: Date | None = None):
    return query("dim_league", dt=_dt(as_of))


@app.get("/teams")
def teams(league: str | None = None, season: int | None = None, as_of: Date | None = None):
    return query("dim_team", dt=_dt(as_of), filters=[("is_current", "eq", True), *_league_filter(league)])


@app.get("/players")
def players(query_text: str | None = Query(None, alias="query"), league: str | None = None, as_of: Date | None = None):
    filters: List[Filter] = [("is_current", "eq", True), *_league_filter(league)]
    if query_text:
        filters.append(("primary_name", "contains", query_text))
    return query("dim_player", dt=_dt(as_of), filters=filters)


@app.get("/games")
def games(league: str | None = None, date: Date | None = None, as_of: Date | None = None):
    # Each game lands in the snapshot for its date, so date picks the snapshot.
    return query("fact_game", dt=_dt(date or as_of), filters=_league_filter(league))


@app.get("/games/{game_id}")
def game_by_id(game_id: str, as_of: Date | None = None):
    rows = query("fact_game", dt=_dt(as_of), filters=[("game_id", "eq", game_id)], all_dts=as_of is None)
    if not rows:
        raise HTTPException(status_code=404, detail="game not found")
    return rows


@app.get("/players/{player_id}/stats")
def player_stats(player_id: str, window: str | None = None, as_of: Date | None = None):
    filters: List[Filter] = [("player_id", "eq", player_id)]
    if window == "rolling_30d":
        return query("feature_player_rolling_30d", dt=_dt(as_of), filters=filters)
    return query("fact_boxscore_batting", dt=_dt(as_of), filters=filters, all_dts=as_of is None)


@app.get("/run-expectancy")
def run_expectancy(league: str | None = None, season: int | None = None, as_of: Date | None = None):
    """Season-to-date RE24: expected runs to the end of the inning per base/out state.

    base_state is a bitmask (1 = runner on first, 2 = second, 4 = third).
    With a season and no as_of, returns that season's latest table.
    """
    filters: List[Filter] = _league_filter(league)
    if season is not None:
        filters.append(("season_year", "eq", season))
    if season is not None and as_of is None:
        # 24 states x leagues x every date of the season; the default 1000 would truncate it.
        rows = query("run_expectancy", filters=filters, all_dts=True, limit=100_000)
        last = max((r["dt"] for r in rows), default=None)
        return [r for r in rows if r["dt"] == last]
    return query("run_expectancy", dt=_dt(as_of), filters=filters)


@app.get("/breakout-candidates")
def breakout_candidates(league: str | None = None, season: int | None = None, as_of: Date | None = None):
    return query("breakout_candidates", dt=_dt(as_of))


@app.get("/schema")
def schema():
    """Semantic layer: table descriptions, columns, caveats, vocabularies,
    views, metric views and metric definitions. Read this before writing a
    /query SQL string."""
    return load_semantic_layer()


@app.get("/metrics")
def metrics():
    """Every governed metric: name, description, metric view and format.
    GET /metrics/{name} for its SQL, dimensions and caveats."""
    sl = semantic_layer()
    return [
        {"name": m.name, "description": m.description, "metric_view": m.metric_view, "format": m.format}
        for m in sl.metrics.values()
    ]


@app.get("/metrics/{name}")
def metric(name: str):
    try:
        return semantic_layer().metric_summary(name)
    except SemanticLayerError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


class MetricFilterIn(BaseModel):
    dimension: str
    op: Literal[tuple(FILTER_OPS)] = "eq"  # type: ignore[valid-type]
    value: Any = None


class HavingIn(BaseModel):
    metric: str
    op: Literal["eq", "ne", "gt", "gte", "lt", "lte"]
    value: float


class OrderIn(BaseModel):
    column: str
    direction: Literal["asc", "desc"] = "desc"


class MetricQuery(BaseModel):
    metrics: List[str] = Field(min_length=1)
    dimensions: List[str] = []
    filters: List[MetricFilterIn] = []
    start: Optional[Date] = None
    end: Optional[Date] = None
    having: List[HavingIn] = []
    order_by: List[OrderIn] = []
    limit: int = Field(1000, ge=1, le=100_000)


@app.post("/metrics/query")
def metric_query(req: MetricQuery):
    """Compute metrics by dimensions, e.g. {"metrics": ["avg", "ops"],
    "dimensions": ["batter_name"], "filters": [{"dimension": "league_code",
    "value": "MLB"}], "start": "2026-05-01", "end": "2026-05-31"}. All
    metrics must share a metric view. The SQL comes from the semantic layer;
    values are bound, never pasted in. Returns rows and the SQL used."""
    request = MetricRequest(
        metrics=req.metrics,
        dimensions=req.dimensions,
        filters=[MetricFilter(f.dimension, f.op, f.value) for f in req.filters],
        start=req.start.isoformat() if req.start else None,
        end=req.end.isoformat() if req.end else None,
        having=[(h.metric, h.op, h.value) for h in req.having],
        order_by=[(o.column, o.direction) for o in req.order_by],
        limit=req.limit,
    )
    try:
        return compute_metrics(request)
    except SemanticLayerError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"metric query failed: {exc}")


class QueryRequest(BaseModel):
    sql: str
    limit: int = 1000


@app.post("/query", dependencies=[Depends(_require_token)])
def run_query(req: QueryRequest):
    """Ad-hoc read-only SQL against the gold layer. Tables are addressable
    by their bare semantic-layer name (e.g. `fact_game`) regardless of
    whether the backend is parquet/DuckDB or Delta/Spark. See GET /schema
    for what's available before writing a query."""
    try:
        return run_sql(req.sql, limit=req.limit)
    except UnsafeSqlError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"query failed: {exc}")
