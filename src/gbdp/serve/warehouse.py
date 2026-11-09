"""Databricks SQL warehouse client for the serving layer (cloud mode).

Talks to the SQL Statement Execution API (POST /api/2.0/sql/statements) over
HTTPS with a bearer token. Values travel as named parameters (`:p0`), never
pasted into the statement text. Nothing here writes: callers hand it SELECTs
(see serve.db for the read-only guard and the LIMIT wrapper).

Settings come from the environment, never from code:

  GBDP_SERVE_BACKEND    duckdb (default) | databricks
  DATABRICKS_HOST       https://<workspace>.cloud.databricks.com
  DATABRICKS_TOKEN      a personal access token or service-principal token
  GBDP_WAREHOUSE_ID     SQL warehouse id (default: the repo's warehouse)
  GBDP_SERVE_CATALOG    Unity Catalog catalog (default gbdp)
  GBDP_SERVE_SCHEMA     schema holding the gold tables (default gold_gbdp)
  GBDP_SERVE_UC_MODE    managed (default; renames "2b" to "c_2b") | external
  GBDP_SERVE_TIMEOUT_S  total seconds to wait for one statement (default 120)
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import requests

DEFAULT_WAREHOUSE_ID = "a9d3d2589a30ab67"
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

Transport = Callable[[str, str, Optional[dict]], dict]


class WarehouseError(RuntimeError):
    """The warehouse rejected or failed a statement (never carries credentials)."""

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code

    @property
    def table_missing(self) -> bool:
        return "TABLE_OR_VIEW_NOT_FOUND" in str(self) or self.code == "TABLE_OR_VIEW_NOT_FOUND"


class WarehouseConfigError(WarehouseError):
    """Cloud mode is selected but a setting is missing or malformed."""


@dataclass(frozen=True)
class WarehouseSettings:
    host: str
    token: str
    warehouse_id: str = DEFAULT_WAREHOUSE_ID
    catalog: str = "gbdp"
    schema: str = "gold_gbdp"
    uc_mode: str = "managed"
    timeout_s: float = 120.0

    @classmethod
    def from_env(cls) -> "WarehouseSettings":
        env = os.environ
        host = (env.get("DATABRICKS_HOST") or "").strip().rstrip("/")
        token = (env.get("DATABRICKS_TOKEN") or "").strip()
        missing = [n for n, v in (("DATABRICKS_HOST", host), ("DATABRICKS_TOKEN", token)) if not v]
        if missing:
            raise WarehouseConfigError(
                f"GBDP_SERVE_BACKEND=databricks needs {', '.join(missing)} in the environment"
            )
        if not host.startswith(("https://", "http://")):
            host = "https://" + host
        catalog = env.get("GBDP_SERVE_CATALOG", "gbdp").strip()
        schema = env.get("GBDP_SERVE_SCHEMA", "gold_gbdp").strip()
        for label, value in (("GBDP_SERVE_CATALOG", catalog), ("GBDP_SERVE_SCHEMA", schema)):
            if not _IDENT.match(value):
                raise WarehouseConfigError(f"{label}={value!r} is not a plain identifier")
        try:
            timeout_s = float(env.get("GBDP_SERVE_TIMEOUT_S", "120"))
        except ValueError as exc:
            raise WarehouseConfigError("GBDP_SERVE_TIMEOUT_S must be a number") from exc
        return cls(
            host=host,
            token=token,
            warehouse_id=(env.get("GBDP_WAREHOUSE_ID") or DEFAULT_WAREHOUSE_ID).strip(),
            catalog=catalog,
            schema=schema,
            uc_mode=(env.get("GBDP_SERVE_UC_MODE") or "managed").strip().lower(),
            timeout_s=timeout_s,
        )

    def qualified(self, table: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{table}`"


def param(name: str, value: Any) -> Dict[str, str]:
    """One Statement API named parameter, typed so comparisons against typed columns bind."""
    if isinstance(value, bool):
        return {"name": name, "value": "true" if value else "false", "type": "BOOLEAN"}
    if isinstance(value, int):
        return {"name": name, "value": str(value), "type": "BIGINT"}
    if isinstance(value, float):
        return {"name": name, "value": repr(value), "type": "DOUBLE"}
    if isinstance(value, (date, datetime)):
        return {"name": name, "value": value.isoformat(), "type": "STRING"}
    return {"name": name, "value": str(value), "type": "STRING"}


def named_params(values: Sequence[Any]) -> List[Dict[str, str]]:
    """Positional values as p0, p1, ... (the semantic layer's "named" param style)."""
    return [param(f"p{i}", v) for i, v in enumerate(values)]


_INT_TYPES = {"BYTE", "SHORT", "INT", "LONG"}
_FLOAT_TYPES = {"FLOAT", "DOUBLE", "DECIMAL"}
_JSON_TYPES = {"ARRAY", "MAP", "STRUCT"}


def convert_value(value: Any, type_name: str) -> Any:
    """The API returns every cell as a string; turn it back into the DuckDB path's Python type."""
    if value is None:
        return None
    t = (type_name or "").upper()
    if t in _INT_TYPES:
        return int(value)
    if t in _FLOAT_TYPES:
        return float(value)
    if t == "BOOLEAN":
        return str(value).lower() == "true"
    if t in _JSON_TYPES and isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def requests_transport(settings: WarehouseSettings) -> Transport:
    """HTTPS with a bearer token. The token is only ever in the Authorization header."""

    def send(method: str, path: str, body: Optional[dict]) -> dict:
        try:
            resp = requests.request(
                method,
                settings.host + path,
                json=body,
                headers={"Authorization": f"Bearer {settings.token}"},
                timeout=60,
            )
        except requests.RequestException as exc:
            raise WarehouseError(f"could not reach the Databricks workspace: {type(exc).__name__}") from exc
        if resp.status_code >= 400:
            detail = resp.text[:500]
            code = None
            try:
                payload = resp.json()
                detail = payload.get("message") or detail
                code = payload.get("error_code")
            except ValueError:
                pass
            raise WarehouseError(f"Databricks API {resp.status_code}: {detail}", code)
        return resp.json()

    return send


class Warehouse:
    """Runs one statement at a time on a SQL warehouse and returns rows as dicts."""

    def __init__(self, settings: WarehouseSettings, transport: Transport | None = None,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.settings = settings
        self._send = transport or requests_transport(settings)
        self._sleep = sleep

    def execute(self, statement: str, params: Sequence[Dict[str, str]] = (), limit: int = 1000,
                use_schema: bool = True) -> Tuple[List[str], List[Dict[str, Any]]]:
        s = self.settings
        body: Dict[str, Any] = {
            "warehouse_id": s.warehouse_id,
            "statement": statement,
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
            "row_limit": int(limit),
        }
        if use_schema:
            body["catalog"], body["schema"] = s.catalog, s.schema
        if params:
            body["parameters"] = list(params)
        result = self._send("POST", "/api/2.0/sql/statements", body)
        deadline = time.monotonic() + s.timeout_s
        while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
            if time.monotonic() >= deadline:
                try:
                    self._send("POST", f"/api/2.0/sql/statements/{result['statement_id']}/cancel", None)
                except WarehouseError:
                    pass
                raise WarehouseError(f"statement did not finish in {s.timeout_s:g}s and was cancelled")
            self._sleep(1.0)
            result = self._send("GET", f"/api/2.0/sql/statements/{result['statement_id']}", None)
        status = result.get("status", {})
        if status.get("state") != "SUCCEEDED":
            err = status.get("error") or {}
            raise WarehouseError(
                err.get("message") or f"statement ended {status.get('state')}", err.get("error_code")
            )
        schema_cols = result.get("manifest", {}).get("schema", {}).get("columns", [])
        names = [c["name"] for c in schema_cols]
        types = [c.get("type_name", "STRING") for c in schema_cols]
        chunk = result.get("result") or {}
        data: List[list] = list(chunk.get("data_array") or [])
        nxt = chunk.get("next_chunk_internal_link")
        while nxt:
            more = self._send("GET", nxt, None)
            data += more.get("data_array") or []
            nxt = more.get("next_chunk_internal_link")
        rows = [
            {n: convert_value(v, t) for n, t, v in zip(names, types, row)}
            for row in data
        ]
        return names, rows[: int(limit)]
