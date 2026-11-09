"""Read-only smoke check of the FastAPI cloud mode against the live warehouse.

Runs real endpoints through the databricks backend (FastAPI TestClient, no
server) and prints status, row count and a sample row for each. Only SELECTs
are sent.

    python scripts/smoke_serve_cloud.py [--profile NAME] [--date 2026-06-10] [--warehouse ID]
    python scripts/smoke_serve_cloud.py --direct        # DATABRICKS_HOST/DATABRICKS_TOKEN, HTTPS like the server

By default statements go through the `databricks` CLI's own auth (like
scripts/verify_lakehouse.py), so no token has to be exported. --direct uses
the exact HTTPS path the deployed server uses.
Exits 1 if an endpoint returns a 5xx. A 502 for one table (a column the
rebuild hasn't added yet) is reported, and counts as a failure.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gbdp.serve import db, warehouse as wh  # noqa: E402


def cli_transport(profile: str | None):
    def send(method: str, path: str, body: dict | None) -> dict:
        cmd = ["databricks", "api", method.lower(), path]
        if profile:
            cmd += ["--profile", profile]
        tmp = None
        if body is not None:
            fd, tmp = tempfile.mkstemp(suffix=".json")
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(body, f)
            cmd += ["--json", "@" + tmp]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
        finally:
            if tmp:
                os.unlink(tmp)
        if r.returncode:
            raise wh.WarehouseError((r.stderr or r.stdout).strip()[:500])
        return json.loads(r.stdout)

    return send


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile")
    ap.add_argument("--date", default="2026-06-10")
    ap.add_argument("--warehouse", default=os.getenv("GBDP_WAREHOUSE_ID", wh.DEFAULT_WAREHOUSE_ID))
    ap.add_argument("--direct", action="store_true")
    args = ap.parse_args()

    os.environ["GBDP_SERVE_BACKEND"] = "databricks"
    os.environ["GBDP_WAREHOUSE_ID"] = args.warehouse
    if args.direct:
        db.set_warehouse(None)
    else:
        settings = wh.WarehouseSettings(
            host="https://cli.invalid", token="unused-cli-auth", warehouse_id=args.warehouse,
            catalog=os.getenv("GBDP_SERVE_CATALOG", "gbdp"), schema=os.getenv("GBDP_SERVE_SCHEMA", "gold_gbdp"),
        )
        db.set_warehouse(wh.Warehouse(settings, transport=cli_transport(args.profile)))

    from fastapi.testclient import TestClient
    from gbdp.serve.api import app

    client = TestClient(app, raise_server_exceptions=False)
    checks = [
        ("GET", "/health", None),
        ("GET", "/leagues", None),
        ("GET", f"/games?date={args.date}", None),
        ("GET", f"/games?date={args.date}&league=MLB", None),
        ("GET", "/teams?league=MLB", None),
        ("GET", "/players?query=ohtani&league=MLB", None),
        ("GET", f"/breakout-candidates?as_of={args.date}", None),
        ("GET", "/run-expectancy?league=MLB&season=2026", None),
        ("POST", "/query", {"sql": "SELECT league_code, count(*) AS n FROM v_team_game GROUP BY 1 ORDER BY 2 DESC", "limit": 10}),
        ("POST", "/query", {"sql": "SELECT count(*) AS n FROM fact_game"}),
        ("POST", "/metrics/query", {"metrics": ["avg"], "dimensions": ["league_code"], "start": args.date, "end": args.date, "limit": 5}),
    ]
    bad = 0
    for method, path, body in checks:
        r = client.request(method, path, json=body)
        try:
            data = r.json()
        except ValueError:
            data = None
        if r.status_code >= 500:
            bad += 1
        if isinstance(data, dict) and "rows" in data:
            rows = data["rows"]
        else:
            rows = data if isinstance(data, list) else None
        label = f"{method} {path}" + (f" {json.dumps(body)[:90]}" if body else "")
        if rows is not None:
            sample = json.dumps(rows[0], default=str)[:160] if rows else "-"
            print(f"{r.status_code} {label}\n    rows={len(rows)} first={sample}")
        else:
            print(f"{r.status_code} {label}\n    {json.dumps(data, default=str)[:300]}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
