"""Draft social posts from insight_candidates; keep only numbers gold re-derives.

    python scripts/content_agent.py --cloud --as-of 2026-08-31     # production UC, via the databricks CLI login
    python scripts/content_agent.py --as-of 2025-08-31             # local gold (GBDP_GOLD_ROOT)
    python scripts/content_agent.py --cloud --backend ollama --model qwen2.5:3b   # a local open model, no API key

Needs Claude API credentials (ANTHROPIC_API_KEY, or an `ant auth login`
profile). Writes content_queue/drafts_<as_of>.md (+ .json) for a human to
review: drafts whose every claim verify_claim confirmed and whose text states
no number outside the insight's data, then the rejected ones with reasons.
Nothing is published.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--as-of", help="insight_candidates dt (default: the latest)")
    ap.add_argument("--limit", type=int, default=12, help="insights to draft from")
    ap.add_argument("--out", default="content_queue")
    ap.add_argument("--cloud", action="store_true", help="read production UC through the databricks CLI")
    ap.add_argument("--profile", help="databricks CLI profile (with --cloud)")
    ap.add_argument("--backend", choices=["claude", "ollama"], default="claude",
                    help="ollama: a local open-source model, for testing without a Claude API key")
    ap.add_argument("--model", help="model name (default claude-opus-5-5; for ollama e.g. qwen2.5:3b)")
    args = ap.parse_args()

    if args.cloud:
        from smoke_serve_cloud import cli_transport

        from gbdp.serve import db
        from gbdp.serve import warehouse as wh

        os.environ["GBDP_SERVE_BACKEND"] = "databricks"
        settings = wh.WarehouseSettings(
            host="https://cli.invalid", token="unused-cli-auth",
            warehouse_id=os.getenv("GBDP_WAREHOUSE_ID", wh.DEFAULT_WAREHOUSE_ID),
            catalog=os.getenv("GBDP_SERVE_CATALOG", "gbdp"), schema=os.getenv("GBDP_SERVE_SCHEMA", "gold_gbdp"),
        )
        db.set_warehouse(wh.Warehouse(settings, transport=cli_transport(args.profile)))

    from gbdp.content.agent import MODEL, run

    if args.backend == "ollama":
        path = run(None, args.as_of, Path(args.out), limit=args.limit, model=args.model or "qwen2.5:3b", backend="ollama")
    else:
        import anthropic

        path = run(anthropic.Anthropic(), args.as_of, Path(args.out), limit=args.limit, model=args.model or MODEL)
    print(f"wrote {path}")
    print(path.read_text(encoding="utf-8").split("\n")[2])
    return 0


if __name__ == "__main__":
    sys.exit(main())
