"""Let a model explore gold with read-only SQL and find its own stories.

    python scripts/explore_agent.py --cloud                          # production UC, Claude (ANTHROPIC_API_KEY)
    python scripts/explore_agent.py --backend groq --lens arsenal   # local gold, an open-weights model on Groq
    python scripts/explore_agent.py --list-lenses

Every finding cites the queries behind it; code re-runs them and checks every
number and name in the post against their rows, then a skeptic call reviews
the SQL. Writes content_queue/explore_<as_of>.md (+ .json) for a human to
review. Nothing is published.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--as-of", help="treat this date as today (default: the latest game date)")
    ap.add_argument("--lens", help="where to start looking (default: a random one)")
    ap.add_argument("--list-lenses", action="store_true")
    ap.add_argument("--findings", type=int, default=3, help="stop after this many accepted findings")
    ap.add_argument("--max-steps", type=int, default=40, help="tool-call budget")
    ap.add_argument("--seed", type=int, help="seed for the lens pick")
    ap.add_argument("--out", default="content_queue")
    ap.add_argument("--cloud", action="store_true", help="read production UC through the databricks CLI")
    ap.add_argument("--profile", help="databricks CLI profile (with --cloud)")
    ap.add_argument("--backend", choices=["claude", "groq"], default="claude")
    ap.add_argument("--model", help="default claude-opus-5-5; groq openai/gpt-oss-120b")
    args = ap.parse_args()

    from gbdp.content import explorer

    if args.list_lenses:
        for name, text in explorer.LENSES.items():
            print(f"{name:13} {text}")
        return 0
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

    if args.backend == "groq":
        # A free tier's 8k tokens a minute: few rows per result, one full result in context.
        backend = explorer.OpenAICompatBackend(model=args.model or "openai/gpt-oss-120b")
        sizes = {"keep_full": 2, "rows_shown": 15, "chars": 2500}
    else:
        import anthropic

        backend = explorer.ClaudeBackend(anthropic.Anthropic(), model=args.model or explorer.agent.MODEL)
        sizes = {}
    path = explorer.explore(backend, as_of=args.as_of, out_dir=Path(args.out), lens=args.lens,
                            findings=args.findings, max_steps=args.max_steps, seed=args.seed, **sizes)
    print(f"wrote {path}")
    print(path.read_text(encoding="utf-8").split("\n")[2])
    return 0


if __name__ == "__main__":
    sys.exit(main())
