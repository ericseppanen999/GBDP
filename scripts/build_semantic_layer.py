"""Generate docs/semantic_layer_uc.sql, the Unity Catalog DDL for the semantic layer.

From configs/semantic_layer.yaml: COMMENTs on every gold table and column,
the plain views (v_*) and the metric views (mv_*, CREATE VIEW ... WITH
METRICS LANGUAGE YAML). The file is for review and for running by hand in
the SQL editor; deploys apply the same statements with `gbdp semantic-layer`
(the gbdp-semantic-layer job in databricks.yml), which skips comments on
tables or columns that don't exist yet.

    python scripts/build_semantic_layer.py           # write the file
    python scripts/build_semantic_layer.py --check   # exit 1 if it is stale

    # Create the views and metric views in a scratch schema (reading the real
    # gold tables, touching nothing in gold_gbdp) and query every measure.
    # Uses the SQL warehouse through the `databricks` CLI's auth.
    python scripts/build_semantic_layer.py --validate-in semantic_check
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from gbdp.semantic import layer as semantic  # noqa: E402
from gbdp.semantic.uc import sql_script  # noqa: E402

OUT = ROOT / "docs" / "semantic_layer_uc.sql"
# The lakehouse's own schemas (databricks.yml): a scratch check never writes there.
PROTECTED_SCHEMAS = frozenset({"bronze_gbdp", "silver_gbdp", "gold_gbdp"})


def build(catalog: str = "gbdp", schema: str = "gold_gbdp", uc_mode: str = "managed") -> str:
    return sql_script(semantic.load(ROOT / "configs" / "semantic_layer.yaml"), catalog, schema, uc_mode)


def validate(catalog: str, schema: str, scratch: str, uc_mode: str, warehouse, out=print) -> int:
    """Create every view and metric view in catalog.scratch over catalog.schema's
    tables, then SELECT every measure of every metric view. Returns failures."""
    from gbdp.semantic import uc

    if scratch in {schema} | PROTECTED_SCHEMAS:
        raise SystemExit(f"--validate-in must be a scratch schema, not {scratch} "
                         f"(never {', '.join(sorted(PROTECTED_SCHEMAS | {schema}))})")
    sl = semantic.load(ROOT / "configs" / "semantic_layer.yaml")
    failures = 0

    def run(label: str, sql: str) -> bool:
        nonlocal failures
        try:
            warehouse.query(sql)
            out(f"OK    {label}")
            return True
        except RuntimeError as exc:
            failures += 1
            out(f"FAIL  {label}: {str(exc)[:300]}")
            return False

    run(f"schema {catalog}.{scratch}", f"CREATE SCHEMA IF NOT EXISTS {catalog}.{scratch}")
    created = set()
    for s in uc.statements(sl, catalog, schema, uc_mode, view_schema=scratch, include_comments=False):
        if run(f"{s.kind} {s.table}", s.sql):
            created.add(s.table)
    for name in sl.metric_views:
        if name not in created:
            continue
        measures = ", ".join(f"MEASURE(`{m}`) AS `{m}`" for m, x in sl.metrics.items() if x.metric_view == name)
        run(f"query {name}", f"SELECT {measures} FROM {catalog}.{scratch}.{name}")
    out(f"{failures} failure(s); drop the scratch schema with: DROP SCHEMA {catalog}.{scratch} CASCADE")
    return failures


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--catalog", default="gbdp")
    ap.add_argument("--schema", default="gold_gbdp")
    ap.add_argument("--uc-mode", default="managed", choices=["managed", "external"])
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--check", action="store_true", help="exit 1 if the file differs from what would be written")
    ap.add_argument("--validate-in", metavar="SCRATCH_SCHEMA",
                    help="create the views and metric views in this scratch schema and query them")
    ap.add_argument("--warehouse", default=None, help="SQL warehouse ID (default: GBDP_WAREHOUSE_ID or the bundle's)")
    ap.add_argument("--profile", default=None, help="databricks CLI profile")
    args = ap.parse_args()
    if args.validate_in:
        import os

        sys.path.insert(0, str(ROOT / "scripts"))
        from verify_lakehouse import Warehouse

        wh = Warehouse(args.warehouse or os.getenv("GBDP_WAREHOUSE_ID", "a9d3d2589a30ab67"),
                       args.profile or os.getenv("DATABRICKS_CONFIG_PROFILE"))
        return 1 if validate(args.catalog, args.schema, args.validate_in, args.uc_mode, wh) else 0
    text = build(args.catalog, args.schema, args.uc_mode)
    if args.check:
        current = args.out.read_text(encoding="utf-8") if args.out.exists() else ""
        if current != text:
            print(f"{args.out} is stale: run python scripts/build_semantic_layer.py")
            return 1
        print(f"{args.out} is current")
        return 0
    args.out.write_text(text, encoding="utf-8", newline="\n")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
