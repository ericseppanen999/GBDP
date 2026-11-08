"""Generate docs/semantic_layer_uc.sql, the Unity Catalog DDL for the semantic layer.

From configs/semantic_layer.yaml: COMMENTs on every gold table and column,
the plain views (v_*) and the metric views (mv_*, CREATE VIEW ... WITH
METRICS LANGUAGE YAML). The file is for review and for running by hand in
the SQL editor; deploys apply the same statements with `gbdp semantic-layer`
(the gbdp_semantic_layer task in databricks.yml), which skips comments on
tables or columns that don't exist yet.

    python scripts/build_semantic_layer.py           # write the file
    python scripts/build_semantic_layer.py --check   # exit 1 if it is stale
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


def build(catalog: str = "gbdp", schema: str = "gold_gbdp", uc_mode: str = "managed") -> str:
    return sql_script(semantic.load(ROOT / "configs" / "semantic_layer.yaml"), catalog, schema, uc_mode)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--catalog", default="gbdp")
    ap.add_argument("--schema", default="gold_gbdp")
    ap.add_argument("--uc-mode", default="managed", choices=["managed", "external"])
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--check", action="store_true", help="exit 1 if the file differs from what would be written")
    args = ap.parse_args()
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
