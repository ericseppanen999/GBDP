"""Train the breakout baseline on gold's ml_breakout_examples and save the artifact.

Logistic regression on gbdp/ml/breakout.py's MODEL_INPUTS, splits from
configs/ml.yaml (time-based, embargoed), L2 strength picked on validation
log-loss. Prints the evaluation (model vs the naive base rate and simple
heuristics on train, validation and test) and writes the artifact that gold's
breakout_candidates scores with. Deterministic: the same gold gives the same
file.

    python scripts/train_breakout.py --gold-root ./data/gold            # train + write configs/models/...
    python scripts/train_breakout.py --gold-root ./data/gold --dry-run  # evaluate only

Exits 2, writing nothing, when there isn't enough labeled data. Re-publish
gold afterwards (gold_publish over the dates) to rescore breakout_candidates.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
CWD = Path.cwd()  # relative arguments are relative to where you ran it
os.chdir(ROOT)  # configs/ paths are relative to the repo, as in the pipeline

from gbdp.ml.config import ml_config  # noqa: E402
from gbdp.ml.model import write_artifact  # noqa: E402
from gbdp.ml.train import train  # noqa: E402
from gbdp.utils.io import gold_root, spark_path, storage_format  # noqa: E402

TABLE = "ml_breakout_examples"


def read_examples(gold: Path) -> List[Dict[str, Any]]:
    base = gold / TABLE
    if storage_format() == "parquet":
        files = sorted(base.glob("dt=*/*.parquet"))
        if not files:
            return []
        import duckdb

        con = duckdb.connect()
        cur = con.execute("SELECT * FROM read_parquet(?, union_by_name=true, hive_partitioning=false)",
                          [[str(f) for f in files]])
        names = [c[0] for c in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    return [r.asDict() for r in spark.read.format("delta").load(spark_path(base)).collect()]


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--gold-root", type=Path, help="gold root (default: GBDP_GOLD_ROOT / GBDP_DATA_ROOT)")
    ap.add_argument("--artifact", type=Path, help="where to write the model (default: configs/ml.yaml model.artifact)")
    ap.add_argument("--dry-run", action="store_true", help="evaluate only; write nothing")
    ap.add_argument("--report", type=Path, help="also write the full report as JSON here")
    args = ap.parse_args(argv)

    cfg = ml_config()
    gold = (CWD / args.gold_root).resolve() if args.gold_root else gold_root().resolve()
    rows = read_examples(gold)
    artifact, report = train(rows, cfg)
    report["gold_root"] = str(gold)
    text = json.dumps(report, indent=2, sort_keys=True, default=str)
    print(text)
    if args.report:
        (CWD / args.report).write_text(text + "\n", encoding="utf-8")
    if artifact is None:
        print(f"\nNo model written: {report['refused']}", file=sys.stderr)
        return 2
    if not args.dry_run:
        out = (CWD / args.artifact) if args.artifact else Path(cfg.model.artifact)
        write_artifact(out, artifact)
        print(f"\nwrote {out} ({artifact['model_version']})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
