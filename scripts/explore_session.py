"""The explorer's tools as a command line, for an agent that drives itself
(a Claude Code subagent, say) instead of the API loop in content/explorer.py.

State lives in one session file, so every call is a separate process and the
checks stay in code: `submit` runs the same mechanical checks as the API
explorer (cited queries re-run, every number and name in the post found in
their rows, rankings backed, house rules), and a finding only counts once a
separate reviewer records a pass with `review`.

    python scripts/explore_session.py new --lens arsenal [--as-of 2025-09-28]
    python scripts/explore_session.py tables
    python scripts/explore_session.py describe v_pitch
    python scripts/explore_session.py query --purpose "slider whiffs" --sql "SELECT ..."
    python scripts/explore_session.py show q3
    python scripts/explore_session.py submit --title ... --post ... --why ... --subjects "Josh Hader" --queries q3,q4
    python scripts/explore_session.py pending                 # what the reviewer should read
    python scripts/explore_session.py review f1 --verdict fail --problem "thin sample"
    python scripts/explore_session.py report                  # explore_<as_of>.md (+ .json)

--session (default content_queue/explore_session.json) picks the file.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _load(path: Path):
    from gbdp.content import explorer as ex
    from gbdp.serve.db import run_sql

    s = json.loads(path.read_text(encoding="utf-8"))
    e = ex.Exploration(as_of=s["as_of"], coverage=s["coverage"], lens=s["lens"], avoid=s["avoid"],
                       run_sql=lambda q: run_sql(q, limit=1000))
    e.queries = {q["query_id"]: ex.Query(**q) for q in s["queries"]}
    e.findings = [ex.Finding(**{k: f[k] for k in ("title", "post", "why", "subjects", "query_ids", "problems")})
                  for f in s["findings"]]
    reviews = {f["id"]: f.get("review") for f in s["findings"]}
    return e, reviews, s


def _save(path: Path, e, reviews, s) -> None:
    s["queries"] = [q.__dict__ for q in e.queries.values()]
    s["findings"] = [dict(f.__dict__, id=f"f{i + 1}", review=reviews.get(f"f{i + 1}")) for i, f in enumerate(e.findings)]
    path.write_text(json.dumps(s, ensure_ascii=False, indent=1, default=str), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--session", default="content_queue/explore_session.json")
    sub = ap.add_subparsers(dest="cmd", required=True)
    n = sub.add_parser("new")
    n.add_argument("--lens", default="surprise")
    n.add_argument("--as-of")
    n.add_argument("--out", default="content_queue")
    sub.add_parser("tables")
    d = sub.add_parser("describe")
    d.add_argument("table")
    sub.add_parser("metrics")
    m = sub.add_parser("metric")
    m.add_argument("name")
    q = sub.add_parser("query")
    q.add_argument("--sql", required=True)
    q.add_argument("--purpose", required=True)
    q.add_argument("--rows", type=int, default=40)
    sh = sub.add_parser("show")
    sh.add_argument("query_id")
    sm = sub.add_parser("submit")
    for a in ("--title", "--post", "--why"):
        sm.add_argument(a, required=True)
    sm.add_argument("--subjects", required=True, help="comma-separated, as named in the results")
    sm.add_argument("--queries", required=True, help="comma-separated query_ids")
    sub.add_parser("pending")
    r = sub.add_parser("review")
    r.add_argument("finding_id")
    r.add_argument("--verdict", choices=["pass", "fail"], required=True)
    r.add_argument("--problem", action="append", default=[])
    sub.add_parser("report")
    args = ap.parse_args()

    from gbdp.content import explorer as ex

    path = Path(args.session)
    if args.cmd == "new":
        from gbdp.content import agent

        cov = agent.coverage(args.as_of)
        as_of = args.as_of or str(cov.get("end"))
        out = Path(args.out)
        s = {"as_of": as_of, "coverage": cov, "lens": args.lens, "out": str(out),
             "avoid": ex.recent_subjects(out, as_of), "queries": [], "findings": []}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(s, indent=1, default=str), encoding="utf-8")
        print(json.dumps({"session": str(path), "coverage": cov, "as_of": as_of, "lens": ex.LENSES.get(args.lens, args.lens),
                          "recently_covered": s["avoid"]}, default=str))
        return 0

    e, reviews, s = _load(path)
    if args.cmd in ("tables", "describe", "metrics", "metric", "query", "show"):
        name, a = {"tables": ("list_tables", {}), "describe": ("describe_table", {"table": getattr(args, "table", "")}),
                   "metrics": ("list_metrics", {}), "metric": ("describe_metric", {"name": getattr(args, "name", "")}),
                   "query": ("query", {"sql": getattr(args, "sql", ""), "purpose": getattr(args, "purpose", "")}),
                   "show": ("show_query", {"query_id": getattr(args, "query_id", "")})}[args.cmd]
        print(ex.execute(e, name, a, None, getattr(args, "rows", 40), 20000))
        _save(path, e, reviews, s)
        return 0
    if args.cmd == "submit":
        f = ex.Finding(args.title, args.post, args.why, [x.strip() for x in args.subjects.split(",") if x.strip()],
                       [x.strip() for x in args.queries.split(",") if x.strip()])
        f.problems = ex.check_finding(f, e)
        e.findings.append(f)
        fid = f"f{len(e.findings)}"
        if f.problems:
            reviews[fid] = {"verdict": "fail", "problems": ["(code check)"]}
            print("REJECTED by the code check:\n- " + "\n- ".join(f.problems))
        else:
            f.problems = ["awaiting review"]
            print(f"PASSED the code check as {fid}; it counts once the reviewer passes it.")
        _save(path, e, reviews, s)
        return 0
    if args.cmd == "pending":
        out = []
        for i, f in enumerate(e.findings):
            if f.problems == ["awaiting review"]:
                out.append({"finding_id": f"f{i + 1}", "review_payload": json.loads(ex.skeptic_payload(f, e, chars=10 ** 6))})
        print(json.dumps(out, ensure_ascii=False, indent=1, default=str) if out else "nothing awaiting review")
        return 0
    if args.cmd == "review":
        i = int(args.finding_id.lstrip("f")) - 1
        f = e.findings[i]
        if f.problems != ["awaiting review"]:
            print(f"{args.finding_id} isn't awaiting review")
            return 1
        f.problems = [] if args.verdict == "pass" else [f"reviewer: {p}" for p in (args.problem or ["failed without a reason"])]
        reviews[args.finding_id] = {"verdict": args.verdict, "problems": args.problem}
        _save(path, e, reviews, s)
        print(f"{args.finding_id}: {args.verdict}")
        return 0
    if args.cmd == "report":
        p = ex.write_explore(e, Path(s.get("out") or "content_queue"), len(e.queries))
        print(f"wrote {p}")
        print(p.read_text(encoding="utf-8"))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
