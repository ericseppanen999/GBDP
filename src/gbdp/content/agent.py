"""Content agent: turn insight_candidates into short posts, keeping only the
ones whose every number gold re-derives.

The flow is code-controlled, not model-driven:
  1. gather   - the best insights (find_insights) and their evidence rows
                (explain_insight), straight from the MCP tool functions;
  2. draft    - one Claude call with a JSON schema: posts, each listing the
                numbers it states as claims;
  3. verify   - every claim through verify_claim, and every number in the
                text matched against that insight's own numbers, so a number
                the model invented or mis-rounded can't slip through as prose;
  4. queue    - passing drafts (and the rejected ones, with reasons) written
                for a human to review before anything is published.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

MODEL = "claude-opus-5-5"

SYSTEM = """You write short social posts about baseball data from the Global Baseball Data Platform.

Each insight you get was found by a deterministic detector over a season of
reconciled MLB and NPB data. Write one post per insight you find worth posting
(skip weak or repetitive ones). Rules:
- State only numbers that appear in that insight's data (value, baseline,
  delta, sample_size, facts, evidence rows). Never compute a new number, and
  never bring in outside stats or history.
- Rates are fractions in the data: write 0.2227 as 22.3% or 22.27%, and wOBA
  as .521 (three decimals, no leading zero).
- For change_vs_baseline, keep its caveat in spirit: a hot or cold two weeks
  is notable, not a forecast. Don't call it a breakout or a new normal.
- Name the player or league as the data does. Under 280 characters per post,
  no hashtags, at most one emoji.
- In `claims`, list every headline number the post states, as the data's raw
  fractions or counts: value, and baseline when the post states it."""

SCHEMA = {
    "type": "object",
    "properties": {
        "posts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "insight_id": {"type": "string"},
                    "text": {"type": "string"},
                    "claims": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "insight_id": {"type": "string"},
                                "value": {"type": "number"},
                                "baseline": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                            },
                            "required": ["insight_id", "value", "baseline"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["insight_id", "text", "claims"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["posts"],
    "additionalProperties": False,
}


@dataclass
class Draft:
    insight_id: str
    text: str
    claims: List[Dict[str, Any]]
    problems: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


# ---------------------------------------------------------------- gather ---

def gather(as_of: Optional[str] = None, limit: int = 12, evidence_rows: int = 30) -> List[Dict[str, Any]]:
    """The best insights, interleaved across detectors, each with its evidence."""
    from gbdp import mcp_server as tools

    found = tools.find_insights(as_of=as_of, limit=limit)
    if found and "error" in found[0]:
        raise RuntimeError(found[0]["error"])
    out = []
    for ins in found:
        detail = tools.explain_insight(ins["insight_id"], as_of=as_of)
        if "error" in detail:
            continue
        rows = detail.get("evidence_rows") or []
        out.append({k: v for k, v in detail.items() if k not in ("evidence_sql", "evidence_rows")}
                   | {"evidence_rows": rows[:evidence_rows]})
    return out


# ----------------------------------------------------------------- draft ---

def draft(client, insights: List[Dict[str, Any]], model: str = MODEL) -> List[Draft]:
    """One structured call: a post (with its claims) per insight worth posting."""
    if not insights:
        return []
    payload = json.dumps(insights, ensure_ascii=False, default=str, sort_keys=True)
    response = client.beta.messages.create(
        model=model,
        max_tokens=16000,
        system=SYSTEM,
        output_config={"effort": "medium", "format": {"type": "json_schema", "schema": SCHEMA}},
        # On a policy decline the API reruns the request on a fallback model.
        betas=["server-side-fallback-2026-07-01"],
        extra_body={"fallbacks": "default"},
        messages=[{"role": "user", "content": f"Insights (JSON):\n{payload}"}],
    )
    if response.stop_reason == "refusal":
        raise RuntimeError(f"declined: {getattr(response, 'stop_details', None)}")
    if response.stop_reason == "max_tokens":
        raise RuntimeError("draft hit max_tokens; ask for fewer insights")
    text = next(b.text for b in response.content if b.type == "text")
    posts = json.loads(text)["posts"]
    return [Draft(p["insight_id"], p["text"], p["claims"]) for p in posts]


# ---------------------------------------------------------------- verify ---

_NUMBER = re.compile(r"(?<![\w.])(\d+(?:\.\d+)?|\.\d+)(%?)")


def _numbers(value: Any) -> Iterable[float]:
    if isinstance(value, bool):
        return
    if isinstance(value, (int, float)):
        yield float(value)
    elif isinstance(value, str):
        try:
            yield float(value)
        except ValueError:
            # dates and other strings: their parts (2026-08-17 -> 2026, 8, 17)
            for part in re.findall(r"\d+(?:\.\d+)?", value):
                yield float(part)
    elif isinstance(value, dict):
        for v in value.values():
            yield from _numbers(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            yield from _numbers(v)


def allowed_numbers(insight: Dict[str, Any]) -> Set[float]:
    """Every number the insight's data states, in the forms a post may write it:
    as is, as a percentage of a fraction, and the delta's absolute size."""
    raw = set(_numbers({k: insight.get(k) for k in ("value", "baseline", "delta", "sample_size", "facts",
                                                  "evidence_rows", "rank")}))
    out: Set[float] = set()
    for x in raw:
        out |= {x, abs(x), x * 100, abs(x) * 100}
    return out


def _rounds_to(actual: float, stated: float) -> bool:
    """A data number, written to the stated number's precision, is that number
    (22.3 for 22.269, .521 for 0.5208, 14 for 14.0)."""
    s = f"{stated}".rstrip("0").rstrip(".") if "." in f"{stated}" else f"{stated}"
    decimals = len(s.split(".")[1]) if "." in s else 0
    return round(actual, decimals) == round(stated, decimals) or abs(actual - stated) < 1e-9


def text_numbers(text: str) -> List[tuple]:
    """(number, is_percent, as written) for each number in a post."""
    out = []
    for m in _NUMBER.finditer(text):
        raw, pct = m.group(1), m.group(2) == "%"
        out.append((float(raw), pct, raw + m.group(2)))
    return out


def verify(drafts: List[Draft], insights: List[Dict[str, Any]], as_of: Optional[str] = None) -> List[Draft]:
    """Mark each draft's problems: an unknown insight, a claim gold doesn't
    re-derive, or a number in the text that isn't one of the insight's own."""
    from gbdp import mcp_server as tools

    by_id = {i["insight_id"]: i for i in insights}
    for d in drafts:
        ins = by_id.get(d.insight_id)
        if ins is None:
            d.problems.append(f"unknown insight {d.insight_id}")
            continue
        if not d.claims:
            d.problems.append("no claims listed")
        for c in d.claims:
            if c["insight_id"] != d.insight_id:
                d.problems.append(f"claim names another insight {c['insight_id']}")
                continue
            check = tools.verify_claim(c["insight_id"], value=c["value"], baseline=c.get("baseline"), as_of=as_of)
            if not check.get("verified"):
                d.problems.append(f"claim not verified: {json.dumps(check, default=str)}")
        allowed = allowed_numbers(ins)
        for number, pct, written in text_numbers(d.text):
            if not any(_rounds_to(a, number) for a in allowed):
                d.problems.append(f"number in text not in the data: {written}")
        if len(d.text) > 280:
            d.problems.append(f"too long ({len(d.text)} characters)")
    return drafts


# ----------------------------------------------------------------- queue ---

def write_queue(drafts: List[Draft], insights: List[Dict[str, Any]], out_dir: Path, as_of: str) -> Path:
    """Markdown for a human to review: passing drafts first, rejects with reasons."""
    out_dir.mkdir(parents=True, exist_ok=True)
    by_id = {i["insight_id"]: i for i in insights}
    lines = [f"# Content drafts as of {as_of}", "",
             f"{sum(d.ok for d in drafts)} ready for review, {sum(not d.ok for d in drafts)} rejected.", ""]
    for title, keep in (("Ready for review", True), ("Rejected", False)):
        group = [d for d in drafts if d.ok is keep]
        if not group:
            continue
        lines += [f"## {title}", ""]
        for d in group:
            ins = by_id.get(d.insight_id, {})
            lines += [f"### {ins.get('detector', '?')}: {ins.get('subject_name', '?')} ({ins.get('league_code', '?')})", "",
                      f"> {d.text}", "",
                      f"- insight `{d.insight_id}`, score {ins.get('score')}",
                      f"- claims: {json.dumps(d.claims)}"]
            lines += [f"- problem: {p}" for p in d.problems]
            lines.append("")
    path = out_dir / f"drafts_{as_of}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    (out_dir / f"drafts_{as_of}.json").write_text(json.dumps(
        [{"insight_id": d.insight_id, "text": d.text, "claims": d.claims, "ok": d.ok, "problems": d.problems}
         for d in drafts], ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def run(client, as_of: Optional[str], out_dir: Path, limit: int = 12, model: str = MODEL) -> Path:
    insights = gather(as_of, limit)
    as_of = as_of or (str(insights[0].get("dt")) if insights else date.today().isoformat())
    drafts = verify(draft(client, insights, model=model), insights, as_of=as_of)
    return write_queue(drafts, insights, out_dir, as_of)
