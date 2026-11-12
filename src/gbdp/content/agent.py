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

from gbdp.utils.logging import get_logger  # noqa: E402

logger = get_logger("gbdp.content")

SYSTEM = """You write short social posts about baseball data from the Global Baseball Data Platform.

Each insight you get was found by a deterministic detector over a season of
reconciled MLB and NPB data. Write one post for each insight (skip one only
if it repeats another post's point). Rules:
- State only numbers that appear in that insight's data (value, baseline,
  delta, sample_size, facts, evidence rows). Never compute a new number, and
  never bring in outside stats or history.
- Rates are fractions in the data: write 0.2227 as 22.3% or 22.27%, and wOBA
  as .521 (three decimals, no leading zero).
- For change_vs_baseline, keep its caveat in spirit: a hot or cold two weeks
  is notable, not a forecast. Don't call it a breakout or a new normal.
- Name the player or league as the data does. Under 280 characters per post,
  no hashtags, at most one emoji.
- The data covers only `coverage.start` to `coverage.end` (in the input). If
  that doesn't start at the season's opening day, never say "season" or "in
  2026": say "since <start date>" instead.
- No superlatives or rankings (longest, best, most, highest, record, first,
  only): nothing here compares a player with the whole league. Trend words
  (rose, fell, jumped, climbed, dropped) only for change_vs_baseline,
  league_month_shift, new_pitch and velocity_change, which compare two
  periods; a cross-league gap, a streak or a luck gap is a single fact.
- luck_gap: wOBA against xwOBA (expected wOBA from exit velocity and launch
  angle). "Lucky" means results ahead of his contact, "unlucky" behind it;
  say the gap tends to close, never that it will.
- new_pitch: the pitch's share of his pitches before and since facts.since,
  with his whiff rate and xwOBA against before and since. Name the pitch in
  words (FF four-seam, SI sinker, FC cutter, SL slider, ST sweeper, CU
  curveball, KC knuckle curve, CH changeup, FS splitter).
- In `claims`, list every headline number the post states, as the data's raw
  fractions or counts: value, and baseline when the post states it.

The why: most MLB player insights carry `facts.drivers`, the Statcast
components that explain the storyline, strongest first. Each has a label,
`before` and `since` values, and a z-score. For a luck_gap, `since` is the
player and `before` is the league (`vs: league`); for new_pitch, a driver with
`vs: his other pitches` compares the new pitch with the rest of his mix. Build
the post around the why - "he's chasing 22% of pitches outside the zone, down
from 35%, and barreling breaking balls" beats "he had a good two weeks".
Use one or two drivers, in plain baseball words, with their numbers. Never
give a reason that isn't in the drivers (no injuries, mechanics, lineup
spots, or anything else the data doesn't show); with no drivers, state the
storyline without a why.

Revisions: an insight with `revise_previous_draft` was drafted before and
rejected by the fact-checker; write that post again, fixing every listed
problem, and keep everything else that was right.

Style: write for a baseball fan scrolling past, not a data dictionary.
- Lead with the player (or the two leagues) and the striking fact; give the
  window in plain words (facts.recent_days, facts.recent_pa: "over his last
  14 days (48 PA)"), the dates of a streak, the months of a league shift.
- Each number once, in its natural form: wOBA as .521 (round to three
  decimals: 0.5208 is .521), rates as a percentage with one decimal (0.0314 is
  3.1%). Never also print the raw fraction.
- One or two sentences. Vary the openings across posts.

Shape: each insight carries `post_format`; write its post in that shape, so a
day's posts don't all read "X posted a .xxx wOBA over his last 14 days":
- number_first: open with the one striking number, then who and why.
- question: open with a short question a fan would ask; answer it with the data.
- contrast: two numbers side by side (then and now, or him and the league).
- under_the_hood: the surface result in a few words, then what's driving it.
- scouting_note: terse, a scout's notebook - the skill or habit first, the result after.
- plain_story: one conversational sentence with the player as its subject.

Examples of the tone (their numbers are made up; yours must come from the data):
- "Jane Doe has been on fire: a .512 wOBA over his last 14 days (51 PA), against .298 before that."
- "Since May 13, MLB hitters have homered on 3.1% of plate appearances; in NPB it's 2.3%." """

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

def coverage(as_of: Optional[str]) -> Dict[str, Any]:
    """The game dates gold holds for the as-of year, so a post can't call a
    partial season a season."""
    from gbdp.serve.db import run_sql

    year = (as_of or date.today().isoformat())[:4]
    rows = run_sql(f"SELECT min(dt) AS start, max(dt) AS end FROM fact_game "
                   f"WHERE dt >= '{year}-01-01' AND dt <= '{as_of or year + '-12-31'}'", limit=1)
    return rows[0] if rows else {"start": None, "end": None}


# The shapes a post can take (SYSTEM describes each). gather hands them out
# so one batch never repeats a shape until it has used them all.
FORMATS = ("number_first", "question", "contrast", "under_the_hood", "scouting_note", "plain_story")


def assign_formats(insights: List[Dict[str, Any]], as_of: Optional[str]) -> None:
    """A shape for each insight: every format once (in a per-day order)
    before any repeats, so neither the day nor the week reads the same."""
    import random

    order = list(FORMATS)
    random.Random(as_of or "").shuffle(order)
    for k, ins in enumerate(insights):
        ins["post_format"] = order[k % len(order)]


def recently_posted(out_dir: Optional[Path], as_of: Optional[str], days: int = 7) -> Set[tuple]:
    """(detector, subject_id) of the posts that passed in the `days` before
    as_of, from the queue's own drafts_<dt>.json files."""
    from datetime import timedelta

    if not out_dir or not as_of or not Path(out_dir).exists():
        return set()
    cutoff = (date.fromisoformat(as_of) - timedelta(days=days)).isoformat()
    seen: Set[tuple] = set()
    for p in Path(out_dir).glob("drafts_*.json"):
        if cutoff <= p.stem[len("drafts_"):] < as_of:
            for d in json.loads(p.read_text(encoding="utf-8")):
                if d.get("ok") and d.get("subject_id"):
                    seen.add((d.get("detector"), d["subject_id"]))
    return seen


def gather(as_of: Optional[str] = None, limit: int = 12, evidence_rows: int = 10,
           skip: Optional[Set[tuple]] = None) -> List[Dict[str, Any]]:
    """The best insights, interleaved across detectors, each with its evidence;
    a (detector, subject) in `skip` (posted this week) makes way for the next."""
    from gbdp import mcp_server as tools

    skip = skip or set()
    found = tools.find_insights(as_of=as_of, limit=limit + 3 * len(skip) if skip else limit)
    if found and "error" in found[0]:
        raise RuntimeError(found[0]["error"])
    found = [i for i in found if (i.get("detector"), i.get("subject_id")) not in skip][:limit]
    out = []
    for ins in found:
        detail = tools.explain_insight(ins["insight_id"], as_of=as_of)
        if "error" in detail:
            continue
        rows = detail.get("evidence_rows") or []
        out.append({k: v for k, v in detail.items() if k not in ("evidence_sql", "evidence_rows")}
                   | {"evidence_rows": rows[:evidence_rows]})
    assign_formats(out, as_of)
    if out:
        out[0]["_coverage"] = coverage(as_of)
    return out


# ----------------------------------------------------------------- draft ---

def _payload(insights: List[Dict[str, Any]]) -> str:
    cov = next((i.get("_coverage") for i in insights if i.get("_coverage")), None)
    body = {"coverage": cov or {"start": None, "end": None},
            "insights": [{("revise_previous_draft" if k == "_revise" else k): v
                          for k, v in i.items() if k != "_coverage"} for i in insights]}
    return json.dumps(body, ensure_ascii=False, default=str, sort_keys=True)

def draft(client, insights: List[Dict[str, Any]], model: str = MODEL) -> List[Draft]:
    """One structured call: a post (with its claims) per insight worth posting."""
    if not insights:
        return []
    payload = _payload(insights)
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


def draft_ollama(insights: List[Dict[str, Any]], model: str = "qwen2.5:3b",
                 host: str = "http://localhost:11434", timeout: float = 900.0) -> List[Draft]:
    """The same drafting step on a local open-source model through Ollama's
    chat API, constrained to the same JSON schema. For testing the pipeline
    (and the verifier) without a Claude API key; small models get rejected
    more often, which is the point of verify()."""
    import requests

    if not insights:
        return []
    payload = _payload(insights)
    r = requests.post(f"{host.rstrip('/')}/api/chat", timeout=timeout, json={
        "model": model,
        "stream": False,
        "format": SCHEMA,
        "options": {"temperature": 0},
        "messages": [{"role": "system", "content": SYSTEM},
                     {"role": "user", "content": f"Insights (JSON):\n{payload}"}],
    })
    r.raise_for_status()
    posts = json.loads(r.json()["message"]["content"]).get("posts", [])
    return [Draft(str(p.get("insight_id", "")), str(p.get("text", "")), list(p.get("claims") or [])) for p in posts]


def _post_with_retry(requests, url, headers, body, timeout, attempts: int = 4, give_up=None):
    """POST, waiting out 429s (free tiers rate-limit tokens per minute) for
    the server's retry-after, or 20 s, up to `attempts` times. give_up(r):
    a 429 not worth waiting for (a daily cap), returned at once."""
    import time

    for i in range(attempts):
        r = requests.post(url, headers=headers, json=body, timeout=timeout)
        if r.status_code != 429 or i == attempts - 1 or (give_up and give_up(r)):
            return r
        wait = float(r.headers.get("retry-after") or 20)
        logger.info("rate limited; retrying in %.0fs", wait)
        time.sleep(min(wait, 120))
    return r


def draft_openai_compat(insights: List[Dict[str, Any]], model: str = "openai/gpt-oss-120b",
                        base_url: str = "https://api.groq.com/openai/v1", api_key_env: str = "GROQ_API_KEY",
                        timeout: float = 300.0) -> List[Draft]:
    """The same drafting step on an open-weights model behind an
    OpenAI-compatible chat API (Groq by default), with the same JSON schema.
    If the server won't take the schema strictly, it falls back to JSON mode
    with the schema in the prompt; verify() guards the output either way."""
    import os

    import requests

    if not insights:
        return []
    key = os.environ.get(api_key_env)
    if not key:
        raise RuntimeError(f"{api_key_env} is not set")
    payload = _payload(insights)
    messages = [{"role": "system", "content": SYSTEM},
                {"role": "user", "content": f"Insights (JSON):\n{payload}"}]
    url = f"{base_url.rstrip('/')}/chat/completions"
    headers = {"Authorization": f"Bearer {key}"}
    body = {"model": model, "temperature": 0, "messages": messages,
            "response_format": {"type": "json_schema", "json_schema": {"name": "posts", "strict": True, "schema": SCHEMA}}}
    r = _post_with_retry(requests, url, headers, body, timeout)
    if r.status_code == 400:
        schema_note = "\n\nAnswer with JSON matching this schema:\n" + json.dumps(SCHEMA)
        body = {"model": model, "temperature": 0, "response_format": {"type": "json_object"},
                "messages": [messages[0], {"role": "user", "content": messages[1]["content"] + schema_note}]}
        r = _post_with_retry(requests, url, headers, body, timeout)
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"]
    posts = json.loads(content).get("posts", [])
    return [Draft(str(p.get("insight_id", "")), str(p.get("text", "")), list(p.get("claims") or [])) for p in posts]


# ---------------------------------------------------------------- verify ---

# 11,432 is one number; .521 and 22.3% are numbers too.
_NUMBER = re.compile(r"(?<![\w.,])(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?|\.\d+)(%?)")


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
    # half a unit either way: 56.25 written as 56.3 (or 56.2) is the same number
    return abs(actual - stated) <= 0.5 * 10 ** -decimals + 1e-9


def text_numbers(text: str) -> List[tuple]:
    """(number, is_percent, as written) for each number in a post."""
    out = []
    for m in _NUMBER.finditer(text):
        raw, pct = m.group(1), m.group(2) == "%"
        out.append((float(raw.replace(",", "")), pct, raw + m.group(2)))
    return out


_SUPERLATIVES = ("longest", "shortest", "best", "worst", "most", "least", "highest", "lowest", "record",
                 "first", "only", "top", "leads", "leading")
_TREND_WORDS = ("rose", "rising", "climbed", "climbing", "jumped", "fell", "falling", "dropped", "surged",
                "plunged", "increased", "decreased", "edging", "up from", "down from")


def _is_detector_fact(claim: Dict[str, Any], insight: Dict[str, Any]) -> bool:
    """A claimed number the detector itself stated in facts (exactly, to 4
    places: facts are rounded there), as opposed to a made-up one."""
    facts = set(_numbers(insight.get("facts") or {}))
    stated = [claim.get("value")] + ([claim["baseline"]] if claim.get("baseline") is not None else [])
    return all(isinstance(v, (int, float)) and any(abs(v - f) < 5e-5 for f in facts) for v in stated)


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
            if not isinstance(c, dict) or "value" not in c:
                d.problems.append(f"malformed claim: {c!r}")
                continue
            if c.get("insight_id") != d.insight_id:
                d.problems.append(f"claim names another insight {c['insight_id']}")
                continue
            check = tools.verify_claim(c["insight_id"], value=c["value"], baseline=c.get("baseline"), as_of=as_of)
            if not check.get("verified") and c.get("baseline") is not None:
                # The headline pair stated the other way round (xwOBA first).
                check = tools.verify_claim(c["insight_id"], value=c["baseline"], baseline=c["value"], as_of=as_of)
            if not check.get("verified") and _is_detector_fact(c, ins):
                continue  # a supporting number from the insight's facts (a whiff rate before/since, ...)
            if not check.get("verified"):
                d.problems.append(f"claim not verified: {json.dumps(check, default=str)}")
        allowed = allowed_numbers(ins)
        # "since May 13": the dates of the coverage window are the data's too.
        cov_window = next((i.get("_coverage") for i in insights if i.get("_coverage")), None) or {}
        allowed |= set(_numbers([cov_window.get("start"), cov_window.get("end")]))
        for number, pct, written in text_numbers(d.text):
            if not any(_rounds_to(a, number) for a in allowed):
                d.problems.append(f"number in text not in the data: {written}")
        low = d.text.lower()
        for word in _SUPERLATIVES:
            if re.search(rf"\b{word}\b", low):
                d.problems.append(f"superlative or ranking the data doesn't support: {word!r}")
        if ins.get("detector") not in ("change_vs_baseline", "league_month_shift", "new_pitch", "velocity_change"):
            for word in _TREND_WORDS:
                if re.search(rf"\b{word}\b", low):
                    d.problems.append(f"trend word for a single fact: {word!r}")
        cov = next((i.get("_coverage") for i in insights if i.get("_coverage")), None) or {}
        start = str(cov.get("start") or "")
        if start and not start.endswith(("-03-25", "-03-26", "-03-27", "-03-28")) and re.search(r"\bseason\b|\bin 20\d\d\b", low):
            d.problems.append(f"says 'season' but the data starts {start}")
        # A league-wide rate needs its window, or a reader takes it for the season.
        partial = start and not start.endswith(("-03-25", "-03-26", "-03-27", "-03-28"))
        if partial and ins.get("detector") == "cross_league_gap" and "since" not in low:
            d.problems.append(f"league-wide rate without its window (data starts {start}: say 'since ...')")
        if len(d.text) > 280:
            d.problems.append(f"too long ({len(d.text)} characters)")
    # The same first three words as an earlier post reads like a template.
    openings: Dict[str, str] = {}
    for d in drafts:
        words = re.findall(r"[a-z']+", d.text.lower())
        key = " ".join(words[:3])
        if len(words) >= 3 and key in openings and openings[key] != d.insight_id:
            d.problems.append(f"opens like another post ({key!r}...); start it differently")
        openings.setdefault(key, d.insight_id)
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
        [{"insight_id": d.insight_id, "text": d.text, "claims": d.claims, "ok": d.ok, "problems": d.problems,
          **{k: by_id.get(d.insight_id, {}).get(k) for k in ("detector", "subject_type", "subject_id", "subject_name",
                                                             "league_code", "post_format")}}
         for d in drafts], ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def run(client, as_of: Optional[str], out_dir: Path, limit: int = 12, model: str = MODEL,
        backend: str = "claude", batch: int = 0) -> Path:
    """backend "claude" (client: an anthropic.Anthropic), "groq" (an
    open-weights model on Groq; GROQ_API_KEY) or "ollama" (a local model);
    client is unused for the last two."""
    insights = gather(as_of, limit, skip=recently_posted(out_dir, as_of))
    as_of = as_of or (str(insights[0].get("dt")) if insights else date.today().isoformat())

    def draft_one(chunk):
        if backend == "ollama":
            return draft_ollama(chunk, model=model)
        if backend == "groq":
            return draft_openai_compat(chunk, model=model)
        return draft(client, chunk, model=model)

    def drafting(items):
        # batch > 0: several smaller calls (free tiers cap request size and
        # tokens per minute); each chunk carries the coverage window.
        size = batch or len(items) or 1
        cov = next((i.get("_coverage") for i in items if i.get("_coverage")), None)
        out: List[Draft] = []
        for k in range(0, len(items), size):
            chunk = [dict(i) for i in items[k:k + size]]
            if cov:
                chunk[0]["_coverage"] = cov
            out += draft_one(chunk)
        return out

    drafts = verify(drafting(insights), insights, as_of=as_of)
    drafts = revise_once(drafts, insights, drafting, as_of)
    return write_queue(drafts, insights, out_dir, as_of)


def revise_once(drafts: List[Draft], insights: List[Dict[str, Any]], drafting, as_of: Optional[str]) -> List[Draft]:
    """One revision pass: each rejected draft goes back with the checker's
    problems, and its rewrite has to pass the same checks. A draft that still
    fails stays rejected, with both rounds' problems."""
    by_id = {i["insight_id"]: i for i in insights}
    rejected = [d for d in drafts if not d.ok and d.insight_id in by_id]
    if not rejected:
        return drafts
    cov = next((i.get("_coverage") for i in insights if i.get("_coverage")), None)
    batch = []
    for d in rejected:
        ins = {k: v for k, v in by_id[d.insight_id].items() if k != "_coverage"}
        ins["_revise"] = {"previous": d.text, "problems": d.problems}
        batch.append(ins)
    if cov:
        batch[0]["_coverage"] = cov
    revised = {r.insight_id: r for r in verify(drafting(batch), batch, as_of=as_of)}
    out = []
    for d in drafts:
        r = revised.get(d.insight_id) if d in rejected else None
        if r is None:
            out.append(d)
        elif r.ok:
            out.append(r)
        else:
            r.problems = [f"first draft: {p}" for p in d.problems] + [f"revision: {p}" for p in r.problems]
            out.append(r)
    return out
