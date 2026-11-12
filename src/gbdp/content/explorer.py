"""Explorer: a model with read-only SQL over gold, looking for its own stories.

The content agent (agent.py) writes posts from what the deterministic
detectors found. The explorer is the open-ended half: a model gets the
semantic layer's tables and a query tool, a lens to start from (pitch
arsenals, team quirks, MLB against NPB, ...), the stories posted lately (to
go somewhere else), and a budget of tool calls, and goes looking.

It stays honest the same way the agent does, in code, not in the prompt:
  - every query it runs is kept by id; a finding cites the queries behind it;
  - submit_finding re-runs those queries and rejects the finding unless the
    results come back the same, every number in the post is in them (or is a
    literal in their SQL, like a 100-PA minimum), every player or team the
    post names is in them, and the post follows the house rules (length, no
    unsupported rankings, no "season" for a partial one);
  - a separate skeptic call reads the finding, its SQL and its rows and
    rejects it if the SQL doesn't measure what the post says, the sample is
    thin, the window is cherry-picked, a join double counts, or the post
    claims a cause the data doesn't show.
A rejection goes back to the explorer as the tool's answer, so it can fix the
finding or move on. Accepted findings are written for a human to review;
nothing is published.

Backends: "claude" (the Anthropic SDK) or "groq" (an open-weights model behind
an OpenAI-compatible API). Old query results shrink to a one-line stub in the
context (show_query brings one back), which keeps each request small enough
for a free tier's tokens-per-minute cap and the Claude bill down.
"""
from __future__ import annotations

import json
import os
import random
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from gbdp.content import agent
from gbdp.utils.logging import get_logger

logger = get_logger("gbdp.explorer")

# Where to start looking. One is picked per run (or named with --lens), so
# runs don't all circle the same few stories.
LENSES = {
    "arsenal": "one pitch: a pitcher's pitch that's unhittable or getting crushed, how its velocity, "
               "movement or location shapes its whiff rate, or what a batter does against one pitch type",
    "batted_ball": "batted-ball profiles: pull-side air contact, launch angles, exit velocity, who hits the ball "
                   "hard without results (or soft with them)",
    "discipline": "plate discipline: who chases, who never swings at strike one, counts where a hitter or a "
                  "pitcher changes, two-strike approaches",
    "situational": "situations: run expectancy (RE24), runners in scoring position, late and close innings, "
                   "the first time through the order against the third",
    "teams": "teams: run scoring by inning, home against away, bullpen against rotation, one-run games, "
             "a team that wins or loses in a way the others don't",
    "cross_league": "MLB against NPB: how the game differs (strikeouts, walks, home runs, stolen bases, "
                    "sacrifice bunts, pitch counts), or a player who played in both",
    "surprise": "anything: look for the pattern nobody would think to check, and check it",
}

SYSTEM = """You are a baseball analyst with read-only SQL access to the Global Baseball Data Platform: reconciled MLB and NPB data (games, plate appearances, MLB Statcast pitches, run expectancy, daily player features). Your job is to find stories a sharp fan would want to read and write each as a short social post.

How to work:
- Start with list_tables and describe_table (columns, grain and caveats matter: read them before querying an unfamiliar table). describe_metric gives the house formulas (wOBA and friends).
- Explore with query. Each result has a query_id; older results shrink to a one-line stub in your context, and show_query brings one back.
- Look for what's surprising, specific and explainable: not "X is good" but what he does that makes it so (a pitch, a count, a location, a batted-ball shape), or a team or league pattern nobody would guess. Compare against a baseline (the league, his own past, other teams) so the number means something.
- When a finding holds up, call submit_finding with the query_ids that prove it. Then look for a different kind of story about a different subject.

The fact-checker (code) re-runs the cited queries and rejects a finding unless:
- every number in the post appears in their results, or is a literal in their SQL (a 100-PA minimum, a date); compute ratios, rates and differences in SQL, not in your head. Rates in results are fractions; the post may write 0.312 as 31.2%. wOBA-style numbers as .312.
- every player and team the post names appears in the results (join v_player_current or dim_team for names);
- results come back the same on a re-run: give any query with LIMIT an ORDER BY with a tiebreak;
- the post is at most 280 characters, no hashtags, at most one emoji;
- ranking words (best, most, leads, highest, only, first, record) only when a cited query ranks the whole population it names, and the post names it ("among MLB hitters with 200+ PA");
- "season" or "in <year>" only if the data starts at opening day; otherwise "since <date>".
A skeptical reviewer then reads your SQL and rows and rejects findings where the SQL doesn't measure what the post says, the sample is thin (say the PA, pitch or game count; under ~50 PA or ~100 pitches is noise), the window looks cherry-picked, a join double counts, or the post claims a cause the data doesn't show. Never bring in outside knowledge (injuries, contracts, history, other seasons not in the data).
If a submission is rejected, fix what it says and resubmit, or move on.

Style: write for a fan scrolling past. Lead with the subject and the striking fact, then the why or the comparison, in plain baseball words. One to three sentences. Each finding should read differently from the last: vary the openings and the shape (a question, a contrast, a number first, a scouting note)."""

TOOL_SPECS = [
    ("list_tables", "Every queryable gold table and view, with a one-line description.", {}),
    ("describe_table", "One table or view in full: grain, keys, columns and caveats.",
     {"table": {"type": "string"}}),
    ("list_metrics", "The semantic layer's governed metrics (wOBA, OBP, K%, ...).", {}),
    ("describe_metric", "One metric: its formula, view and dimensions.", {"name": {"type": "string"}}),
    ("query", "Run a read-only SQL query (SELECT or WITH ... SELECT) over gold. Tables and views by bare "
              "name. Returns a query_id, the row count and the first rows.",
     {"sql": {"type": "string"}, "purpose": {"type": "string", "description": "What this query checks, in a few words."}}),
    ("show_query", "Show a query you ran earlier again, by query_id.", {"query_id": {"type": "string"}}),
    ("submit_finding", "Submit one story for fact-checking. The answer says accepted, or what to fix.",
     {"title": {"type": "string", "description": "A few words, for the review queue."},
      "post": {"type": "string", "description": "The post, at most 280 characters."},
      "why": {"type": "string", "description": "For the reviewer: what the queries show and why it's not noise."},
      "subjects": {"type": "array", "items": {"type": "string"},
                   "description": "The players, teams or leagues the post is about, as named in the results."},
      "query_ids": {"type": "array", "items": {"type": "string"}}}),
    ("finish", "Stop exploring.", {"summary": {"type": "string"}}),
]
_REQUIRED = {"describe_table": ["table"], "describe_metric": ["name"], "query": ["sql", "purpose"],
             "show_query": ["query_id"], "submit_finding": ["title", "post", "why", "subjects", "query_ids"],
             "finish": ["summary"]}


def _schema(props: Dict[str, Any], name: str) -> Dict[str, Any]:
    return {"type": "object", "properties": props, "required": _REQUIRED.get(name, []), "additionalProperties": False}


def anthropic_tools() -> List[Dict[str, Any]]:
    return [{"name": n, "description": d, "input_schema": _schema(p, n)} for n, d, p in TOOL_SPECS]


def openai_tools() -> List[Dict[str, Any]]:
    return [{"type": "function", "function": {"name": n, "description": d, "parameters": _schema(p, n)}}
            for n, d, p in TOOL_SPECS]


SKEPTIC = """You review a baseball data finding before it's posted. You get the post, the analyst's reasoning, and the SQL and result rows behind it. A code check has already confirmed every number in the post appears in the rows; your job is whether the rows mean what the post says.

Reject (verdict "fail") when any of these holds, and say exactly which and why:
- the SQL doesn't measure what the post claims (wrong denominator, wrong filter, a rate computed over the wrong events, a "since" date the SQL doesn't use, a comparison against the wrong group);
- a join can double count rows (one-to-many without dedup) or drops rows the claim needs;
- the sample is too small to mean anything (under ~50 PA, ~100 pitches, ~10 games for a rate), or the post hides its sample size;
- the window or threshold looks chosen to make the number (an odd start date, a cutoff just above one player);
- a ranking or "only" isn't backed by a query over the whole population the post names;
- the post claims a cause, a trend or a forecast the rows don't show, or uses outside knowledge;
- the post says "season" or "in <year>" though coverage starts after opening day.
Otherwise "pass". Small style quibbles are not reasons to fail."""

SKEPTIC_SCHEMA = {
    "type": "object",
    "properties": {"verdict": {"type": "string", "enum": ["pass", "fail"]},
                   "problems": {"type": "array", "items": {"type": "string"}}},
    "required": ["verdict", "problems"],
    "additionalProperties": False,
}


# ------------------------------------------------------------- the state ---

@dataclass
class Query:
    query_id: str
    sql: str
    purpose: str
    rows: List[Dict[str, Any]]


@dataclass
class Finding:
    title: str
    post: str
    why: str
    subjects: List[str]
    query_ids: List[str]
    problems: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


@dataclass
class Exploration:
    as_of: str
    coverage: Dict[str, Any]
    lens: str
    avoid: List[str]
    run_sql: Callable[[str], List[Dict[str, Any]]]
    queries: Dict[str, Query] = field(default_factory=dict)
    findings: List[Finding] = field(default_factory=list)
    finished: bool = False
    stopped: str = ""
    spent: str = ""

    @property
    def accepted(self) -> List[Finding]:
        return [f for f in self.findings if f.ok]


def _jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        o = {}
        for k, v in r.items():
            if isinstance(v, float):
                o[k] = round(v, 6)
            elif v is None or isinstance(v, (int, str, bool)):
                o[k] = v
            else:
                o[k] = str(v)
        out.append(o)
    return out


def _clip(text: str, chars: int) -> str:
    return text if len(text) <= chars else text[:chars] + f" ... [{len(text) - chars} more characters]"


# ----------------------------------------------------------- the checker ---

_RANKING = ("best", "worst", "most", "least", "highest", "lowest", "leads", "leading", "led", "only", "first",
            "record", "top", "fewest")
_SQL_NUMBER = re.compile(r"(?<![\w.])(\d+(?:\.\d+)?|\.\d+)")


def allowed_numbers(queries: List[Query], coverage: Dict[str, Any]) -> List[float]:
    """Every number a post citing these queries may state: the values in their
    rows (and dates' parts), the literals in their SQL, the coverage dates, and
    each as a percentage of a fraction."""
    raw: List[float] = []
    for q in queries:
        for r in q.rows:
            raw += list(agent._numbers(list(r.values())))
        raw += [float(x) for x in _SQL_NUMBER.findall(re.sub(r"'[^']*'", lambda m: m.group(0).replace("-", " "), q.sql))]
    raw += list(agent._numbers([coverage.get("start"), coverage.get("end")]))
    out = set()
    for x in raw:
        out |= {x, abs(x), x * 100, abs(x) * 100}
    return sorted(out)


def _strings(queries: List[Query]) -> str:
    return " | ".join(str(v) for q in queries for r in q.rows for v in r.values() if isinstance(v, str)).lower()


def check_finding(f: Finding, ex: Exploration) -> List[str]:
    """The mechanical checks; [] when the finding passes them."""
    problems: List[str] = []
    unknown = [i for i in f.query_ids if i not in ex.queries]
    if not f.query_ids:
        problems.append("cite the query_ids that prove the post")
    if unknown:
        problems.append(f"unknown query_ids: {unknown}")
    cited = [ex.queries[i] for i in f.query_ids if i in ex.queries]
    for q in cited:
        try:
            again = _jsonable(ex.run_sql(q.sql))
        except Exception as exc:  # the data moved, or the query was never valid
            problems.append(f"{q.query_id} fails on a re-run: {exc}")
            continue
        if again != q.rows:
            problems.append(f"{q.query_id} returns different rows on a re-run; give its LIMIT an ORDER BY with a tiebreak")
    text = f.post
    if len(text) > 280:
        problems.append(f"post is {len(text)} characters; the limit is 280")
    if "#" in text:
        problems.append("no hashtags")
    allowed = allowed_numbers(cited, ex.coverage)
    for number, _pct, written in agent.text_numbers(text):
        if not any(agent._rounds_to(a, number) for a in allowed):
            problems.append(f"number not in the cited results or their SQL: {written}")
    names = _strings(cited)
    for s in f.subjects:
        if s.strip() and s.strip().lower() not in names:
            problems.append(f"subject {s!r} isn't in the cited results (join v_player_current or dim_team for names)")
        if s.strip() and s.strip().lower() not in text.lower():
            problems.append(f"subject {s!r} isn't named in the post")
    low = text.lower()
    ranks = [w for w in _RANKING if re.search(rf"\b{w}\b", low)]
    if ranks and not any(re.search(r"\border\s+by\b", q.sql, re.IGNORECASE) for q in cited):
        problems.append(f"ranking word {ranks} needs a cited query that ranks the whole population (ORDER BY)")
    start = str(ex.coverage.get("start") or "")
    if start and not start.endswith(("-03-25", "-03-26", "-03-27", "-03-28")) and \
            re.search(r"\bseason\b|\bin 20\d\d\b", low):
        problems.append(f"says 'season' but the data starts {start}; say 'since {start}'")
    taken = {s.lower() for g in ex.accepted for s in g.subjects} | {a.lower() for a in ex.avoid}
    repeats = [s for s in f.subjects if s.lower() in taken and s.lower() not in ("mlb", "npb", "kbo")]
    if repeats:
        problems.append(f"already covered: {repeats}; find a different subject")
    return problems


def skeptic_payload(f: Finding, ex: Exploration, rows_per_query: int = 25, chars: int = 6000) -> str:
    cited = [ex.queries[i] for i in f.query_ids if i in ex.queries]
    body = {"coverage": ex.coverage, "post": f.post, "analyst_reasoning": f.why,
            "queries": [{"query_id": q.query_id, "purpose": q.purpose, "sql": q.sql, "row_count": len(q.rows),
                         "rows": q.rows[:rows_per_query]} for q in cited]}
    return _clip(json.dumps(body, ensure_ascii=False, default=str), chars)


# ------------------------------------------------------------- the tools ---

def execute(ex: Exploration, name: str, args: Dict[str, Any], skeptic: Callable[[str], Dict[str, Any]],
            rows_shown: int, chars: int) -> str:
    from gbdp import mcp_server as tools

    if name == "list_tables":
        return json.dumps(tools.list_tables())
    if name == "describe_table":
        return _clip(json.dumps(tools.describe_table(str(args.get("table", ""))), default=str), chars)
    if name == "list_metrics":
        return _clip(json.dumps(tools.list_metrics(), default=str), chars)
    if name == "describe_metric":
        return _clip(json.dumps(tools.describe_metric(str(args.get("name", ""))), default=str), chars)
    if name == "query":
        sql = str(args.get("sql", ""))
        try:
            rows = _jsonable(ex.run_sql(sql))
        except Exception as exc:
            return f"ERROR: {_clip(str(exc), 800)}"
        qid = f"q{len(ex.queries) + 1}"
        ex.queries[qid] = Query(qid, sql, str(args.get("purpose", "")), rows)
        return _show(ex.queries[qid], rows_shown, chars)
    if name == "show_query":
        q = ex.queries.get(str(args.get("query_id", "")))
        return _show(q, rows_shown, chars) if q else "ERROR: no such query_id"
    if name == "submit_finding":
        f = Finding(str(args.get("title", "")), str(args.get("post", "")), str(args.get("why", "")),
                    [str(s) for s in args.get("subjects") or []], [str(i) for i in args.get("query_ids") or []])
        f.problems = check_finding(f, ex)
        if not f.problems:
            try:
                review = skeptic(skeptic_payload(f, ex))
            except Exception as exc:
                review = {"verdict": "fail", "problems": [f"reviewer failed: {exc}"]}
            if review.get("verdict") != "pass":
                f.problems = [f"reviewer: {p}" for p in review.get("problems") or ["failed without a reason"]]
        ex.findings.append(f)
        if f.ok:
            return f"ACCEPTED ({len(ex.accepted)} so far). Now look for a different kind of story about a different subject."
        return "REJECTED:\n- " + "\n- ".join(f.problems) + "\nFix and resubmit, or move on."
    if name == "finish":
        ex.finished = True
        return "ok"
    return f"ERROR: unknown tool {name}"


def _show(q: Query, rows_shown: int, chars: int) -> str:
    body = {"query_id": q.query_id, "row_count": len(q.rows),
            "columns": list(q.rows[0].keys()) if q.rows else [], "rows": q.rows[:rows_shown]}
    if len(q.rows) > rows_shown:
        body["note"] = f"showing {rows_shown} of {len(q.rows)} rows"
    return _clip(json.dumps(body, ensure_ascii=False, default=str), chars)


def _stub(name: str, args: Dict[str, Any], result: str) -> str:
    """What an old tool result shrinks to in the context."""
    if name in ("query", "show_query"):
        m = re.search(r'"query_id": "(q\d+)", "row_count": (\d+)', result)
        if m:
            return f"[{m.group(1)}: {m.group(2)} rows; {str(args.get('purpose', ''))[:80]} - show_query to see it again]"
    if name == "submit_finding":
        return result.split("\n")[0][:300]
    return f"[{name} result, shown earlier]"


# ---------------------------------------------------------- the backends ---

class BudgetSpent(RuntimeError):
    """The run's token budget is spent; nothing more is sent."""


class TokenMeter:
    """A hard cap on the tokens one run may spend, counted from the API's own
    usage numbers. Before each call it reserves a deliberately high estimate
    of the request (a token per 3 characters, where English runs nearer 4)
    plus the call's whole output allowance, and refuses the call if that
    could pass the budget, so a run can't go over it, even by one call."""

    def __init__(self, budget: int):
        self.budget, self.spent, self.calls = int(budget), 0, 0

    def reserve(self, request: Any, max_output: int) -> None:
        estimate = len(json.dumps(request, ensure_ascii=False, default=str)) // 3 + max_output
        if self.spent + estimate > self.budget:
            raise BudgetSpent(f"token budget spent: {self.spent:,} of {self.budget:,} used, "
                              f"the next call could take up to {estimate:,}")

    def add(self, input_tokens: int, output_tokens: int) -> None:
        self.spent += int(input_tokens or 0) + int(output_tokens or 0)
        self.calls += 1


def _claude_usage(response) -> tuple:
    u = getattr(response, "usage", None)
    if u is None:
        return 0, 0
    read = sum(int(getattr(u, k, 0) or 0) for k in ("input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"))
    return read, int(getattr(u, "output_tokens", 0) or 0)


class ClaudeBackend:
    """The Anthropic SDK, with adaptive thinking. Assistant turns go back
    verbatim (thinking blocks included); only tool results get trimmed."""

    def __init__(self, client, model: str = agent.MODEL, max_tokens: int = 16000, budget: int = 400_000):
        self.client, self.model, self.max_tokens = client, model, max_tokens
        self.meter = TokenMeter(budget)

    def step(self, system: str, first: str, turns: List[Dict[str, Any]]):
        messages: List[Dict[str, Any]] = [{"role": "user", "content": first}]
        for t in turns:
            if t["kind"] == "assistant":
                messages.append({"role": "assistant", "content": t["raw"]})
            elif t["kind"] == "results":
                messages.append({"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": r["id"], "content": r["shown"]} for r in t["results"]]})
            else:
                messages.append({"role": "user", "content": t["text"]})
        self.meter.reserve([system, anthropic_tools(), messages], self.max_tokens)
        response = self.client.messages.create(model=self.model, max_tokens=self.max_tokens, system=system,
                                               tools=anthropic_tools(), thinking={"type": "adaptive"},
                                               messages=messages)
        self.meter.add(*_claude_usage(response))
        if response.stop_reason == "refusal":
            raise RuntimeError("the model declined")
        calls = [{"id": b.id, "name": b.name, "args": dict(b.input)} for b in response.content if b.type == "tool_use"]
        text = " ".join(b.text for b in response.content if b.type == "text")
        return response.content, calls, text

    def json_call(self, system: str, user: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        self.meter.reserve([system, user, schema], 4000)
        response = self.client.beta.messages.create(
            model=self.model, max_tokens=4000, system=system,
            output_config={"effort": "medium", "format": {"type": "json_schema", "schema": schema}},
            messages=[{"role": "user", "content": user}])
        self.meter.add(*_claude_usage(response))
        if response.stop_reason == "refusal":
            raise RuntimeError("the reviewer declined")
        return json.loads(next(b.text for b in response.content if b.type == "text"))


class OpenAICompatBackend:
    """Tool calling on an OpenAI-compatible chat API (Groq by default)."""

    def __init__(self, model: str = "openai/gpt-oss-120b", base_url: str = "https://api.groq.com/openai/v1",
                 api_key_env: str = "GROQ_API_KEY", max_tokens: int = 2500, timeout: float = 300.0,
                 max_chars: Optional[int] = None, budget: int = 400_000):
        key = os.environ.get(api_key_env)
        if not key:
            raise RuntimeError(f"{api_key_env} is not set")
        self.model, self.url, self.max_tokens, self.timeout = model, f"{base_url.rstrip('/')}/chat/completions", max_tokens, timeout
        self.headers = {"Authorization": f"Bearer {key}"}
        # A request-size cap (characters of messages and tools, ~4 per
        # token): Groq's free tier refuses a request over ~7k input tokens.
        self.max_chars = max_chars
        self.meter = TokenMeter(budget)

    def _post(self, body: Dict[str, Any]) -> Dict[str, Any]:
        import requests

        self.meter.reserve(body, int(body.get("max_tokens") or 0))
        r = agent._post_with_retry(requests, self.url, self.headers, body, self.timeout, attempts=8,
                                   give_up=lambda resp: "per day" in resp.text)
        if r.status_code >= 400:
            raise RuntimeError(f"{r.status_code}: {r.text[:400]}")
        out = r.json()
        usage = out.get("usage") or {}
        self.meter.add(usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0))
        return out

    def step(self, system: str, first: str, turns: List[Dict[str, Any]]):
        messages: List[Dict[str, Any]] = [{"role": "system", "content": system}, {"role": "user", "content": first}]
        for t in turns:
            if t["kind"] == "assistant":
                messages.append(t["raw"])
            elif t["kind"] == "results":
                messages += [{"role": "tool", "tool_call_id": r["id"], "content": r["shown"]} for r in t["results"]]
            else:
                messages.append({"role": "user", "content": t["text"]})
        if self.max_chars:
            compact(messages, self.max_chars - len(json.dumps(openai_tools())))
        msg = self._post({"model": self.model, "messages": messages, "tools": openai_tools(), "tool_choice": "auto",
                          "temperature": 0.7, "max_tokens": self.max_tokens})["choices"][0]["message"]
        calls = []
        for c in msg.get("tool_calls") or []:
            try:
                args = json.loads(c["function"].get("arguments") or "{}")
            except json.JSONDecodeError:
                args = {"_bad_json": c["function"].get("arguments")}
            calls.append({"id": c["id"], "name": c["function"]["name"], "args": args})
        raw = {"role": "assistant", "content": msg.get("content") or ""}
        if msg.get("tool_calls"):
            raw["tool_calls"] = [{"id": c["id"], "type": "function", "function": {
                "name": c["function"]["name"], "arguments": c["function"].get("arguments") or "{}"}} for c in msg["tool_calls"]]
        return raw, calls, msg.get("content") or ""

    def json_call(self, system: str, user: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        body = {"model": self.model, "temperature": 0, "max_tokens": 1500,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "response_format": {"type": "json_schema", "json_schema": {"name": "review", "strict": True, "schema": schema}}}
        try:
            out = self._post(body)
        except RuntimeError as exc:
            if not str(exc).startswith("400"):
                raise
            body["response_format"] = {"type": "json_object"}
            body["messages"][1]["content"] += "\n\nAnswer with JSON matching this schema:\n" + json.dumps(schema)
            out = self._post(body)
        return json.loads(out["choices"][0]["message"]["content"])


def compact(messages: List[Dict[str, Any]], budget: int) -> None:
    """Shrink an OpenAI-format conversation under `budget` characters, oldest
    first: an assistant turn's text, then its calls' long arguments (a query's
    SQL becomes a pointer; show_query has the rows), then tool results to
    their first line. The system prompt, the task and the newest exchange stay."""
    def size() -> int:
        return len(json.dumps(messages, ensure_ascii=False))

    last_assistant = max((i for i, m in enumerate(messages) if m.get("role") == "assistant"), default=-1)
    for i, m in enumerate(messages[:last_assistant]):
        if size() <= budget:
            return
        if m.get("role") == "assistant":
            m["content"] = (m.get("content") or "")[:150]
            for c in m.get("tool_calls") or []:
                try:
                    args = json.loads(c["function"]["arguments"] or "{}")
                except json.JSONDecodeError:
                    args = {}
                c["function"]["arguments"] = json.dumps(
                    {k: (v[:100] + " ..." if isinstance(v, str) and len(v) > 100 else v) for k, v in args.items()})
        elif m.get("role") == "tool":
            m["content"] = str(m.get("content") or "").split("\n")[0][:200]


# --------------------------------------------------------------- the run ---

def recent_subjects(out_dir: Path, as_of: str, days: int = 14) -> List[str]:
    """Players, teams and titles the explorer and the agent covered lately."""
    cutoff = (date.fromisoformat(as_of) - timedelta(days=days)).isoformat()
    seen: List[str] = []
    for p in sorted(out_dir.glob("explore_*.json")):
        if cutoff <= p.stem[len("explore_"):] < as_of:
            for f in json.loads(p.read_text(encoding="utf-8")).get("accepted", []):
                seen += f.get("subjects", [])
    for p in sorted(out_dir.glob("drafts_*.json")):
        if cutoff <= p.stem[len("drafts_"):] < as_of:
            seen += [d.get("subject_name") for d in json.loads(p.read_text(encoding="utf-8"))
                     if d.get("ok") and d.get("subject_name") and d.get("subject_type") == "player"]
    return sorted({s for s in seen if s})


def explore(backend, as_of: Optional[str] = None, out_dir: Path = Path("content_queue"), lens: Optional[str] = None,
            findings: int = 3, max_steps: int = 40, keep_full: int = 4, rows_shown: int = 30, chars: int = 6000,
            seed: Optional[int] = None, run_sql: Optional[Callable[[str], List[Dict[str, Any]]]] = None) -> Path:
    """Explore until `findings` are accepted, the model finishes, or
    `max_steps` tool calls are spent; write explore_<as_of>.md (+ .json)."""
    from gbdp.serve.db import run_sql as gold_sql

    sql = run_sql or (lambda s: gold_sql(s, limit=1000))
    cov = agent.coverage(as_of)
    as_of = as_of or str(cov.get("end") or date.today().isoformat())
    lens = lens or random.Random(seed).choice(sorted(LENSES))
    if lens not in LENSES:
        raise ValueError(f"unknown lens {lens!r}; one of {sorted(LENSES)}")
    ex = Exploration(as_of=as_of, coverage=cov, lens=lens, avoid=recent_subjects(out_dir, as_of), run_sql=sql)
    first = (f"Data covers {cov.get('start')} to {cov.get('end')}; treat {as_of} as today.\n"
             f"Start from this lens: {LENSES[lens]}. Follow the data wherever it gets interesting.\n"
             f"Find {findings} stories, each about a different subject and a different kind of pattern.\n"
             + (f"Recently covered, so pick other subjects: {', '.join(ex.avoid)}.\n" if ex.avoid else "")
             + f"You have {max_steps} tool calls.")

    def skeptic(payload: str) -> Dict[str, Any]:
        return backend.json_call(SKEPTIC, payload, SKEPTIC_SCHEMA)

    turns: List[Dict[str, Any]] = []
    out_dir.mkdir(parents=True, exist_ok=True)
    log = (out_dir / f"explore_{as_of}.steps.jsonl").open("w", encoding="utf-8")
    try:
        steps = _loop(backend, ex, first, turns, skeptic, findings, max_steps, keep_full, rows_shown, chars, log)
    except Exception as exc:  # a rate limit, an API error: keep what was found
        ex.stopped = f"stopped early: {_clip(str(exc), 300)}"
        logger.warning("explorer %s", ex.stopped)
        steps = sum(len(t["results"]) for t in turns if t["kind"] == "results")
    finally:
        log.close()
    meter = getattr(backend, "meter", None)
    if meter:
        ex.spent = f"{meter.spent:,} of {meter.budget:,} tokens in {meter.calls} calls"
    return write_explore(ex, out_dir, steps)


def _loop(backend, ex: Exploration, first: str, turns: List[Dict[str, Any]], skeptic, findings: int, max_steps: int,
          keep_full: int, rows_shown: int, chars: int, log) -> int:
    steps = 0
    idle = 0
    while steps < max_steps and len(ex.accepted) < findings and not ex.finished:
        _trim(turns, keep_full)
        raw, calls, text = backend.step(SYSTEM, first, turns)
        turns.append({"kind": "assistant", "raw": raw})
        if not calls:
            idle += 1
            if idle >= 3:
                break
            turns.append({"kind": "nudge", "text": "Keep going: run a query, submit a finding, or call finish."})
            continue
        idle = 0
        results = []
        for c in calls:
            steps += 1
            if "_bad_json" in c["args"]:
                out = "ERROR: the arguments weren't valid JSON"
            else:
                out = execute(ex, c["name"], c["args"], skeptic, rows_shown, chars)
            left = max_steps - steps
            if left <= 5:
                out += f"\n[{left} tool calls left: submit what holds up, or finish]"
            elif steps >= max_steps // 2 and not ex.findings:
                out += (f"\n[{left} tool calls left and nothing submitted: stop surveying, pick the most interesting "
                        f"thing you've seen, query it properly and submit it]")
            logger.info("step %d %s -> %s", steps, c["name"], out.split("\n")[0][:120])
            log.write(json.dumps({"step": steps, "tool": c["name"], "args": c["args"], "result": out[:2000]},
                                 ensure_ascii=False, default=str) + "\n")
            log.flush()
            results.append({"id": c["id"], "name": c["name"], "args": c["args"], "full": out, "shown": out,
                            "stub": _stub(c["name"], c["args"], out)})
        turns.append({"kind": "results", "results": results})
    return steps


def _trim(turns: List[Dict[str, Any]], keep_full: int) -> None:
    """Every tool result but the newest `keep_full` shows only its stub."""
    results = [r for t in turns if t["kind"] == "results" for r in t["results"]]
    for i, r in enumerate(results):
        r["shown"] = r["full"] if i >= len(results) - keep_full else r["stub"]


def write_explore(ex: Exploration, out_dir: Path, steps: int) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    lines = [f"# Explorer findings as of {ex.as_of}", "",
             f"Lens: {ex.lens}. {len(ex.accepted)} accepted, {len(ex.findings) - len(ex.accepted)} rejected; "
             f"{len(ex.queries)} queries, {steps} tool calls" + (f", {ex.spent}" if ex.spent else "") + "."
             + (f" {ex.stopped}." if ex.stopped else ""), ""]
    for title, keep in (("Accepted", True), ("Rejected", False)):
        group = [f for f in ex.findings if f.ok is keep]
        if not group:
            continue
        lines += [f"## {title}", ""]
        for f in group:
            lines += [f"### {f.title}", "", f"> {f.post}", "", f"- why: {f.why}", f"- subjects: {', '.join(f.subjects)}"]
            lines += [f"- problem: {p}" for p in f.problems]
            for qid in f.query_ids:
                q = ex.queries.get(qid)
                if q:
                    lines += [f"- {qid} ({q.purpose}, {len(q.rows)} rows):", "", "```sql", q.sql.strip(), "```"]
            lines.append("")
    lines += ["## Every query", ""] + [f"- {q.query_id}: {q.purpose} ({len(q.rows)} rows)" for q in ex.queries.values()]
    path = out_dir / f"explore_{ex.as_of}.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    (out_dir / f"explore_{ex.as_of}.json").write_text(json.dumps({
        "as_of": ex.as_of, "lens": ex.lens, "coverage": ex.coverage, "stopped": ex.stopped, "spent": ex.spent,
        "accepted": [f.__dict__ | {"queries": [ex.queries[i].__dict__ for i in f.query_ids if i in ex.queries]}
                     for f in ex.accepted],
        "rejected": [f.__dict__ for f in ex.findings if not f.ok],
        "queries": [{"query_id": q.query_id, "purpose": q.purpose, "sql": q.sql, "row_count": len(q.rows)}
                    for q in ex.queries.values()],
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path
