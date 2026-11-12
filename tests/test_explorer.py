"""The explorer (content/explorer.py): a model with read-only SQL finds its
own stories; code re-runs the cited queries and a skeptic reviews them."""
import json

import duckdb
import pytest

from gbdp.content import explorer as ex


@pytest.fixture
def sql():
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES ('Shohei Ohtani', 210, 0.4123), ('Aaron Judge', 205, 0.4011),"
                " ('Tiny Sample', 12, 0.700)) v(name, pa, woba)")

    def run(s):
        cur = con.execute(s)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    return run


def _ex(sql, avoid=(), start="2025-03-27"):
    return ex.Exploration(as_of="2025-08-31", coverage={"start": start, "end": "2025-08-31"}, lens="surprise",
                          avoid=list(avoid), run_sql=sql)


RANK = "SELECT name, pa, woba FROM t WHERE pa >= 200 ORDER BY woba DESC, name"


def test_a_post_built_from_its_cited_rows_passes(sql):
    e = _ex(sql)
    ex.execute(e, "query", {"sql": RANK, "purpose": "wOBA leaders"}, None, 30, 6000)
    f = ex.Finding("Ohtani", "Among hitters with 200+ PA, Shohei Ohtani leads with a .412 wOBA (210 PA).",
                   "rank", ["Shohei Ohtani"], ["q1"])
    assert ex.check_finding(f, e) == []


def test_invented_numbers_names_rankings_and_repeats_are_caught(sql):
    e = _ex(sql, avoid=["Aaron Judge"], start="2025-05-13")  # a partial season
    ex.execute(e, "query", {"sql": "SELECT name, woba FROM t WHERE pa >= 200", "purpose": "no order"}, None, 30, 6000)
    f = ex.Finding("x", "Mookie Betts leads the season with a .455 wOBA, ahead of Aaron Judge.", "",
                   ["Mookie Betts", "Aaron Judge"], ["q1", "q9"])
    problems = " | ".join(ex.check_finding(f, e))
    for want in ("unknown query_ids", ".455", "'Mookie Betts' isn't in the cited results", "ranking word",
                 "already covered", "says 'season'"):
        assert want in problems, want


def test_the_loop_runs_tools_asks_the_skeptic_and_writes_the_queue(sql, tmp_path, monkeypatch):
    monkeypatch.setattr(ex.agent, "coverage", lambda as_of: {"start": "2025-03-27", "end": "2025-08-31"})
    good = "Among hitters with 200+ PA, Shohei Ohtani leads with a .412 wOBA (210 PA)."
    script = [
        [("query", {"sql": RANK, "purpose": "wOBA leaders"})],
        [("submit_finding", {"title": "t", "post": "Shohei Ohtani has a .999 wOBA.", "why": "", "subjects": ["Shohei Ohtani"], "query_ids": ["q1"]})],
        [("submit_finding", {"title": "Ohtani", "post": good, "why": "rank", "subjects": ["Shohei Ohtani"], "query_ids": ["q1"]})],
    ]
    seen = []

    class Fake:
        def step(self, system, first, turns):
            seen.append([r["shown"] for t in turns if t["kind"] == "results" for r in t["results"]])
            calls = [{"id": f"c{len(seen)}{k}", "name": n, "args": a} for k, (n, a) in enumerate(script[len(seen) - 1])]
            return {"role": "assistant"}, calls, ""

        def json_call(self, system, user, schema):
            assert "Shohei Ohtani" in user and RANK in user
            return {"verdict": "pass", "problems": []}

    path = ex.explore(Fake(), as_of="2025-08-31", out_dir=tmp_path, lens="arsenal", findings=1, keep_full=1, run_sql=sql)
    out = json.loads(path.with_suffix(".json").read_text())
    assert [f["post"] for f in out["accepted"]] == [good]
    assert ".999" in out["rejected"][0]["problems"][0]
    assert seen[2][0].startswith("[q1: 2 rows")  # the old query result shrank to its stub


def test_a_skeptic_fail_rejects_a_clean_finding(sql):
    e = _ex(sql)
    ex.execute(e, "query", {"sql": RANK, "purpose": "leaders"}, None, 30, 6000)
    out = ex.execute(e, "submit_finding", {"title": "t", "post": "Shohei Ohtani: .412 wOBA over 210 PA.", "why": "",
                                           "subjects": ["Shohei Ohtani"], "query_ids": ["q1"]},
                     lambda payload: {"verdict": "fail", "problems": ["cherry-picked threshold"]}, 30, 6000)
    assert out.startswith("REJECTED") and "cherry-picked" in out and not e.accepted


def test_an_api_error_mid_run_keeps_what_was_found(sql, tmp_path, monkeypatch):
    monkeypatch.setattr(ex.agent, "coverage", lambda as_of: {"start": "2025-03-27", "end": "2025-08-31"})

    class Dies:
        n = 0

        def step(self, system, first, turns):
            Dies.n += 1
            if Dies.n > 1:
                raise RuntimeError("429: tokens per day")
            return {}, [{"id": "c1", "name": "query", "args": {"sql": RANK, "purpose": "leaders"}}], ""

    path = ex.explore(Dies(), as_of="2025-08-31", out_dir=tmp_path, lens="teams", run_sql=sql)
    out = json.loads(path.with_suffix(".json").read_text())
    assert "tokens per day" in out["stopped"] and out["queries"][0]["query_id"] == "q1"
    assert (tmp_path / "explore_2025-08-31.steps.jsonl").read_text().count("\n") == 1


def test_compact_shrinks_old_turns_and_keeps_the_newest():
    big = "SELECT " + "x, " * 500 + "1"
    msgs = [{"role": "system", "content": "s"}, {"role": "user", "content": "task"}]
    for k in range(6):
        msgs.append({"role": "assistant", "content": "thinking " * 50, "tool_calls": [
            {"id": f"c{k}", "type": "function", "function": {"name": "query", "arguments": json.dumps({"sql": big, "purpose": "p"})}}]})
        msgs.append({"role": "tool", "tool_call_id": f"c{k}", "content": "row\n" * 300})
    ex.compact(msgs, 6000)
    assert len(json.dumps(msgs)) < 6000 + 2 * (len(big) + 1500)
    assert json.loads(msgs[-2]["tool_calls"][0]["function"]["arguments"])["sql"] == big  # the newest call stays whole
    assert json.loads(msgs[2]["tool_calls"][0]["function"]["arguments"])["sql"].endswith(" ...")
    assert msgs[1]["content"] == "task"


def test_the_token_budget_stops_the_run_before_a_call_that_could_pass_it(sql, tmp_path, monkeypatch):
    monkeypatch.setattr(ex.agent, "coverage", lambda as_of: {"start": "2025-03-27", "end": "2025-08-31"})
    sent = []

    class Usage:
        input_tokens, output_tokens = 9000, 1000

    class Block:
        type, id, name, input = "tool_use", "t", "list_tables", {}

    class Resp:
        stop_reason = "tool_use"
        usage = Usage()
        content = [Block()]

    class Messages:
        @staticmethod
        def create(**kw):
            sent.append(kw)
            return Resp()

    class Client:
        messages = Messages()

    backend = ex.ClaudeBackend(Client(), max_tokens=4000, budget=30_000)
    monkeypatch.setattr(ex, "execute", lambda *a, **k: "[]")
    path = ex.explore(backend, as_of="2025-08-31", out_dir=tmp_path, lens="teams", run_sql=sql, max_steps=50)
    out = json.loads(path.with_suffix(".json").read_text())
    assert "token budget spent" in out["stopped"]
    assert backend.meter.spent <= 30_000 and len(sent) == backend.meter.calls


def test_a_missing_column_error_lists_the_real_columns(monkeypatch):
    from gbdp import mcp_server as tools

    monkeypatch.setattr(tools, "list_tables", lambda: [{"table": "v_player_current"}, {"table": "v_pitch"}])
    monkeypatch.setattr(tools, "describe_table", lambda t: {"table": t, "columns": {
        "v_player_current": {"player_id": "", "primary_name": ""}, "v_pitch": {"pitcher_id": "", "plate_x": ""}}[t]})
    cols = {"v_player_current": ["player_id", "primary_name"], "v_pitch": ["pitcher_id", "plate_x"]}
    hint = ex.column_hint('Binder Error: Values list "pp" does not have a column named "name_first"',
                          "SELECT pp.name_first FROM v_pitch p JOIN v_player_current pp ON pp.player_id = p.pitcher_id",
                          cols.get)
    assert "v_pitch: pitcher_id, plate_x" in hint and "v_player_current: player_id, primary_name" in hint
    assert ex.column_hint("Parser Error: syntax", "SELECT 1", cols.get) == ""
    brief = ex.table_brief({"table": "v_pitch", "sql": "WITH ... long", "columns": {"pitcher_id": "FK", "plate_x": "ft"},
                            "description": "x"})
    assert brief["column_names"] == ["pitcher_id", "plate_x"] and "sql" not in brief


def test_a_views_columns_come_from_one_row(sql, monkeypatch):
    from gbdp import mcp_server as tools

    monkeypatch.setattr(tools, "describe_table", lambda t: {"table": t, "category": "view", "sql": "SELECT ..."})
    e = _ex(sql)
    assert ex.columns_of(e, "t") == ["name", "pa", "woba"]
