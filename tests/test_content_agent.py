"""Content agent: drafts from the fixture's insights with a stand-in for
Claude, verified against gold. A wrong claim, a number the data doesn't
have, or an unknown insight must each keep a draft out of review."""
import json
from types import SimpleNamespace

import pytest

from gbdp.content import agent


class _FakeClient:
    """Answers messages.create with the posts it was built with, and records the request."""

    def __init__(self, make_posts):
        self.make_posts, self.requests = make_posts, []
        self.beta = SimpleNamespace(messages=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.requests.append(kwargs)
        insights = json.loads(kwargs["messages"][0]["content"].split("\n", 1)[1])["insights"]
        text = json.dumps({"posts": self.make_posts(insights)})
        return SimpleNamespace(stop_reason="end_turn", content=[SimpleNamespace(type="text", text=text)])


def _honest(insights):
    posts = []
    for i in insights:
        if i["detector"] == "hitting_streak":
            text = f"{i['subject_name']} has hit in {int(i['value'])} straight games."
            posts.append({"insight_id": i["insight_id"], "text": text,
                          "claims": [{"insight_id": i["insight_id"], "value": i["value"], "baseline": None}]})
    return posts


def test_honest_drafts_pass_and_reach_the_queue(semantic_gold, tmp_path):
    insights = agent.gather(limit=50)
    assert insights and all("evidence_rows" in i for i in insights)
    client = _FakeClient(_honest)
    drafts = agent.verify(agent.draft(client, insights), insights)
    assert drafts and all(d.ok for d in drafts), [d.problems for d in drafts]
    req = client.requests[0]
    assert req["model"] == agent.MODEL
    assert req["output_config"]["format"]["schema"] == agent.SCHEMA
    assert req["extra_body"] == {"fallbacks": "default"}
    path = agent.write_queue(drafts, insights, tmp_path, "2026-05-14")
    assert "Ready for review" in path.read_text(encoding="utf-8")


def test_a_wrong_claim_an_invented_number_or_an_unknown_insight_is_rejected(semantic_gold):
    insights = agent.gather(limit=50)
    streak = next(i for i in insights if i["detector"] == "hitting_streak")
    sid = streak["insight_id"]

    def lying(_):
        return [
            {"insight_id": sid, "text": "A streak.", "claims": [{"insight_id": sid, "value": streak["value"] + 3, "baseline": None}]},
            {"insight_id": sid, "text": f"{int(streak['value'])} straight games, 9999 fans watching.",
             "claims": [{"insight_id": sid, "value": streak["value"], "baseline": None}]},
            {"insight_id": "made-up", "text": "Nothing.", "claims": []},
        ]

    wrong_claim, invented, unknown = agent.verify(agent.draft(_FakeClient(lying), insights), insights)
    assert any("not verified" in p for p in wrong_claim.problems)
    assert any("9999" in p for p in invented.problems) and not any("not verified" in p for p in invented.problems)
    assert unknown.problems == ["unknown insight made-up"]


def test_numbers_match_at_the_precision_written():
    ins = {"value": 0.5208, "baseline": 0.2614, "delta": 0.2594, "sample_size": 48,
           "facts": {"z": 3.24, "recent_pa": 48, "baseline_pa": 207}, "evidence_rows": []}
    allowed = agent.allowed_numbers(ins)
    for written in (".521", "52.1", "48", "207", "3.24", "26.1", ".261"):
        assert any(agent._rounds_to(a, float(written)) for a in allowed), written
    assert not any(agent._rounds_to(a, 0.53) for a in allowed)
    assert [w for _, _, w in agent.text_numbers("A .521 wOBA over 48 PAs, up from .261 (22.3%)")] == [
        ".521", "48", ".261", "22.3%"]


def test_a_refusal_or_truncation_stops_the_run():
    def respond(stop):
        return SimpleNamespace(beta=SimpleNamespace(messages=SimpleNamespace(
            create=lambda **k: SimpleNamespace(stop_reason=stop, stop_details=None, content=[]))))
    with pytest.raises(RuntimeError, match="declined"):
        agent.draft(respond("refusal"), [{"insight_id": "x"}])
    with pytest.raises(RuntimeError, match="max_tokens"):
        agent.draft(respond("max_tokens"), [{"insight_id": "x"}])


def test_ollama_backend_drafts_with_the_same_schema_and_survives_junk(semantic_gold, monkeypatch):
    import requests

    insights = agent.gather(limit=50)
    sent = {}

    class _Resp:
        def __init__(self, posts):
            self.posts = posts

        def raise_for_status(self):
            pass

        def json(self):
            return {"message": {"content": json.dumps({"posts": self.posts})}}

    def fake_post(url, timeout, json):
        sent.update(url=url, body=json)
        return _Resp(_honest(insights) + [{"insight_id": insights[0]["insight_id"], "text": "x", "claims": ["junk"]}])

    monkeypatch.setattr(requests, "post", fake_post)
    drafts = agent.verify(agent.draft_ollama(insights, model="qwen2.5:3b"), insights)
    assert sent["url"].endswith("/api/chat") and sent["body"]["format"] == agent.SCHEMA
    assert sent["body"]["model"] == "qwen2.5:3b"
    assert all(d.ok for d in drafts[:-1]) and any("malformed claim" in p for p in drafts[-1].problems)


def test_superlatives_trend_words_and_season_for_a_partial_season_are_rejected(semantic_gold):
    insights = agent.gather(limit=50)
    streak = next(i for i in insights if i["detector"] == "hitting_streak")
    sid, n = streak["insight_id"], int(streak["value"])
    claim = [{"insight_id": sid, "value": streak["value"], "baseline": None}]
    # The 2026-11-11 Groq run: "the longest active streak in MLB" passed the number checks.
    posts = [
        {"insight_id": sid, "text": f"{n} straight games, the longest active streak.", "claims": claim},
        {"insight_id": sid, "text": f"His streak rose to {n} games.", "claims": claim},
        {"insight_id": sid, "text": f"{n} straight games this season.", "claims": claim},
        {"insight_id": sid, "text": f"{n} straight games with a hit.", "claims": claim},
    ]
    insights[0]["_coverage"] = {"start": "2026-05-13", "end": "2026-11-04"}
    longest, rose, season, fine = agent.verify(agent.draft(_FakeClient(lambda _: posts), insights), insights)
    assert any("longest" in p for p in longest.problems)
    assert any("rose" in p for p in rose.problems)
    assert any("data starts 2026-05-13" in p for p in season.problems)
    assert fine.ok, fine.problems


def test_the_prompt_carries_the_coverage(semantic_gold):
    insights = agent.gather(limit=5)
    client = _FakeClient(lambda _: [])
    agent.draft(client, insights)
    body = json.loads(client.requests[0]["messages"][0]["content"].split("\n", 1)[1])
    assert set(body["coverage"]) == {"start", "end"} and body["coverage"]["start"]
    assert all("_coverage" not in i for i in body["insights"])


def test_a_league_wide_rate_needs_its_window_and_may_name_the_start_date(semantic_gold):
    insights = agent.gather(limit=50)
    gap = next((i for i in insights if i["detector"] == "cross_league_gap"), None)
    if gap is None:
        pytest.skip("fixture has no cross-league insight")
    gid = gap["insight_id"]
    claim = [{"insight_id": gid, "value": gap["value"], "baseline": gap["baseline"]}]
    v, b = round(gap["value"] * 100, 1), round(gap["baseline"] * 100, 1)
    insights[0]["_coverage"] = {"start": "2026-05-13", "end": "2026-11-04"}
    posts = [{"insight_id": gid, "text": f"MLB: {v}% vs NPB {b}%.", "claims": claim},
             {"insight_id": gid, "text": f"Since May 13, MLB: {v}% vs NPB {b}%.", "claims": claim}]
    bare, windowed = agent.verify(agent.draft(_FakeClient(lambda _: posts), insights), insights)
    assert any("without its window" in p for p in bare.problems)
    assert windowed.ok, windowed.problems


def test_a_rejected_draft_gets_one_revision_with_its_problems(semantic_gold):
    insights = agent.gather(limit=50)
    streak = next(i for i in insights if i["detector"] == "hitting_streak")
    sid, n = streak["insight_id"], int(streak["value"])
    claim = [{"insight_id": sid, "value": streak["value"], "baseline": None}]
    seen = []

    def drafting(batch):
        seen.append(batch)
        if len(seen) == 1:
            return [agent.Draft(sid, f"{n} straight games, the longest active streak.", claim)]
        assert batch[0]["_revise"]["problems"] and "longest" in batch[0]["_revise"]["previous"]
        return [agent.Draft(sid, f"{n} straight games with a hit.", claim)]

    drafts = agent.verify(drafting(insights), insights)
    assert not drafts[0].ok
    out = agent.revise_once(drafts, insights, drafting, None)
    assert out[0].ok and out[0].text.endswith("with a hit.") and len(seen) == 2


def test_a_tie_rounds_either_way_and_a_comparison_with_the_league_is_not_a_trend():
    # 0.5625 as a percentage is 56.25: 56.3 and 56.2 both say it
    assert agent._rounds_to(56.25, 56.3) and agent._rounds_to(56.25, 56.2)
    assert not agent._rounds_to(56.25, 56.4)
    assert "down from" in agent._TREND_WORDS and "up from" in agent._TREND_WORDS
