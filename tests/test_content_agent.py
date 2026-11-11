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
        insights = json.loads(kwargs["messages"][0]["content"].split("\n", 1)[1])
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
