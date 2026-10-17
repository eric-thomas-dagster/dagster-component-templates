"""<think> stripping + opt-in include_reasoning for llm_evaluator (only the judge model call is faked)."""
import inspect
import sys
import types

import dagster as dg

from .conftest import load_component_module

THINK = "SCORE: 2 would be too harsh, SCORE: 3 is wrong; the answer is on topic"  # a decoy SCORE inside the thinking
VERDICT = "SCORE: 9\nREASON: directly answers the question"


def _run(monkeypatch, reply, **extra):
    def completion(**kw):
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=reply))])

    fake = types.ModuleType("litellm")
    fake.completion = completion
    monkeypatch.setitem(sys.modules, "litellm", fake)
    mod = load_component_module()
    cls = next(c for c in vars(mod).values() if inspect.isclass(c) and issubclass(c, dg.Component)
               and c is not dg.Component and c.__module__ == mod.__name__)
    comp = cls(asset_name="scored", upstream_asset_key="agent_out", evaluations=["answer_relevance"], **extra)

    @dg.asset(name="agent_out")
    def agent_out():
        return {"final_answer": "Paris is the capital of France.", "question": "What is the capital of France?"}

    res = dg.materialize([agent_out, comp.build_defs(None).assets[0]], resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    return res.output_for_node("scored")["evaluations"]["answer_relevance"]


def test_decoy_score_in_thinking_is_not_the_verdict(monkeypatch):
    ev = _run(monkeypatch, f"<think>{THINK}</think>{VERDICT}")
    assert ev == {"score": 0.9, "reason": "directly answers the question"}  # default shape unchanged: no 'reasoning' key


def test_plain_reply_unchanged(monkeypatch):
    assert _run(monkeypatch, VERDICT) == {"score": 0.9, "reason": "directly answers the question"}


def test_include_reasoning_adds_thinking(monkeypatch):
    ev = _run(monkeypatch, f"<think>{THINK}</think>{VERDICT}", include_reasoning=True)
    assert ev["score"] == 0.9 and ev["reasoning"] == THINK
    assert _run(monkeypatch, VERDICT, include_reasoning=True)["reasoning"] is None
