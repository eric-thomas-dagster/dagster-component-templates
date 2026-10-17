"""<think> stripping, ollama routing, text tool-call recovery and opt-in include_reasoning for provider_ab_evaluator.
Only the judge's model call is faked."""
import inspect
import json
import sys
import types

import dagster as dg

from .conftest import load_component_module

THINK = "candidate b is more precise"
SCORES = {"a": {"score": 60, "reasoning": "ok"}, "b": {"score": 90, "reasoning": "precise"}}
TEXT_CALL = '<tool_call>{"name": "record_scores", "arguments": %s}</tool_call>' % json.dumps(SCORES)


def _run(monkeypatch, content, tool_calls=None, judge_model="ollama/qwen3", **extra):
    seen = []

    def completion(**kw):
        seen.append(kw)
        msg = types.SimpleNamespace(content=content, tool_calls=tool_calls)
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=msg)],
                                     usage=types.SimpleNamespace(total_tokens=5))

    fake = types.ModuleType("litellm")
    fake.completion = completion
    fake.completion_cost = lambda **kw: 0.0
    monkeypatch.setitem(sys.modules, "litellm", fake)
    mod = load_component_module()
    cls = next(c for c in vars(mod).values() if inspect.isclass(c) and issubclass(c, dg.Component)
               and c is not dg.Component and c.__module__ == mod.__name__)
    comp = cls(asset_name="verdict", candidates=["cand_a", "cand_b"], rubric={"kind": "literal", "text": "be precise"},
               judge={"model": judge_model}, **extra)

    @dg.asset(name="cand_a")
    def cand_a():
        return {"text": "vague answer", "provider_alias": "a"}

    @dg.asset(name="cand_b")
    def cand_b():
        return {"text": "precise answer", "provider_alias": "b"}

    res = dg.materialize([cand_a, cand_b, comp.build_defs(None).assets[0]],
                         resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    return res.output_for_node("verdict"), seen


def test_ollama_judge_text_tool_call_after_think(monkeypatch):
    out, seen = _run(monkeypatch, f"<think>{THINK}</think>{TEXT_CALL}")
    assert out["winner"] == "b" and out["winner_score"] == 90
    assert "judge_reasoning" not in out                        # default output unchanged
    assert seen[0]["model"] == "ollama_chat/qwen3"             # /api/generate cannot do tools
    assert seen[0]["tool_choice"] == "auto"                    # ollama has no 'required'


def test_structured_tool_call_non_ollama_keeps_required(monkeypatch):
    tc = types.SimpleNamespace(function=types.SimpleNamespace(name="record_scores", arguments=json.dumps(SCORES)))
    out, seen = _run(monkeypatch, None, tool_calls=[tc], judge_model="gpt-4o")
    assert out["winner"] == "b"
    assert seen[0]["model"] == "gpt-4o" and seen[0]["tool_choice"] == "required"


def test_include_reasoning(monkeypatch):
    out, _ = _run(monkeypatch, f"<think>{THINK}</think>{TEXT_CALL}", include_reasoning=True)
    assert out["judge_reasoning"] == THINK and out["winner"] == "b"
