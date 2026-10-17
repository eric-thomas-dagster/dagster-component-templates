"""<think> stripping + opt-in include_reasoning for inference_provider_ab_test (only the model call is faked)."""
import inspect
import sys
import types

import dagster as dg

from .conftest import load_component_module

THINK = "compare the options"


def _run(monkeypatch, replies, **extra):
    def completion(**kw):
        msg = types.SimpleNamespace(content=replies[kw["model"]], tool_calls=None)
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=msg)],
            usage=types.SimpleNamespace(prompt_tokens=3, completion_tokens=4, total_tokens=7),
        )

    fake = types.ModuleType("litellm")
    fake.completion = completion
    fake.completion_cost = lambda **kw: 0.001
    monkeypatch.setitem(sys.modules, "litellm", fake)
    mod = load_component_module()
    cls = next(c for c in vars(mod).values() if inspect.isclass(c) and issubclass(c, dg.Component)
               and c is not dg.Component and c.__module__ == mod.__name__)
    comp = cls(
        asset_name_prefix="ab",
        prompt={"kind": "literal", "text": "classify: crash on load"},
        providers=[{"alias": "think", "model": "ollama/qwen3"}, {"alias": "plain", "model": "gpt-4o-mini"}],
        **extra,
    )
    res = dg.materialize(comp.build_defs(None).assets)
    assert res.success
    return {a: res.output_for_node("ab_ab_test", output_name=f"ab_{a}") for a in ("think", "plain")}


REPLIES = {"ollama/qwen3": f"<think>{THINK}</think>defect", "gpt-4o-mini": "defect"}


def test_think_stripped_and_default_shape_unchanged(monkeypatch):
    out = _run(monkeypatch, REPLIES)
    for alias in ("think", "plain"):
        assert out[alias]["text"] == out[alias]["content"] == "defect"
        assert "reasoning" not in out[alias]


def test_include_reasoning_adds_key(monkeypatch):
    out = _run(monkeypatch, REPLIES, include_reasoning=True)
    assert out["think"]["text"] == "defect" and out["think"]["reasoning"] == THINK
    assert out["plain"]["text"] == "defect" and out["plain"]["reasoning"] is None
