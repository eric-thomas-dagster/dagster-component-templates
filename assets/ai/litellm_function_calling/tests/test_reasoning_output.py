"""<think> stripping, ollama routing and text <tool_call> recovery for litellm_function_calling.

Only the model call is faked; the REAL component is materialized with dg.materialize()."""
import inspect
import json
import sys
import types

import dagster as dg
import pandas as pd

from .conftest import load_component_module

TOOLS = [{"type": "function", "function": {"name": "search", "description": "s",
                                           "parameters": {"type": "object", "properties": {"q": {"type": "string"}}}}}]
TEXT_CALL = '<tool_call>{"name": "search", "arguments": {"q": "shoes"}}</tool_call>'


def _run(monkeypatch, content, tool_calls=None, model="ollama/qwen3", **extra):
    calls = []

    def completion(**kw):
        calls.append(kw)
        msg = types.SimpleNamespace(content=content, tool_calls=tool_calls)
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=msg)])

    fake = types.ModuleType("litellm")
    fake.completion = completion
    monkeypatch.setitem(sys.modules, "litellm", fake)
    mod = load_component_module()
    cls = next(c for c in vars(mod).values() if inspect.isclass(c) and issubclass(c, dg.Component)
               and c is not dg.Component and c.__module__ == mod.__name__)
    comp = cls(asset_name="out_asset", upstream_asset_key="src", text_column="q", tools=TOOLS, model=model, **extra)
    frame = pd.DataFrame({"q": ["find shoes", "find hats"]})

    @dg.asset(name="src")
    def src():
        return frame

    res = dg.materialize([src, comp.build_defs(None).assets[0]], resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    return res.output_for_node("out_asset"), calls


def test_text_tool_call_after_think_block_is_recovered(monkeypatch):
    out, calls = _run(monkeypatch, f"<think>user wants shoes</think>{TEXT_CALL}")
    parsed = json.loads(out["tool_calls"][0])
    assert parsed[0]["function"] == {"name": "search", "arguments": '{"q": "shoes"}'}
    assert "tool_calls_reasoning" not in out.columns
    assert calls[0]["model"] == "ollama_chat/qwen3"  # /api/generate has no tool support


def test_structured_tool_calls_still_win_and_non_ollama_model_untouched(monkeypatch):
    tc = types.SimpleNamespace(id="c1", type="function", function=types.SimpleNamespace(name="search", arguments="{}"))
    out, calls = _run(monkeypatch, None, tool_calls=[tc], model="gpt-4o-mini")
    assert json.loads(out["tool_calls"][0])[0]["id"] == "c1"
    assert calls[0]["model"] == "gpt-4o-mini"


def test_plain_reply_without_tools_gives_empty_list(monkeypatch):
    out, _ = _run(monkeypatch, "I cannot help with that")
    assert out["tool_calls"].tolist() == ["[]", "[]"]


def test_include_reasoning_exposes_thinking(monkeypatch):
    out, _ = _run(monkeypatch, f"<think>user wants shoes</think>{TEXT_CALL}", include_reasoning=True)
    assert out["tool_calls_reasoning"].tolist() == ["user wants shoes"] * 2
    assert json.loads(out["tool_calls"][1])[0]["function"]["name"] == "search"
