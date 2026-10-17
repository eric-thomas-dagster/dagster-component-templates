"""<think> stripped before JSON parsing for text_moderator (OpenAI-compatible chat API, e.g. Ollama's /v1 endpoint).

Only the SDK call is faked (a stand-in `openai` module); the REAL component is materialized with dg.materialize()."""
import inspect
import sys
import types

import dagster as dg
import pandas as pd

from .conftest import load_component_module

REPLY = '{"scores": {"hate": 0.9}, "flagged": true, "explanation": "x"}'
THINK = "<think>The text mentions {braces} and [brackets] while I reason</think>"


def _run(monkeypatch, reply):
    class _Completions:
        def create(self, **kw):
            msg = types.SimpleNamespace(content=reply)
            return types.SimpleNamespace(
                choices=[types.SimpleNamespace(message=msg)],
                usage=types.SimpleNamespace(prompt_tokens=1, completion_tokens=1, total_tokens=2),
            )

    class _Client:
        def __init__(self, *a, **kw):
            self.chat = types.SimpleNamespace(completions=_Completions())

    fake = types.ModuleType("openai")
    fake.OpenAI = fake.AzureOpenAI = _Client
    monkeypatch.setitem(sys.modules, "openai", fake)
    mod = load_component_module()
    cls = next(c for c in vars(mod).values() if inspect.isclass(c) and issubclass(c, dg.Component)
               and c is not dg.Component and c.__module__ == mod.__name__)
    comp = cls(asset_name="out_asset", upstream_asset_key="src", method='llm', llm_provider='openai', llm_model='qwen3', api_key='k', input_column='text', categories='hate')
    frame = pd.DataFrame({"text": ["bad words", "worse words"]})

    @dg.asset(name="src")
    def src():
        return frame

    res = dg.materialize([src, comp.build_defs(None).assets[0]], resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    return res.output_for_node("out_asset")


def test_think_block_does_not_break_json_parsing(monkeypatch):
    plain = _run(monkeypatch, REPLY)
    thinking = _run(monkeypatch, THINK + REPLY)
    assert plain["flagged"].tolist() == [True, True]
    pd.testing.assert_frame_equal(plain, thinking)
