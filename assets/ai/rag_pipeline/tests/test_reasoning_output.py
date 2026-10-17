"""<think> stripping + opt-in include_reasoning for rag_pipeline (ops mode). Only the model call is faked.

The HyDE `expand_query` step's output becomes the *retrieval query*, so un-stripped thinking would be embedded and
searched; the final `generate` answer is what lands in the answer column."""
import inspect
import sys
import types

import dagster as dg
import pandas as pd

from .conftest import load_component_module

STEPS = [
    {"id": "hyde", "op": "expand_query", "model": "ollama/qwen3"},
    {"id": "gen", "op": "generate", "model": "ollama/qwen3", "query_source": "hyde"},
]


def _run(monkeypatch, replies, **extra):
    calls = []
    queue = list(replies)

    def completion(**kw):
        calls.append(kw)
        msg = types.SimpleNamespace(content=queue.pop(0), tool_calls=None)
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=msg)],
                                     usage=types.SimpleNamespace(total_tokens=3, model_dump=lambda: {"total_tokens": 3}))

    fake = types.ModuleType("litellm")
    fake.completion = completion
    fake.completion_cost = lambda **kw: 0.0
    monkeypatch.setitem(sys.modules, "litellm", fake)
    mod = load_component_module()
    comp = mod.RAGPipelineComponent(asset_name="answers", upstream_asset_key="questions", steps=STEPS, **extra)
    frame = pd.DataFrame({"query": ["capital of France?"]})

    @dg.asset(name="questions")
    def questions():
        return frame

    res = dg.materialize([questions, comp.build_defs(None).assets[0]], resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    return res.output_for_node("answers"), calls


THINKING = ["<think>hyde thinking</think>Paris is France's capital.", "<think>final thinking</think>Paris."]
PLAIN = ["Paris is France's capital.", "Paris."]


def test_thinking_is_not_embedded_and_not_in_the_answer(monkeypatch):
    out, calls = _run(monkeypatch, THINKING)
    assert out["answer"].tolist() == ["Paris."]
    assert "reasoning" not in " ".join(out.columns) and "answer_reasoning" not in out.columns
    # the generate step's prompt carries the CLEAN hypothetical answer, never the HyDE thinking
    assert "Paris is France's capital." in calls[1]["messages"][-1]["content"]
    assert "hyde thinking" not in calls[1]["messages"][-1]["content"]


def test_plain_replies_give_identical_output(monkeypatch):
    plain, _ = _run(monkeypatch, PLAIN)
    thinking, _ = _run(monkeypatch, THINKING)
    pd.testing.assert_frame_equal(plain, thinking)


def test_include_reasoning_adds_final_step_thinking(monkeypatch):
    out, _ = _run(monkeypatch, THINKING, include_reasoning=True)
    assert out["answer"].tolist() == ["Paris."]
    assert out["answer_reasoning"].tolist() == ["final thinking"]
    plain, _ = _run(monkeypatch, PLAIN, include_reasoning=True)
    assert plain["answer_reasoning"].isna().all()
