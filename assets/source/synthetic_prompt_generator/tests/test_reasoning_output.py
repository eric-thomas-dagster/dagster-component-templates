"""<think> stripping + opt-in include_reasoning for synthetic_prompt_generator (mode: llm). Only the model call is faked."""
import sys
import types

import dagster as dg

from .conftest import load_component_module

THINK = "pick a tricky angle"


def _run(monkeypatch, reply, **extra):
    def completion(**kw):
        msg = types.SimpleNamespace(content=reply)
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=msg)])

    fake = types.ModuleType("litellm")
    fake.completion = completion
    monkeypatch.setitem(sys.modules, "litellm", fake)
    mod = load_component_module()
    comp = mod.SyntheticPromptGeneratorComponent(
        asset_name="prompts", topics=["billing"], count_per_topic=1, paraphrase_model="ollama/qwen3", **extra
    )
    assets = comp.build_defs(None).assets
    res = dg.materialize(assets, partition_key="billing", resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    md = res.get_asset_materialization_events()[0].materialization.metadata
    return res.output_for_node("prompts"), md


def test_think_stripped_from_the_prompt_and_default_metadata_unchanged(monkeypatch):
    text, md = _run(monkeypatch, f"<think>{THINK}</think>\"Why was I charged twice?\"")
    assert text == "Why was I charged twice?"
    assert "reasoning" not in md


def test_plain_reply_unchanged(monkeypatch):
    text, md = _run(monkeypatch, "Why was I charged twice?", include_reasoning=True)
    assert text == "Why was I charged twice?" and "reasoning" not in md   # nothing to surface


def test_include_reasoning_adds_metadata_only(monkeypatch):
    text, md = _run(monkeypatch, f"<think>{THINK}</think>Why was I charged twice?", include_reasoning=True)
    assert text == "Why was I charged twice?"            # the asset value stays the clean prompt string
    assert THINK in md["reasoning"].value
