"""Committed regression tests for LangGraphAgentComponent.

Covers both modes:
  - `steps` (declarative prompt-chain DSL): the LLM call itself is mocked
    via `_build_llm` (the one paid call), everything else is real --
    real StateGraph construction, real edge wiring, real conditional
    routing.
  - `graph_fn` (bring-your-own-graph): no mocking at all, since the graph
    in _fixtures.py has no LLM calls -- it's pure Python, run through a
    real compiled StateGraph.
"""
import importlib

import dagster as dg
import pytest

from .conftest import FakeLLM, load_component_module, requires_langgraph

# Imported via the exact same absolute dotted path the component's own
# `_resolve()` uses below (not `from . import _fixtures`) -- otherwise
# pytest's test-collection import and the component's importlib.import_module
# call land on two SEPARATE module objects with independent CALLS dicts.
_fixtures = importlib.import_module("assets.ai.langgraph_agent.tests._fixtures")

pytestmark = requires_langgraph

_ECHO_GRAPH = "assets.ai.langgraph_agent.tests._fixtures:build_echo_graph"


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _reset_fixture_state():
    _fixtures.reset()
    yield
    _fixtures.reset()


def _materialize(component):
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def])


# --- mutual exclusivity -----------------------------------------------

def test_neither_steps_nor_graph_fn_raises(mod):
    with pytest.raises(ValueError, match="must set either"):
        mod.LangGraphAgentComponent(asset_name="x").build_defs(load_context=None)


def test_both_steps_and_graph_fn_raises(mod):
    with pytest.raises(ValueError, match="mutually exclusive"):
        mod.LangGraphAgentComponent(
            asset_name="x",
            input_prompt="hi",
            steps=[{"name": "a", "prompt": "{input}"}],
            graph_fn=_ECHO_GRAPH,
        ).build_defs(load_context=None)


def test_steps_mode_requires_input_prompt(mod):
    with pytest.raises(ValueError, match="input_prompt.*required"):
        mod.LangGraphAgentComponent(
            asset_name="x",
            steps=[{"name": "a", "prompt": "{input}"}],
        ).build_defs(load_context=None)


def test_duplicate_step_names_raises(mod):
    with pytest.raises(ValueError, match="unique"):
        mod.LangGraphAgentComponent(
            asset_name="x",
            input_prompt="hi",
            steps=[
                {"name": "a", "prompt": "{input}"},
                {"name": "a", "prompt": "{input}"},
            ],
        ).build_defs(load_context=None)


# --- steps mode: linear chain + conditional routing (real StateGraph,
# LLM call mocked) --------------------------------------------------

def test_steps_mode_linear_chain(mod, monkeypatch):
    def responder(msgs):
        content = msgs[-1].content
        if "sub-questions" in content:
            return "SUB-QUESTIONS"
        if "Combine" in content:
            return "FINAL ANSWER"
        return "PLAN"

    monkeypatch.setattr(mod, "_build_llm", lambda *a, **k: FakeLLM(responder))

    component = mod.LangGraphAgentComponent(
        asset_name="research_report",
        input_prompt="How do vector databases scale?",
        steps=[
            {"name": "plan", "prompt": "Break down: {input}", "next": "research"},
            {"name": "research", "prompt": "Answer the sub-questions:\n{outputs.plan}", "next": "synthesize"},
            {"name": "synthesize", "prompt": "Combine: {outputs.research}"},
        ],
    )
    result = _materialize(component)
    assert result.success
    out = result.output_for_node("research_report")
    assert out["steps_run"] == ["plan", "research", "synthesize"]
    assert out["final"] == "FINAL ANSWER"
    assert out["stopped_by"] == "end_of_pipeline"
    assert out["outputs"] == {"plan": "PLAN", "research": "SUB-QUESTIONS", "synthesize": "FINAL ANSWER"}


def test_steps_mode_conditional_routing_takes_the_match_branch(mod, monkeypatch):
    """Regression test for `condition_regex` -- the one real LangGraph
    differentiator this component ships, and (before this test) never
    actually exercised by any committed test."""
    def responder(msgs):
        content = msgs[-1].content
        if "spam" in content:
            return "SPAM"
        if "Explain" in content:
            return "It mentions free money."
        return "should not be reached"

    monkeypatch.setattr(mod, "_build_llm", lambda *a, **k: FakeLLM(responder))

    component = mod.LangGraphAgentComponent(
        asset_name="spam_filter",
        input_prompt="Buy now, free money!",
        steps=[
            {
                "name": "classify",
                "prompt": "Is this spam?\n{input}",
                "condition_regex": "SPAM",
                "next": "quarantine",
                "condition_else": "allow",
            },
            {"name": "quarantine", "prompt": "Explain why:\n{input}"},
            {"name": "allow", "prompt": "Summarize:\n{input}"},
        ],
    )
    result = _materialize(component)
    assert result.success
    out = result.output_for_node("spam_filter")
    assert out["steps_run"] == ["classify", "quarantine"]
    assert "allow" not in out["outputs"]
    assert out["stopped_by"] == "conditional_end"


def test_steps_mode_conditional_routing_takes_the_else_branch(mod, monkeypatch):
    def responder(msgs):
        content = msgs[-1].content
        if "spam" in content:
            return "HAM"
        if "Summarize" in content:
            return "A legitimate message."
        return "should not be reached"

    monkeypatch.setattr(mod, "_build_llm", lambda *a, **k: FakeLLM(responder))

    component = mod.LangGraphAgentComponent(
        asset_name="spam_filter",
        input_prompt="Let's meet at 3pm.",
        steps=[
            {
                "name": "classify",
                "prompt": "Is this spam?\n{input}",
                "condition_regex": "SPAM",
                "next": "quarantine",
                "condition_else": "allow",
            },
            {"name": "quarantine", "prompt": "Explain why:\n{input}"},
            {"name": "allow", "prompt": "Summarize:\n{input}"},
        ],
    )
    result = _materialize(component)
    assert result.success
    out = result.output_for_node("spam_filter")
    assert out["steps_run"] == ["classify", "allow"]
    assert "quarantine" not in out["outputs"]
    assert out["stopped_by"] == "conditional_end"


# --- graph_fn mode: bring your own graph, no LLM involved --------------

def test_graph_fn_mode_invokes_the_existing_compiled_graph(mod):
    component = mod.LangGraphAgentComponent(
        asset_name="shout_asset",
        input_prompt="hello",
        graph_fn=_ECHO_GRAPH,
    )
    result = _materialize(component)
    assert result.success
    assert _fixtures.CALLS["build_count"] == 1
    out = result.output_for_node("shout_asset")
    assert out["final"] == "HELLO!"


def test_graph_fn_mode_initial_state_wins_over_input_prompt_on_conflict(mod):
    component = mod.LangGraphAgentComponent(
        asset_name="shout_asset",
        input_prompt="hello",
        initial_state={"input": "overridden", "extra": "sidecar"},
        graph_fn=_ECHO_GRAPH,
    )
    result = _materialize(component)
    assert result.success
    out = result.output_for_node("shout_asset")
    assert out["final"] == "OVERRIDDEN!"
    assert out["extra"] == "sidecar"


def test_graph_fn_mode_input_prompt_is_optional(mod):
    component = mod.LangGraphAgentComponent(
        asset_name="shout_asset",
        graph_fn=_ECHO_GRAPH,
    )
    result = _materialize(component)
    assert result.success
    out = result.output_for_node("shout_asset")
    assert out["final"] == "!"


def test_invalid_graph_fn_path_raises(mod):
    # graph_fn is resolved at materialize time (inside the asset body), not
    # at build_defs time -- same convention as dynamic_fanout_asset's and
    # warm_scheduled_job's `_resolve()` -- so instantiating/validating this
    # component doesn't require the user's own project modules to be
    # importable. So this must materialize to actually exercise the check.
    component = mod.LangGraphAgentComponent(
        asset_name="x",
        graph_fn="not_a_valid_path_no_colon",
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=False)
    assert not result.success
