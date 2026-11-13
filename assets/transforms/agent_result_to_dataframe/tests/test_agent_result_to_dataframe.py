"""Committed tests for AgentResultToDataFrameComponent.

Everything here is real: a real upstream asset materializing a real dict
(matching the actual shape anthropic_agent/litellm_agent/etc. return), routed
through the real asset graph via dg.materialize(). Nothing is mocked --
there's no paid call anywhere in this component.
"""
import pandas as pd
import dagster as dg
import pytest

from .conftest import load_component_module

CLS = "AgentResultToDataFrameComponent"


@pytest.fixture()
def mod():
    return load_component_module()


AGENT_RESULT = {
    "final_answer": "The answer is 42.",
    "iterations": 3,
    "tool_calls_made": 2,
    "tool_call_details": [
        {"iteration": 1, "tool": "search", "args": {"q": "life"}, "result_preview": "42", "is_error": False},
        {"iteration": 2, "tool": "calc", "args": {"expr": "6*7"}, "result_preview": "42", "is_error": False},
    ],
    "transcript": [{"role": "user", "content": "what is the answer?"}],
    "model": "claude-x",
}


def _as_df(x):
    return x.value if hasattr(x, "value") and hasattr(x, "metadata") else x


def _materialize(mod, upstream_value, partitioned=False, partition_key=None, **overrides):
    @dg.asset(name="agent_asset")
    def agent_asset():
        return upstream_value

    cls = getattr(mod, CLS)
    kw = dict(asset_name="out_asset", upstream_asset_key="agent_asset")
    if partitioned:
        kw.update(partition_type="daily", partition_start="2024-01-01")
    kw.update(overrides)
    comp = cls(**kw)
    comp_assets = comp.build_defs(None).assets
    result = dg.materialize(
        [agent_asset] + list(comp_assets),
        resources={"io_manager": dg.InMemoryIOManager()},
        partition_key=partition_key,
    )
    assert result.success
    return _as_df(result.output_for_node("out_asset"))


def test_one_row_mode_keeps_scalar_fields_and_drops_nested_by_default(mod):
    df = _materialize(mod, AGENT_RESULT)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["final_answer"] == "The answer is 42."
    assert row["iterations"] == 3
    assert row["tool_calls_made"] == 2
    assert "tool_call_details" not in df.columns
    assert "transcript" not in df.columns


def test_one_row_mode_add_run_columns(mod):
    df = _materialize(mod, AGENT_RESULT)
    for col in ("run_id", "partition_key", "materialized_at", "asset_key"):
        assert col in df.columns
    assert df.iloc[0]["asset_key"] == "out_asset"
    assert df.iloc[0]["partition_key"] is None


def test_add_run_columns_false_omits_them(mod):
    df = _materialize(mod, AGENT_RESULT, add_run_columns=False)
    for col in ("run_id", "partition_key", "materialized_at", "asset_key"):
        assert col not in df.columns


def test_include_columns_can_pull_in_a_nested_field_as_json_text(mod):
    df = _materialize(mod, AGENT_RESULT, include_columns=["final_answer", "tool_call_details"])
    assert "tool_call_details" in df.columns
    assert isinstance(df.iloc[0]["tool_call_details"], str)
    assert "search" in df.iloc[0]["tool_call_details"]


def test_exclude_columns_drops_a_default_scalar_field(mod):
    df = _materialize(mod, AGENT_RESULT, exclude_columns=["model"])
    assert "model" not in df.columns
    assert "final_answer" in df.columns


def test_extra_columns_literal_and_partition_key_substitution(mod):
    df = _materialize(
        mod, AGENT_RESULT, partitioned=True, partition_key="2024-01-02",
        extra_columns={"pipeline": "support_agent", "run_date": "{partition_key}"},
    )
    assert df.iloc[0]["pipeline"] == "support_agent"
    assert df.iloc[0]["run_date"] == "2024-01-02"
    assert df.iloc[0]["partition_key"] == "2024-01-02"


def test_explode_mode_one_row_per_list_entry(mod):
    df = _materialize(mod, AGENT_RESULT, mode="explode")
    assert len(df) == 2
    assert set(df["tool"]) == {"search", "calc"}
    assert df.iloc[0]["is_error"] == False  # noqa: E712


def test_explode_mode_custom_field(mod):
    result = {"final_answer": "x", "steps": [{"n": 1}, {"n": 2}, {"n": 3}]}
    df = _materialize(mod, result, mode="explode", explode_field="steps")
    assert df["n"].tolist() == [1, 2, 3]


def test_explode_mode_missing_field_raises_clear_error(mod):
    with pytest.raises(ValueError, match="no result had explode_field"):
        _materialize(mod, {"final_answer": "x"}, mode="explode", explode_field="tool_call_details")


def test_extract_mode_parses_json_array_in_final_answer(mod):
    result = {"final_answer": '[{"name": "a", "score": 1}, {"name": "b", "score": 2}]'}
    df = _materialize(mod, result, mode="extract")
    assert df["name"].tolist() == ["a", "b"]
    assert df["score"].tolist() == [1, 2]


def test_extract_mode_parses_fenced_json_block(mod):
    result = {"final_answer": 'Here you go:\n```json\n{"category": "urgent", "confidence": 0.9}\n```\nthanks'}
    df = _materialize(mod, result, mode="extract")
    assert df.iloc[0]["category"] == "urgent"
    assert df.iloc[0]["confidence"] == 0.9


def test_extract_mode_bad_json_raises_by_default(mod):
    with pytest.raises(ValueError, match="could not parse final_answer as JSON"):
        _materialize(mod, {"final_answer": "not json at all"}, mode="extract")


def test_extract_mode_keep_raw_on_parse_error(mod):
    df = _materialize(mod, {"final_answer": "not json at all"}, mode="extract", on_parse_error="keep_raw")
    assert len(df) == 1
    assert df.iloc[0]["final_answer_raw"] == "not json at all"
    assert "parse_error" in df.columns


def test_pipeline_step_shape_works_generically(mod):
    """agentic_pipeline step results are {text, ...}, not {final_answer, ...} -- one_row mode is shape-agnostic."""
    result = {"text": "draft content", "cost_usd": 0.002, "op": "draft"}
    df = _materialize(mod, result)
    assert df.iloc[0]["text"] == "draft content"
    assert df.iloc[0]["cost_usd"] == 0.002
    assert df.iloc[0]["op"] == "draft"


def test_partition_bridge_dict_of_results_produces_one_row_per_partition(mod):
    """Unpartitioned downstream reading a partitioned upstream: Dagster's IO manager
    hands back {partition_key: result_dict, ...} instead of one result."""
    bridge = {
        "2024-01-01": {"final_answer": "a1", "iterations": 1},
        "2024-01-02": {"final_answer": "a2", "iterations": 2},
    }
    df = _materialize(mod, bridge)
    assert len(df) == 2
    assert set(df["final_answer"]) == {"a1", "a2"}
    assert set(df["partition_key"]) == {"2024-01-01", "2024-01-02"}
