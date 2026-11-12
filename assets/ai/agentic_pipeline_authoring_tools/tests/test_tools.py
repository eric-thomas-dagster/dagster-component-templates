"""Unit tests for each of the 6 authoring tools, driven directly against the real agentic_pipeline component.py
(loaded by path -- no mocking of agentic_pipeline itself)."""
import json

import pytest

from .conftest import GOOD_PIPELINE_YAML

BAD_FIELD_YAML = """
asset_name_prefix: demo
source: {kind: literal, text: hello}
steps:
  - id: draft
    op: llm_call
    max_tokens: not_a_number
outputs: {assets: [draft]}
"""

UNKNOWN_OP_YAML = """
asset_name_prefix: demo
source: {kind: literal, text: hello}
steps:
  - id: draft
    op: not_a_real_op
outputs: {assets: [draft]}
"""

BUILD_TIME_ERROR_YAML = """
asset_name_prefix: demo
source: {kind: literal, text: 'hello {partition_key}'}
steps:
  - id: draft
    op: llm_call
    model: gpt-4o-mini
    api_key_env_var: OPENAI_API_KEY
    prompt_template: 'Draft: {text}'
  - id: gate
    op: approve
    source: draft
    approval_dir: /tmp/x
    mode: park
    key: fixed-key-not-partitioned
outputs: {assets: [draft, gate]}
partition_type: daily
partition_start: '2026-01-01'
"""

MULTI_STEP_YAML = """
asset_name_prefix: demo
source: {kind: literal, text: hello}
steps:
  - id: draft
    op: llm_call
    model: gpt-4o-mini
    api_key_env_var: OPENAI_API_KEY
    prompt_template: 'Draft: {text}'
  - id: tools
    op: tool_use_loop
    source: draft
    model: gpt-4o-mini
    api_key_env_var: ANTHROPIC_API_KEY
    mcp_servers:
      - name: dagster
        type: stdio
        command: [python, server.py]
      - server_resource_key: shared_mcp
    skill_keys: [some_skill]
    system_prompt: go
outputs: {assets: [draft, tools]}
"""


def test_list_ops_covers_all_25_ops_with_real_summaries(tools):
    result = tools.call("list_ops")
    ops = result["ops"]
    assert len(ops) == 25
    assert {o["op"] for o in ops} >= {"llm_call", "approve", "delegate", "tool_use_loop", "verify"}
    assert all(o["summary"] for o in ops), "every op must have a non-empty one-line summary (from its model's docstring)"


def test_describe_op_returns_the_real_pydantic_schema(tools):
    result = tools.call("describe_op", {"op": "llm_call"})
    assert result["op"] == "llm_call"
    props = result["schema"]["properties"]
    assert "model" in props and "prompt_template" in props and "temperature" in props


def test_describe_op_unknown_op_lists_valid_ones(tools):
    result = tools.call("describe_op", {"op": "nope"})
    assert "error" in result and "nope" in result["error"] and "llm_call" in result["error"]


def test_list_examples_and_get_example_round_trip(tools):
    listing = tools.call("list_examples")
    assert len(listing["examples"]) >= 5
    first = listing["examples"][0]
    assert first["title"] and first["summary"]

    example = tools.call("get_example", {"name": first["name"]})
    assert example["name"] == first["name"]
    assert "op:" in example["pipeline_yaml"]["text"]
    assert example["pipeline_yaml"]["truncated"] is False
    assert "readme" in example


def test_get_example_unknown_name_lists_valid_ones(tools):
    result = tools.call("get_example", {"name": "not_a_real_example"})
    assert "error" in result and "not_a_real_example" in result["error"]


def test_validate_pipeline_accepts_a_real_valid_pipeline(tools):
    assert tools.call("validate_pipeline", {"yaml": GOOD_PIPELINE_YAML}) == {"valid": True, "errors": []}


def test_validate_pipeline_reports_step_index_id_op_and_field_for_a_bad_field(tools):
    result = tools.call("validate_pipeline", {"yaml": BAD_FIELD_YAML})
    assert result["valid"] is False
    err = result["errors"][0]
    assert err["step_index"] == 0
    assert err["step_id"] == "draft"
    assert err["op"] == "llm_call"
    assert err["field"] == "max_tokens"
    assert "integer" in err["message"]


def test_validate_pipeline_reports_an_unknown_op(tools):
    result = tools.call("validate_pipeline", {"yaml": UNKNOWN_OP_YAML})
    assert result["valid"] is False
    assert result["errors"]


def test_validate_pipeline_reports_a_build_time_error_too(tools):
    """Pydantic construction succeeds (every field is individually well-typed); the real error is a semantic
    load-time guard inside build_defs (single_run is rejected for a pipeline with an approve/park step)."""
    result = tools.call("validate_pipeline", {"yaml": BUILD_TIME_ERROR_YAML})
    assert result["valid"] is False
    assert result["errors"][0]["source"] == "build_defs"


def test_preview_pipeline_reports_assets_edges_and_needs(tools):
    result = tools.call("preview_pipeline", {"yaml": MULTI_STEP_YAML})
    names = {a["asset"] for a in result["assets"]}
    assert names == {"demo_draft", "demo_tools"}
    tools_asset = next(a for a in result["assets"] if a["asset"] == "demo_tools")
    assert tools_asset["depends_on"] == ["demo_draft"]
    assert {"from": "demo_draft", "to": "demo_tools"} in result["edges"]

    needs = result["needs"]
    assert set(needs["env_vars"]) == {"OPENAI_API_KEY", "ANTHROPIC_API_KEY"}
    assert {"step": "tools", "name": "dagster", "type": "stdio", "server_resource_key": None} in needs["mcp_servers"]
    assert {"step": "tools", "name": None, "type": None, "server_resource_key": "shared_mcp"} in needs["mcp_servers"]
    assert needs["skill_resource_keys"] == ["some_skill"]


def test_preview_pipeline_reports_partitioning(tools):
    y = GOOD_PIPELINE_YAML.replace("text: hello", "text: 'hello {partition_key}'") + "\npartition_type: daily\npartition_start: '2026-01-01'\n"
    result = tools.call("preview_pipeline", {"yaml": y})
    assert result["partition"]["partitioned"] is True
    assert result["partition"]["partition_type"] == "daily"


def test_preview_pipeline_on_an_invalid_pipeline_returns_an_error_not_a_crash(tools):
    result = tools.call("preview_pipeline", {"yaml": BAD_FIELD_YAML})
    assert "error" in result


def test_allow_restricts_which_tools_are_callable(mod):
    tools = mod.AgenticPipelineAuthoringTools(allow=["list_ops"])
    assert tools.call("list_ops")["ops"]
    result = tools.call("describe_op", {"op": "llm_call"})
    assert "error" in result and "not enabled" in result["error"]


def test_bad_yaml_is_reported_as_an_error_not_an_exception(tools):
    result = tools.call("validate_pipeline", {"yaml": "not: valid: yaml: : :"})
    assert "error" in result
