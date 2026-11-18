"""`{tag.<key>}` substitution in prompt/system_prompt/mcp_servers[].env -- the mechanism a chat
sensor (e.g. slack_agent_request_sensor) uses to hand a run's own identifiers (channel, thread,
requester) to the agent's prompt and to a spawned MCP tool's environment, without the model ever
supplying them as a tool argument. Monkeypatches `_run_agent` itself (not a fake `anthropic`
module) so this test isolates exactly the substitution logic, not the model-call loop."""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _fake_result():
    return {
        "final_answer": "ok", "iterations": 1, "tool_calls_made": 0, "stopped_reason": "end_turn",
        "tool_call_details": [], "loaded_skill_names": [], "skill_load_count": 0,
    }


def test_tag_substitution_in_prompt_and_mcp_env(mod, monkeypatch):
    captured = {}

    async def fake_run_agent(**kwargs):
        captured.update(kwargs)
        return _fake_result()

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    component = mod.AnthropicAgentComponent(
        asset_name="agent_out",
        prompt="Answer the Slack message {tag.slack/message_ts} in channel {tag.slack/channel}.",
        mcp_servers=[dict(
            name="workflow", type="stdio", command=["python", "-m", "workflow_tools"],
            env={"DAGSTER_WORKFLOW_TOOLS_REQUESTER": "{tag.slack/user}", "STATIC": "unchanged"},
        )],
    )
    result = dg.materialize(
        component.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager()},
        tags={"slack/message_ts": "100.001", "slack/channel": "C123", "slack/user": "owner@co.com"},
    )
    assert result.success
    assert captured["prompt"] == "Answer the Slack message 100.001 in channel C123."
    env = captured["mcp_servers"][0]["env"]
    assert env["DAGSTER_WORKFLOW_TOOLS_REQUESTER"] == "owner@co.com"
    assert env["STATIC"] == "unchanged"  # a value with no placeholder passes through untouched


def test_missing_tag_leaves_placeholder_untouched(mod, monkeypatch):
    captured = {}

    async def fake_run_agent(**kwargs):
        captured.update(kwargs)
        return _fake_result()

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    component = mod.AnthropicAgentComponent(asset_name="agent_out", prompt="thread: {tag.slack/thread_ts}")
    result = dg.materialize(
        component.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager()},
    )  # no tags set at all
    assert result.success
    assert captured["prompt"] == "thread: {tag.slack/thread_ts}"  # not silently emptied


def test_run_id_and_tag_substitution_both_still_work(mod, monkeypatch):
    captured = {}

    async def fake_run_agent(**kwargs):
        captured.update(kwargs)
        return _fake_result()

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    component = mod.AnthropicAgentComponent(asset_name="agent_out", prompt="run={run_id} user={tag.slack/user}")
    result = dg.materialize(
        component.build_defs(None).assets,
        resources={"io_manager": dg.InMemoryIOManager()},
        tags={"slack/user": "jane@co.com"},
    )
    assert result.success
    assert captured["prompt"].startswith("run=")
    assert captured["prompt"].endswith("user=jane@co.com")
