"""required_tool_calls -- a real failure mode observed twice in production: the model writes
findings and answers "to be filed" without actually calling the filing tool.

Two layers tested separately:
- _run_agent's own reminder-and-retry loop: a real (not mocked) stdio MCP round trip, only the
  paid litellm.acompletion call faked, via the real stdio server fixture from
  test_mcp_real_server. _run_agent itself only ever REPORTS required_tools_missing; it never
  raises (see the comment at its own call site for why).
- The asset body's on_required_tool_calls_missing="fail"/"warn" decision: _run_agent faked
  directly (same convention as test_outcome_asset.py) through a real dg.materialize round trip.
"""
import json
import sys
import types

import dagster as dg
import pytest

from .conftest import load_component_module
from .test_mcp_real_server import _call_kwargs, _run, mod, server_py, stdio_cfg  # noqa: F401


class _Msg:
    def __init__(self, content, tool_calls=None):
        self.content, self.tool_calls = content, tool_calls

    def model_dump(self, exclude_none=True):
        d = {"role": "assistant", "content": self.content}
        return {k: v for k, v in d.items() if v is not None}


def _install_scripted_litellm(monkeypatch, turns):
    """turns: a list of (content, tool_name_or_None). The Nth acompletion call plays turns[N-1]
    (clamped to the last entry once exhausted)."""
    state = {"n": 0}

    async def acompletion(messages, **kw):
        state["n"] += 1
        content, tool_name = turns[min(state["n"], len(turns)) - 1]
        if tool_name:
            tc = types.SimpleNamespace(
                id=f"c{state['n']}", type="function",
                function=types.SimpleNamespace(name=tool_name, arguments=json.dumps({"text": "hi"})),
            )
            msg = _Msg(content, [tc])
        else:
            msg = _Msg(content)
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=msg)])

    fake = types.ModuleType("litellm")
    fake.acompletion = acompletion
    monkeypatch.setitem(sys.modules, "litellm", fake)


# ── _run_agent's own reminder-and-retry loop (real stdio MCP round trip) ────────────────────────

def test_reminder_gives_the_model_a_second_chance_and_it_calls_the_tool(mod, stdio_cfg, monkeypatch):
    _install_scripted_litellm(monkeypatch, [
        ("Findings written, to be filed.", None),  # tries to stop without calling anything
        (None, "mini__echo"),                       # after the reminder, actually calls it
        ("done", None),
    ])
    result = _run(mod._run_agent(**_call_kwargs(
        mod._run_agent, stdio_cfg, api_key_env_var=None, api_base_env_var=None,
        required_tool_calls=["mini__echo"],
    )))
    assert result["stopped_reason"] == "final_answer"
    assert result["required_tools_missing"] == []
    assert any("Before finishing" in (m.get("content") or "") for m in result["transcript"] if m.get("role") == "user")
    assert result["final_answer"] == "done"


def test_still_missing_after_the_reminder_is_reported_not_raised(mod, stdio_cfg, monkeypatch):
    """_run_agent itself never raises for this -- it only reports. (The asset body decides
    whether to raise; see the dg.materialize-based tests below.)"""
    _install_scripted_litellm(monkeypatch, [
        ("Findings written, to be filed.", None),
        ("Still nothing called.", None),  # ignores the reminder
    ])
    result = _run(mod._run_agent(**_call_kwargs(
        mod._run_agent, stdio_cfg, api_key_env_var=None, api_base_env_var=None,
        required_tool_calls=["mini__echo"],
    )))
    assert result["required_tools_missing"] == ["mini__echo"]
    assert result["stopped_reason"] == "final_answer"


def test_any_of_group_satisfied_by_either_tool_even_if_the_call_errored(mod, stdio_cfg, monkeypatch):
    _install_scripted_litellm(monkeypatch, [
        ("will call boom", "mini__boom"),  # boom always raises -- still counts as "attempted"
        ("done", None),
    ])
    result = _run(mod._run_agent(**_call_kwargs(
        mod._run_agent, stdio_cfg, api_key_env_var=None, api_base_env_var=None,
        required_tool_calls=[["mini__echo", "mini__boom"]],
    )))
    assert result["required_tools_missing"] == []
    assert result["tool_call_details"][0]["is_error"] is True


def test_already_satisfied_before_trying_to_stop_never_sends_a_reminder(mod, stdio_cfg, monkeypatch):
    _install_scripted_litellm(monkeypatch, [
        ("calling it", "mini__echo"),
        ("done", None),
    ])
    result = _run(mod._run_agent(**_call_kwargs(
        mod._run_agent, stdio_cfg, api_key_env_var=None, api_base_env_var=None,
        required_tool_calls=["mini__echo"],
    )))
    assert result["required_tools_missing"] == []
    assert not any("Before finishing" in (m.get("content") or "") for m in result["transcript"] if m.get("role") == "user")


def test_no_required_tool_calls_means_no_new_behavior(mod, stdio_cfg, monkeypatch):
    _install_scripted_litellm(monkeypatch, [("done", None)])
    result = _run(mod._run_agent(**_call_kwargs(mod._run_agent, stdio_cfg, api_key_env_var=None, api_base_env_var=None)))
    assert "required_tools_missing" not in result


# ── the asset body's on_required_tool_calls_missing decision (fake _run_agent, real materialize) ─

@pytest.fixture()
def module_level_mod():
    return load_component_module()


def _fake_run_agent_with(missing):
    async def fake_run_agent(**kwargs):
        result = {
            "final_answer": "done", "iterations": 1, "tool_calls_made": 0, "tool_call_details": [],
            "transcript": [], "stopped_reason": "final_answer", "model": "m", "mcp_servers": [],
            "combined_server_instructions": None,
        }
        if kwargs.get("required_tool_calls"):
            result["required_tools_missing"] = missing
        return result
    return fake_run_agent


def test_on_missing_fail_is_the_default_and_fails_the_run(module_level_mod, monkeypatch):
    monkeypatch.setattr(module_level_mod, "_run_agent", _fake_run_agent_with(["mini__open_issue"]))
    component = module_level_mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", required_tool_calls=["mini__open_issue"],
    )
    assets = component.build_defs(None).assets
    with pytest.raises(Exception, match="required_tool_calls not satisfied"):
        dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)


def test_on_missing_warn_succeeds_and_reports_required_tools_missing(module_level_mod, monkeypatch):
    monkeypatch.setattr(module_level_mod, "_run_agent", _fake_run_agent_with(["mini__open_issue"]))
    component = module_level_mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", required_tool_calls=["mini__open_issue"],
        on_required_tool_calls_missing="warn",
        outcome_ledger_path="/tmp/does-not-matter", emit_outcome_asset=True,
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    outcome = result.output_for_node("agent_out_outcome")
    assert outcome["required_tools_missing"] == ["mini__open_issue"]


def test_satisfied_never_raises_or_warns(module_level_mod, monkeypatch):
    monkeypatch.setattr(module_level_mod, "_run_agent", _fake_run_agent_with([]))
    component = module_level_mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", required_tool_calls=["mini__open_issue"],
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
