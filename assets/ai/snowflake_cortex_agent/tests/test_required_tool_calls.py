"""required_tool_calls -- a real failure mode observed twice in production: the model writes
findings and answers "to be filed" without actually calling the filing tool.

Two layers tested separately:
- _run_agent's own reminder-and-retry loop: a real (not mocked) stdio MCP round trip, only the
  paid requests.post call to Cortex's inference endpoint faked, via the real stdio server
  fixture from test_mcp_real_server. _run_agent itself only ever REPORTS required_tools_missing;
  it never raises (see the comment at its own call site for why).
- The asset body's on_required_tool_calls_missing="fail"/"warn" decision: _run_agent faked
  directly (same convention as the other agent components' own tests) through a real
  dg.materialize round trip.
"""
import json as _json
import sys

import dagster as dg
import pytest

from .conftest import load_component_module
from .test_mcp_real_server import _call_kwargs, _run, mod, server_py, stdio_cfg  # noqa: F401


class _Resp:
    status_code = 200
    text = ""

    def __init__(self, body):
        self._b = body

    def json(self):
        return self._b


def _install_scripted_cortex(monkeypatch, turns):
    """turns: a list of (content_or_None, tool_name_or_None). The Nth post() call plays
    turns[N-1] (clamped to the last entry once exhausted)."""
    import requests
    state = {"n": 0}

    def post(url, json=None, headers=None, timeout=None):  # noqa: A002
        state["n"] += 1
        content, tool_name = turns[min(state["n"], len(turns)) - 1]
        if tool_name:
            tc = {"id": f"c{state['n']}", "type": "function", "function": {"name": tool_name, "arguments": _json.dumps({"text": "hi"})}}
            return _Resp({"choices": [{"message": {"content": content, "tool_calls": [tc]}}]})
        return _Resp({"choices": [{"message": {"content": content}}]})

    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setenv("FAKE_URL", "https://acct.invalid")
    monkeypatch.setenv("FAKE_TOKEN", "pat")


_EXTRA = dict(account_url_env_var="FAKE_URL", auth_token_env_var="FAKE_TOKEN")


# ── _run_agent's own reminder-and-retry loop (real stdio MCP round trip) ────────────────────────

def test_reminder_gives_the_model_a_second_chance_and_it_calls_the_tool(mod, stdio_cfg, monkeypatch):
    _install_scripted_cortex(monkeypatch, [
        ("Findings written, to be filed.", None),
        (None, "mini__echo"),
        ("done", None),
    ])
    result = _run(mod._run_agent(**_call_kwargs(mod._run_agent, stdio_cfg, required_tool_calls=["mini__echo"], **_EXTRA)))
    assert result["stopped_reason"] == "final_answer"
    assert result["required_tools_missing"] == []
    assert any("Before finishing" in (m.get("content") or "") for m in result["transcript"] if m.get("role") == "user")
    assert result["final_answer"] == "done"


def test_still_missing_after_the_reminder_is_reported_not_raised(mod, stdio_cfg, monkeypatch):
    _install_scripted_cortex(monkeypatch, [
        ("Findings written, to be filed.", None),
        ("Still nothing called.", None),
    ])
    result = _run(mod._run_agent(**_call_kwargs(mod._run_agent, stdio_cfg, required_tool_calls=["mini__echo"], **_EXTRA)))
    assert result["required_tools_missing"] == ["mini__echo"]
    assert result["stopped_reason"] == "final_answer"


def test_any_of_group_satisfied_by_either_tool_even_if_the_call_errored(mod, stdio_cfg, monkeypatch):
    _install_scripted_cortex(monkeypatch, [
        ("will call boom", "mini__boom"),
        ("done", None),
    ])
    result = _run(mod._run_agent(**_call_kwargs(mod._run_agent, stdio_cfg, required_tool_calls=[["mini__echo", "mini__boom"]], **_EXTRA)))
    assert result["required_tools_missing"] == []
    assert result["tool_call_details"][0]["is_error"] is True


def test_already_satisfied_before_trying_to_stop_never_sends_a_reminder(mod, stdio_cfg, monkeypatch):
    _install_scripted_cortex(monkeypatch, [
        ("calling it", "mini__echo"),
        ("done", None),
    ])
    result = _run(mod._run_agent(**_call_kwargs(mod._run_agent, stdio_cfg, required_tool_calls=["mini__echo"], **_EXTRA)))
    assert result["required_tools_missing"] == []
    assert not any("Before finishing" in (m.get("content") or "") for m in result["transcript"] if m.get("role") == "user")


def test_no_required_tool_calls_means_no_new_behavior(mod, stdio_cfg, monkeypatch):
    _install_scripted_cortex(monkeypatch, [("done", None)])
    result = _run(mod._run_agent(**_call_kwargs(mod._run_agent, stdio_cfg, **_EXTRA)))
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
            "loaded_skill_names": [], "skill_load_count": 0,
        }
        if kwargs.get("required_tool_calls"):
            result["required_tools_missing"] = missing
        return result
    return fake_run_agent


def test_on_missing_fail_is_the_default_and_fails_the_run(module_level_mod, monkeypatch):
    monkeypatch.setattr(module_level_mod, "_run_agent", _fake_run_agent_with(["mini__open_issue"]))
    component = module_level_mod.SnowflakeCortexAgentComponent(
        asset_name="agent_out", prompt="hi", required_tool_calls=["mini__open_issue"],
    )
    assets = component.build_defs(None).assets
    with pytest.raises(Exception, match="required_tool_calls not satisfied"):
        dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)


def test_on_missing_warn_succeeds_and_reports_required_tools_missing(module_level_mod, monkeypatch):
    monkeypatch.setattr(module_level_mod, "_run_agent", _fake_run_agent_with(["mini__open_issue"]))
    component = module_level_mod.SnowflakeCortexAgentComponent(
        asset_name="agent_out", prompt="hi", required_tool_calls=["mini__open_issue"],
        on_required_tool_calls_missing="warn",
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    md = result.asset_materializations_for_node("agent_out")[0].metadata
    assert md["required_tools_missing"].value == ["mini__open_issue"]


def test_satisfied_never_raises_or_warns(module_level_mod, monkeypatch):
    monkeypatch.setattr(module_level_mod, "_run_agent", _fake_run_agent_with([]))
    component = module_level_mod.SnowflakeCortexAgentComponent(
        asset_name="agent_out", prompt="hi", required_tool_calls=["mini__open_issue"],
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
