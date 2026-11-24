"""required_tool_calls -- a real failure mode observed twice in production: the model writes
findings and answers "to be filed" without actually calling the filing tool.

Two layers tested separately:
- _run_agent's own reminder-and-retry loop: a real (not mocked) stdio MCP round trip, only the
  paid AsyncOpenAI.chat.completions.create call faked (Vercel AI Gateway is OpenAI-compatible),
  via the real stdio server fixture from test_mcp_real_server. _run_agent itself only ever
  REPORTS required_tools_missing; it never raises (see the comment at its own call site for why).
- The asset body's on_required_tool_calls_missing="fail"/"warn" decision: _run_agent faked
  directly (same convention as the other agent components' own tests) through a real
  dg.materialize round trip -- also confirms the fallback-model try/except doesn't swallow it.
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
        return {"role": "assistant", "content": self.content}


def _install_scripted_openai(monkeypatch, turns):
    """turns: a list of (content, tool_name_or_None). The Nth create() call plays turns[N-1]
    (clamped to the last entry once exhausted)."""
    state = {"n": 0}

    class _Completions:
        async def create(self, **kw):
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
            return types.SimpleNamespace(choices=[types.SimpleNamespace(
                message=msg, finish_reason="tool_calls" if msg.tool_calls else "stop",
            )])

    class AsyncOpenAI:
        def __init__(self, **kw):
            self.chat = types.SimpleNamespace(completions=_Completions())

    fake = types.ModuleType("openai")
    fake.AsyncOpenAI = AsyncOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake)
    monkeypatch.setenv("FAKE_KEY", "sk-fake")


_EXTRA = dict(api_key_env_var="FAKE_KEY", base_url_env_var=None, organization_env_var=None, api_base_url="http://unused.invalid", model="openai/m")


# ── _run_agent's own reminder-and-retry loop (real stdio MCP round trip) ────────────────────────

def test_reminder_gives_the_model_a_second_chance_and_it_calls_the_tool(mod, stdio_cfg, monkeypatch):
    _install_scripted_openai(monkeypatch, [
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
    _install_scripted_openai(monkeypatch, [
        ("Findings written, to be filed.", None),
        ("Still nothing called.", None),
    ])
    result = _run(mod._run_agent(**_call_kwargs(mod._run_agent, stdio_cfg, required_tool_calls=["mini__echo"], **_EXTRA)))
    assert result["required_tools_missing"] == ["mini__echo"]
    assert result["stopped_reason"] == "final_answer"


def test_any_of_group_satisfied_by_either_tool_even_if_the_call_errored(mod, stdio_cfg, monkeypatch):
    _install_scripted_openai(monkeypatch, [
        ("will call boom", "mini__boom"),
        ("done", None),
    ])
    result = _run(mod._run_agent(**_call_kwargs(mod._run_agent, stdio_cfg, required_tool_calls=[["mini__echo", "mini__boom"]], **_EXTRA)))
    assert result["required_tools_missing"] == []
    assert result["tool_call_details"][0]["is_error"] is True


def test_already_satisfied_before_trying_to_stop_never_sends_a_reminder(mod, stdio_cfg, monkeypatch):
    _install_scripted_openai(monkeypatch, [
        ("calling it", "mini__echo"),
        ("done", None),
    ])
    result = _run(mod._run_agent(**_call_kwargs(mod._run_agent, stdio_cfg, required_tool_calls=["mini__echo"], **_EXTRA)))
    assert result["required_tools_missing"] == []
    assert not any("Before finishing" in (m.get("content") or "") for m in result["transcript"] if m.get("role") == "user")


def test_no_required_tool_calls_means_no_new_behavior(mod, stdio_cfg, monkeypatch):
    _install_scripted_openai(monkeypatch, [("done", None)])
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


def test_on_missing_fail_is_the_default_and_fails_the_run_not_caught_by_fallback(module_level_mod, monkeypatch):
    """The fallback-model try/except must not swallow this as a 'model failed, try the next
    one' -- it's a policy violation, not a provider failure."""
    monkeypatch.setattr(module_level_mod, "_run_agent", _fake_run_agent_with(["mini__open_issue"]))
    component = module_level_mod.VercelAIGatewayAgentComponent(
        asset_name="agent_out", prompt="hi", required_tool_calls=["mini__open_issue"],
    )
    assets = component.build_defs(None).assets
    with pytest.raises(Exception, match="required_tool_calls not satisfied"):
        dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)


def test_on_missing_warn_succeeds_and_reports_required_tools_missing(module_level_mod, monkeypatch):
    monkeypatch.setattr(module_level_mod, "_run_agent", _fake_run_agent_with(["mini__open_issue"]))
    component = module_level_mod.VercelAIGatewayAgentComponent(
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
    component = module_level_mod.VercelAIGatewayAgentComponent(
        asset_name="agent_out", prompt="hi", required_tool_calls=["mini__open_issue"],
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
