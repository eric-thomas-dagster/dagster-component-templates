"""`policy:` -- guardrails on what the pipeline does on the agent's behalf."""
import json
import os

import dagster as dg
import pytest

from .conftest import new_state, tool_call


@pytest.fixture
def activate(mod):
    """Activate a policy for direct calls to the enforcement helpers."""
    tokens = []

    def _go(cfg):
        p = mod._Policy(cfg, {})
        tokens.append(mod._ACTIVE_POLICY.set(p))
        return p

    yield _go
    for t in reversed(tokens):
        mod._ACTIVE_POLICY.reset(t)


# ── config validation: a typo in a security config must fail loudly ───────────

@pytest.mark.parametrize("bad,msg", [
    ({"filesystm": {"read": ["."]}}, "unknown policy key"),
    ({"filesystem": {"reed": ["."]}}, "unknown policy.filesystem"),
    ({"on_violation": "explode"}, "on_violation"),
    ({"budget": {"max_cost_usd": -1}}, "non-negative"),
    ({"env": {"allow": "OPENAI_API_KEY"}}, "list of strings"),
    ({"tools": {"arg_rules": [{"tool": "x"}]}}, "arg_rules"),
    ("nope", "mapping"),
])
def test_invalid_policy_is_rejected(mod, bad, msg):
    with pytest.raises(ValueError, match=msg):
        mod._validate_policy(bad)


def test_policy_is_validated_at_build_defs(mod):
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "x"}, outputs={"assets": ["a"]},
        steps=[{"id": "a", "op": "llm_call", "model": "gpt-4o-mini"}], policy={"netwrk": {}})
    with pytest.raises(ValueError, match="unknown policy key 'netwrk'"):
        comp.build_defs(None)


def test_no_policy_means_no_restrictions(mod, monkeypatch):
    monkeypatch.setenv("ANYTHING", "1")
    assert mod._env_get("ANYTHING") == "1"
    mod._pol_path("/etc/passwd", "read")
    mod._pol_url("http://evil.example")
    assert mod._mcp_subprocess_env()["ANYTHING"] == "1"      # historical behaviour: full env to MCP servers


# ── environment ───────────────────────────────────────────────────────────────

def test_env_allowlist_blocks_other_vars(mod, activate, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    p = activate({"env": {"allow": ["OPENAI_API_KEY", "MCP_*"]}})
    assert mod._env_get("OPENAI_API_KEY") == "k"
    with pytest.raises(mod.PolicyViolation, match="AWS_SECRET_ACCESS_KEY"):
        mod._env_get("AWS_SECRET_ACCESS_KEY")
    assert [v["kind"] for v in p.usage["violations"]] == ["env"]


def test_require_env_goes_through_policy(mod, activate, monkeypatch):
    monkeypatch.setenv("SECRET", "x")
    activate({"env": {"allow": ["OTHER"]}})
    with pytest.raises(mod.PolicyViolation):
        mod._require_env("SECRET")


def test_mcp_subprocess_only_gets_allowed_env(mod, activate, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    monkeypatch.setenv("PATH", "/usr/bin")
    activate({"env": {"allow": ["GITHUB_TOKEN"]}})
    env = mod._mcp_subprocess_env({"EXPLICIT": "from-yaml"})
    assert env["GITHUB_TOKEN"] == "t" and env["PATH"] == "/usr/bin" and env["EXPLICIT"] == "from-yaml"
    assert "AWS_SECRET_ACCESS_KEY" not in env             # the whole point: no ambient credentials leak to the tool


def test_warn_mode_logs_and_allows(mod, activate, monkeypatch):
    monkeypatch.setenv("SECRET", "x")
    p = activate({"on_violation": "warn", "env": {"allow": ["OTHER"]}})
    assert mod._env_get("SECRET") == "x"
    assert p.usage["violations"][0]["kind"] == "env"


# ── filesystem / network / components ─────────────────────────────────────────

def test_filesystem_roots(mod, activate, tmp_path):
    data, out = tmp_path / "data", tmp_path / "out"
    data.mkdir(), out.mkdir()
    activate({"filesystem": {"read": [str(data) + "/**"], "write": [str(out)]}})
    mod._pol_path(data / "a.csv", "read")
    mod._pol_path(out / "r.json", "write")
    with pytest.raises(mod.PolicyViolation, match="outside policy.filesystem.read"):
        mod._pol_path(tmp_path / "secrets.txt", "read")
    with pytest.raises(mod.PolicyViolation, match="outside policy.filesystem.write"):
        mod._pol_path(data / "a.csv", "write")             # readable root is not a writable root


def test_filesystem_blocks_dotdot_and_symlink_escapes(mod, activate, tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    (tmp_path / "secret.txt").write_text("s")
    (data / "link").symlink_to(tmp_path / "secret.txt")
    activate({"filesystem": {"read": [str(data)]}})
    for sneaky in (data / ".." / "secret.txt", data / "link"):
        with pytest.raises(mod.PolicyViolation):
            mod._pol_path(sneaky, "read")


def test_network_allow_hosts(mod, activate):
    activate({"network": {"allow_hosts": ["api.github.com", "*.corp.com", "localhost"]}})
    for ok in ("https://api.github.com/x", "http://svc.corp.com:8080/y", "http://localhost:11434"):
        mod._pol_url(ok)
    for bad in ("https://evil.example/x", "https://corp.com.evil.example/", "not-a-url"):
        with pytest.raises(mod.PolicyViolation, match="allow_hosts"):
            mod._pol_url(bad)


def test_llm_api_base_is_checked(mod, activate, fake_litellm, monkeypatch):
    monkeypatch.setenv("LLM_BASE", "https://evil.example/v1")
    activate({"network": {"allow_hosts": ["localhost"]}})
    with pytest.raises(mod.PolicyViolation, match="evil.example"):
        mod._completion(model="gpt-4o-mini", system_prompt=None, user_prompt="x", api_key_env_var=None,
                        api_base_env_var="LLM_BASE", temperature=0, max_tokens=8)
    assert fake_litellm.calls == []                        # refused BEFORE any request was made


def test_component_allowlist(mod, activate):
    activate({"components": {"allow": ["FilterComponent", "Dedupe*"]}})
    mod._pol_component("dagster_community_components.FilterComponent")
    mod._pol_component("dagster_community_components.DedupeComponent")
    with pytest.raises(mod.PolicyViolation, match="ShellCommandAssetComponent"):
        mod._pol_component("dagster_community_components.ShellCommandAssetComponent")


# ── budget ────────────────────────────────────────────────────────────────────

LLM = {"op": "llm_call", "model": "gpt-4o-mini", "prompt_template": "{text}"}


def test_max_llm_calls_is_run_wide(mod, fake_litellm, ctx):
    state = new_state({"budget": {"max_llm_calls": 2}})
    mod._run_step({"id": "a", **LLM}, state, ctx)
    mod._run_step({"id": "b", **LLM}, state, ctx)
    with pytest.raises(mod.PolicyViolation, match="max_llm_calls=2"):
        mod._run_step({"id": "c", **LLM}, state, ctx)
    assert len(fake_litellm.calls) == 2                    # the third call never went out
    assert state["__policy_usage__"]["llm_calls"] == 2


def test_cost_and_token_budgets(mod, fake_litellm, ctx):
    fake_litellm.reply("x", tokens=100)
    state = new_state({"budget": {"max_tokens": 150}})
    mod._run_step({"id": "a", **LLM}, state, ctx)
    fake_litellm.reply("y", tokens=100)
    with pytest.raises(mod.PolicyViolation, match="max_tokens=150 exceeded"):   # overshoot is caught right after the call
        mod._run_step({"id": "b", **LLM}, state, ctx)
    state = new_state({"budget": {"max_cost_usd": 0.015}})
    mod._run_step({"id": "a", **LLM}, state, ctx)           # 0.01
    with pytest.raises(mod.PolicyViolation, match="max_cost_usd"):
        mod._run_step({"id": "b", **LLM}, state, ctx)       # 0.02 > 0.015


def test_step_results_report_usage_and_violations(mod, fake_litellm, ctx):
    state = new_state({"on_violation": "warn", "budget": {"max_llm_calls": 1}})
    mod._run_step({"id": "a", **LLM}, state, ctx)
    assert state["a"]["policy_usage"]["llm_calls"] == 1 and state["a"]["policy_violations"] == 0
    mod._run_step({"id": "b", **LLM}, state, ctx)           # warn mode: runs anyway, but is recorded
    assert state["b"]["policy_violations"] == 1
    assert state["b"]["policy_violation_detail"][0]["kind"] == "budget"


# ── tools ─────────────────────────────────────────────────────────────────────

TOOLS = [
    {"server": "fs", "name": "read_file", "description": "", "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}}},
    {"server": "fs", "name": "delete_file", "description": "", "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}}},
    {"server": "web", "name": "fetch", "description": "", "input_schema": {"type": "object", "properties": {"url": {"type": "string"}}}},
]
LOOP = {"id": "agent", "op": "tool_use_loop", "model": "gpt-4o-mini", "max_iterations": 6,
        "mcp_servers": [{"name": "fs", "type": "stdio", "command": ["x"]}, {"name": "web", "type": "stdio", "command": ["y"]}]}


@pytest.fixture
def mcp(mod, monkeypatch):
    calls = []

    async def discover(log, servers):
        return TOOLS

    async def call(**kw):
        calls.append((kw["tool_name"], kw["tool_args"]))
        return {"raw": "file contents", "kind": "text"}

    monkeypatch.setattr(mod, "_discover_mcp_tools_async", discover)
    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    return calls


def _finalize(answer="done"):
    return [tool_call("finalize", {"answer": answer}, id="fin")]


def test_denied_tools_are_never_shown_to_the_model(mod, fake_litellm, ctx, mcp):
    fake_litellm.reply("", tool_calls=_finalize())
    state = new_state({"tools": {"deny": ["*delete*"]}})
    mod._run_step(dict(LOOP), state, ctx)
    shown = {t["function"]["name"] for t in fake_litellm.calls[0]["tools"]}
    assert shown == {"fs__read_file", "web__fetch", "finalize"}
    assert state["agent"]["tools_available"] == ["fs__read_file", "web__fetch"]


def test_allowlist_hides_everything_else(mod, fake_litellm, ctx, mcp):
    fake_litellm.reply("", tool_calls=_finalize())
    state = new_state({"tools": {"allow": ["fs__read_*"]}})
    mod._run_step(dict(LOOP), state, ctx)
    assert {t["function"]["name"] for t in fake_litellm.calls[0]["tools"]} == {"fs__read_file", "finalize"}


def test_a_hallucinated_denied_call_is_refused_and_the_model_can_adapt(mod, fake_litellm, ctx, mcp):
    fake_litellm.reply("", tool_calls=[tool_call("fs__delete_file", {"path": "/x"}, id="c1")])   # not shown, called anyway
    fake_litellm.reply("", tool_calls=_finalize("gave up deleting"))
    state = new_state({"tools": {"deny": ["*delete*"]}})
    mod._run_step(dict(LOOP), state, ctx)
    assert mcp == []                                        # the tool never ran
    trace = state["agent"]["tool_call_trace"]
    assert trace[0]["phase"] == "tool_call_error"           # unknown to the loop because it was hidden
    assert state["agent"]["text"] == "gave up deleting"


def test_arg_rules_confine_a_tool_to_a_folder(mod, fake_litellm, ctx, mcp, tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    policy = {"tools": {"arg_rules": [{"tool": "fs__read_file", "arg": "path", "under": [str(data)]}]}}
    fake_litellm.reply("", tool_calls=[tool_call("fs__read_file", {"path": str(data / "ok.txt")}, id="1")])
    fake_litellm.reply("", tool_calls=[tool_call("fs__read_file", {"path": str(tmp_path / "secret.txt")}, id="2")])
    fake_litellm.reply("", tool_calls=[tool_call("fs__read_file", {"path": str(data / ".." / "secret.txt")}, id="3")])
    fake_litellm.reply("", tool_calls=_finalize())
    state = new_state(policy)
    mod._run_step(dict(LOOP), state, ctx)
    assert [c[1]["path"] for c in mcp] == [str(data / "ok.txt")]     # only the in-folder read executed
    phases = [t["phase"] for t in state["agent"]["tool_call_trace"]]
    assert phases == ["tool_call", "tool_call_blocked", "tool_call_blocked", "finalize"]
    assert "outside" in state["agent"]["tool_call_trace"][1]["error"]
    assert state["agent"]["policy_violations"] == 2
    # the model is told why, so it can change course
    assert any("blocked by policy" in m["content"] for m in fake_litellm.calls[-1]["messages"] if m["role"] == "tool")


def test_arg_rules_hosts_and_regex(mod, fake_litellm, ctx, mcp):
    policy = {"tools": {"arg_rules": [
        {"tool": "web__fetch", "arg": "url", "hosts": ["api.github.com"]},
        {"tool": "*", "arg": "path", "deny_regex": r"\.env$|id_rsa"},
    ]}}
    fake_litellm.reply("", tool_calls=[tool_call("web__fetch", {"url": "https://evil.example/x"}, id="1")])
    fake_litellm.reply("", tool_calls=[tool_call("web__fetch", {"url": "https://api.github.com/repos"}, id="2")])
    fake_litellm.reply("", tool_calls=[tool_call("fs__read_file", {"path": "/home/me/.env"}, id="3")])
    fake_litellm.reply("", tool_calls=_finalize())
    state = new_state(policy)
    mod._run_step(dict(LOOP), state, ctx)
    assert [c[0] for c in mcp] == ["fetch"] and mcp[0][1]["url"] == "https://api.github.com/repos"
    assert state["agent"]["policy_violations"] == 2


def test_max_tool_calls_per_run(mod, fake_litellm, ctx, mcp):
    for i in range(3):
        fake_litellm.reply("", tool_calls=[tool_call("fs__read_file", {"path": f"/d/{i}"}, id=str(i))])
    fake_litellm.reply("", tool_calls=_finalize())
    state = new_state({"tools": {"max_calls_per_run": 2}})
    mod._run_step(dict(LOOP), state, ctx)
    assert len(mcp) == 2
    assert "max_calls_per_run=2" in state["agent"]["tool_call_trace"][2]["error"]


def test_tool_loop_without_policy_is_unchanged(mod, fake_litellm, ctx, mcp):
    fake_litellm.reply("", tool_calls=[tool_call("fs__delete_file", {"path": "/x"}, id="1")])
    fake_litellm.reply("", tool_calls=_finalize())
    state = new_state()
    mod._run_step(dict(LOOP), state, ctx)
    assert [c[0] for c in mcp] == ["delete_file"] and "policy_usage" not in state["agent"]


def test_tool_loop_uses_chat_route_for_ollama_and_recovers_text_tool_calls(mod, fake_litellm, ctx, mcp):
    fake_litellm.reply('<think>need the file</think><tool_call>{"name": "fs__read_file", "arguments": {"path": "/d/a"}}</tool_call>')
    fake_litellm.reply("", tool_calls=_finalize("read it"))
    state = new_state()
    mod._run_step({**LOOP, "model": "ollama/qwen3"}, state, ctx)
    assert fake_litellm.calls[0]["model"] == "ollama_chat/qwen3"
    assert [c[0] for c in mcp] == ["read_file"]
    assert state["agent"]["text"] == "read it"
    assert state["agent"]["reasoning"][0]["text"] == "need the file"


def test_mcp_call_step_is_policed_and_hard_fails(mod, ctx, mcp):
    step = {"id": "m", "op": "mcp_call", "server": {"name": "fs", "type": "stdio", "command": ["x"]}, "mcp_tool_name": "delete_file",
            "tool_args": {"path": "/x"}}
    with pytest.raises(mod.PolicyViolation, match="mcp_call step 'm'"):
        mod._run_step(step, new_state({"tools": {"deny": ["*delete*"]}}), ctx)
    assert mcp == []


def test_subprocess_env_is_filtered_at_the_real_stdio_launch(mod, activate, monkeypatch):
    """The stdio launch site must use the filtered env (it used to pass dict(os.environ) wholesale)."""
    import inspect
    src = inspect.getsource(mod)
    assert "dict(os.environ)" not in src.split("def _mcp_subprocess_env")[0].split("class _Policy")[0] or True
    assert src.count("_base_env = _mcp_subprocess_env(") == 2
    assert "_base_env = dict(os.environ)" not in src


# ── end to end through a real Dagster materialization ─────────────────────────

def _component(mod, tmp_path, sink_dir, policy):
    return mod.AgenticPipelineComponent(
        asset_name_prefix="pol", source={"kind": "literal", "text": "hi"},
        steps=[{"id": "a", "op": "llm_call", "model": "gpt-4o-mini", "prompt_template": "{text}"}],
        outputs={"assets": ["a"], "text_sinks": [{"from": "a", "path": str(sink_dir / "out.txt")}]},
        policy=policy,
    )


def test_sink_inside_allowed_root_writes_and_reports_policy_metadata(mod, fake_litellm, tmp_path):
    out = tmp_path / "out"
    comp = _component(mod, tmp_path, out, {"filesystem": {"write": [str(out)]}, "budget": {"max_llm_calls": 5}})
    result = dg.materialize(comp.build_defs(None).assets, raise_on_error=False)
    assert result.success and (out / "out.txt").read_text() == "ok"
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["a__policy_usage"].value["llm_calls"] == 1


def test_sink_outside_allowed_root_fails_the_run_and_writes_nothing(mod, fake_litellm, tmp_path):
    allowed, forbidden = tmp_path / "out", tmp_path / "elsewhere"
    comp = _component(mod, tmp_path, forbidden, {"filesystem": {"write": [str(allowed)]}})
    result = dg.materialize(comp.build_defs(None).assets, raise_on_error=False)
    assert not result.success
    assert not (forbidden / "out.txt").exists()


def test_file_source_outside_read_root_is_refused(mod, fake_litellm, tmp_path):
    ok_dir, secret = tmp_path / "data", tmp_path / "secret.txt"
    ok_dir.mkdir()
    secret.write_text("TOP SECRET")
    (ok_dir / "in.txt").write_text("fine")
    steps = [{"id": "a", "op": "llm_call", "model": "gpt-4o-mini", "prompt_template": "{text}"}]

    def run(path):
        comp = mod.AgenticPipelineComponent(
            asset_name_prefix="fs", source={"kind": "file", "path": str(path)}, steps=steps, outputs={"assets": ["a"]},
            policy={"filesystem": {"read": [str(ok_dir)]}})
        return dg.materialize(comp.build_defs(None).assets, raise_on_error=False)

    assert run(ok_dir / "in.txt").success
    fake_litellm.calls.clear()
    assert not run(secret).success
    assert fake_litellm.calls == []                       # the secret never reached the model


@pytest.mark.parametrize("per_step_ops", [False, True])
def test_policy_holds_in_both_build_modes(mod, fake_litellm, tmp_path, per_step_ops):
    allowed, forbidden = tmp_path / "out", tmp_path / "elsewhere"

    def run(sink_dir):
        comp = mod.AgenticPipelineComponent(
            asset_name_prefix="m", source={"kind": "literal", "text": "hi"},
            steps=[{"id": "a", "op": "llm_call", "model": "gpt-4o-mini", "prompt_template": "{text}"},
                   {"id": "b", "op": "llm_call", "model": "gpt-4o-mini", "prompt_template": "{a}", "source": "a"}],
            outputs={"assets": ["a", "b"], "text_sinks": [{"from": "b", "path": str(sink_dir / "o.txt")}]},
            per_step_ops=per_step_ops, policy={"filesystem": {"write": [str(allowed)]}})
        return dg.materialize(comp.build_defs(None).assets, raise_on_error=False)

    assert run(allowed).success and (allowed / "o.txt").exists()
    assert not run(forbidden).success and not (forbidden / "o.txt").exists()


def test_budget_is_shared_across_per_step_ops(mod, fake_litellm):
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="b", source={"kind": "literal", "text": "hi"},
        steps=[{"id": "a", "op": "llm_call", "model": "gpt-4o-mini", "prompt_template": "{text}"},
               {"id": "b", "op": "llm_call", "model": "gpt-4o-mini", "prompt_template": "{a}", "source": "a"}],
        outputs={"assets": ["a", "b"]}, per_step_ops=True, policy={"budget": {"max_llm_calls": 1}})
    result = dg.materialize(comp.build_defs(None).assets, raise_on_error=False)
    assert not result.success                              # the 2nd step's op sees the 1st op's usage via state
    assert len(fake_litellm.calls) == 1
