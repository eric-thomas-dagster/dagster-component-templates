"""mcp_server_resource_key: registers a second, MCPServerResource-shaped resource built from this component's own
settings, so any mcp_servers-aware component can reference this toolset via server_resource_key instead of
hand-writing an inline stdio command+env entry. Same pattern as dagster_instance_tools'/dagster_workflow_tools' own
mcp_server_resource_key."""
import sys


def test_unset_mcp_server_resource_key_registers_only_the_existing_resource(mod):
    component = mod.AgenticPipelineAuthoringToolsComponent(resource_key="apat")
    defs = component.build_defs(None)
    assert set(defs.resources) == {"apat"}


def test_mcp_server_resource_key_registers_stdio_mcp_shaped_resource(mod):
    component = mod.AgenticPipelineAuthoringToolsComponent(
        resource_key="apat", mcp_server_resource_key="apat_mcp",
        allow=["list_ops", "describe_op"], max_result_chars=5000,
    )
    defs = component.build_defs(None)
    assert set(defs.resources) == {"apat", "apat_mcp"}

    cfg = defs.resources["apat_mcp"].model_dump()
    assert cfg["name"] == "apat_mcp"
    assert cfg["type"] == "stdio"
    assert cfg["command"][:2] == [sys.executable, "-m"]
    assert cfg["command"][2] == mod.__spec__.name
    assert cfg["env"]["AGENTIC_PIPELINE_AUTHORING_TOOLS_ALLOW"] == "list_ops,describe_op"
    assert cfg["env"]["AGENTIC_PIPELINE_AUTHORING_TOOLS_MAX_CHARS"] == "5000"


def test_live_env_passthrough_field_not_leaked_into_model_dump_output(mod):
    component = mod.AgenticPipelineAuthoringToolsComponent(resource_key="apat", mcp_server_resource_key="apat_mcp")
    cfg = component.build_defs(None).resources["apat_mcp"].model_dump()
    assert "live_env_passthrough" not in cfg


def test_mcp_extra_env_vars_copied_fresh_at_model_dump_time(mod, monkeypatch):
    monkeypatch.delenv("SOME_EXTRA_VAR", raising=False)
    component = mod.AgenticPipelineAuthoringToolsComponent(
        resource_key="apat", mcp_server_resource_key="apat_mcp", mcp_extra_env_vars=["SOME_EXTRA_VAR"],
    )
    resource = component.build_defs(None).resources["apat_mcp"]
    assert "SOME_EXTRA_VAR" not in resource.model_dump()["env"]

    monkeypatch.setenv("SOME_EXTRA_VAR", "value")
    assert resource.model_dump()["env"]["SOME_EXTRA_VAR"] == "value"


def test_mcp_extra_env_vars_csv_string_coerced_to_list(mod, monkeypatch):
    monkeypatch.setenv("FOO", "bar")
    component = mod.AgenticPipelineAuthoringToolsComponent(resource_key="apat", mcp_server_resource_key="apat_mcp", mcp_extra_env_vars="FOO")
    cfg = component.build_defs(None).resources["apat_mcp"].model_dump()
    assert cfg["env"]["FOO"] == "bar"


def test_live_env_passthrough_reaches_a_spawned_process_with_no_inherited_env(mod, monkeypatch):
    """Models the real failure mode directly: MCP's stdio client spawns the server with ONLY the resource's own env
    dict -- no inheritance from the parent process. Spawn a child that way (env=spec['env'], completely replacing
    the environment) while the named var is set in THIS test process, and confirm the child can see it."""
    import subprocess

    monkeypatch.setenv("TEST_PASSTHROUGH_VAR", "reached-child")
    component = mod.AgenticPipelineAuthoringToolsComponent(
        resource_key="apat", mcp_server_resource_key="apat_mcp", mcp_extra_env_vars=["TEST_PASSTHROUGH_VAR"],
    )
    spec = component.build_defs(None).resources["apat_mcp"].model_dump()
    result = subprocess.run(
        [sys.executable, "-c", "import os, sys; sys.stdout.write(os.environ.get('TEST_PASSTHROUGH_VAR', ''))"],
        env=spec["env"], capture_output=True, text=True, timeout=10,
    )
    assert result.stdout == "reached-child"
