"""mcp_server_resource_key: registers a second, MCPServerResource-shaped resource built
from this component's own settings, so any mcp_servers-aware component can reference this
toolset via server_resource_key instead of hand-writing an inline stdio command+env entry.
Same pattern as dagster_workflow_tools' own mcp_server_resource_key (including the
DAGSTER_CLOUD_DEPLOYMENT_NAME deployment-scoping fix in main())."""
import json
import sys

import pytest


def test_unset_mcp_server_resource_key_registers_only_the_existing_resource(mod):
    component = mod.DagsterInstanceToolsComponent(resource_key="dit")
    defs = component.build_defs(None)
    assert set(defs.resources) == {"dit"}


def test_mcp_server_resource_key_registers_stdio_mcp_shaped_resource(mod, world):
    component = mod.DagsterInstanceToolsComponent(
        resource_key="dit", mcp_server_resource_key="dit_mcp",
        allow=["asset_status", "asset_lineage"], max_result_chars=4000,
        definitions_module="fixture_defs",
    )
    defs = component.build_defs(None)
    assert set(defs.resources) == {"dit", "dit_mcp"}

    cfg = defs.resources["dit_mcp"].model_dump()
    assert cfg["name"] == "dit_mcp"
    assert cfg["type"] == "stdio"
    assert cfg["command"][:2] == [sys.executable, "-m"]
    assert cfg["command"][2] == mod.__spec__.name
    assert cfg["env"]["DAGSTER_INSTANCE_TOOLS_ALLOW"] == "asset_status,asset_lineage"
    assert cfg["env"]["DAGSTER_INSTANCE_TOOLS_MAX_CHARS"] == "4000"
    assert cfg["env"]["DAGSTER_INSTANCE_TOOLS_DEFINITIONS_MODULE"] == "fixture_defs"


def test_live_env_passthrough_field_not_leaked_into_model_dump_output(mod):
    component = mod.DagsterInstanceToolsComponent(resource_key="dit", mcp_server_resource_key="dit_mcp")
    cfg = component.build_defs(None).resources["dit_mcp"].model_dump()
    assert "live_env_passthrough" not in cfg


def test_dagster_home_and_deployment_name_read_fresh_at_model_dump_time(mod, world, monkeypatch):
    """Not baked in at build_defs (component load) time -- read fresh every model_dump() call."""
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    monkeypatch.delenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", raising=False)
    component = mod.DagsterInstanceToolsComponent(resource_key="dit", mcp_server_resource_key="dit_mcp")
    resource = component.build_defs(None).resources["dit_mcp"]
    assert "DAGSTER_HOME" not in resource.model_dump()["env"]
    assert "DAGSTER_CLOUD_DEPLOYMENT_NAME" not in resource.model_dump()["env"]

    monkeypatch.setenv("DAGSTER_HOME", str(world["home"]))
    monkeypatch.setenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "prod")
    cfg = resource.model_dump()
    assert cfg["env"]["DAGSTER_HOME"] == str(world["home"])
    assert cfg["env"]["DAGSTER_CLOUD_DEPLOYMENT_NAME"] == "prod"


def test_mcp_extra_env_vars_copied_fresh_at_model_dump_time(mod, monkeypatch):
    monkeypatch.delenv("DAGSTER_CLOUD_AGENT_LABEL", raising=False)
    component = mod.DagsterInstanceToolsComponent(
        resource_key="dit", mcp_server_resource_key="dit_mcp", mcp_extra_env_vars=["DAGSTER_CLOUD_AGENT_LABEL"],
    )
    resource = component.build_defs(None).resources["dit_mcp"]
    assert "DAGSTER_CLOUD_AGENT_LABEL" not in resource.model_dump()["env"]

    monkeypatch.setenv("DAGSTER_CLOUD_AGENT_LABEL", "my-agent")
    assert resource.model_dump()["env"]["DAGSTER_CLOUD_AGENT_LABEL"] == "my-agent"


def test_mcp_extra_env_vars_csv_string_coerced_to_list(mod, monkeypatch):
    monkeypatch.setenv("FOO", "bar")
    component = mod.DagsterInstanceToolsComponent(resource_key="dit", mcp_server_resource_key="dit_mcp", mcp_extra_env_vars="FOO")
    cfg = component.build_defs(None).resources["dit_mcp"].model_dump()
    assert cfg["env"]["FOO"] == "bar"


def test_live_env_passthrough_reaches_a_spawned_process_with_no_inherited_env(mod, monkeypatch):
    """Models the real failure mode directly: MCP's stdio client spawns the server with
    ONLY the resource's own env dict -- no inheritance from the parent process. Spawn a
    child process that way (env=spec['env'], completely replacing the environment) while
    the named var is set in THIS test process, and confirm the child can see it."""
    import subprocess

    monkeypatch.setenv("TEST_PASSTHROUGH_VAR", "reached-child")
    component = mod.DagsterInstanceToolsComponent(
        resource_key="dit", mcp_server_resource_key="dit_mcp", mcp_extra_env_vars=["TEST_PASSTHROUGH_VAR"],
    )
    spec = component.build_defs(None).resources["dit_mcp"].model_dump()
    result = subprocess.run(
        [sys.executable, "-c", "import os, sys; sys.stdout.write(os.environ.get('TEST_PASSTHROUGH_VAR', ''))"],
        env=spec["env"], capture_output=True, text=True, timeout=10,
    )
    assert result.stdout == "reached-child"


def test_main_scopes_instance_to_deployment_when_dagster_cloud_deployment_name_set(mod, monkeypatch):
    """Reproduces the real bug dagster_workflow_tools already hit: inside a Dagster+ agent
    process, dg.DagsterInstance.get() builds the AGENT's instance, which serves every
    deployment it manages -- main() must scope down to one via ref_for_deployment."""
    monkeypatch.setenv("DAGSTER_HOME", "/tmp/unused-for-this-test")
    monkeypatch.setenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "prod")

    scoped_instance = object()

    class _FakeAgentInstance:
        def ref_for_deployment(self, name):
            assert name == "prod"
            return "a-ref-for-prod"

    monkeypatch.setattr(mod.dg.DagsterInstance, "get", staticmethod(lambda: _FakeAgentInstance()))
    monkeypatch.setattr(mod.dg.DagsterInstance, "from_ref", staticmethod(lambda ref: scoped_instance if ref == "a-ref-for-prod" else None))

    captured = {}

    def fake_build_mcp_server(tools):
        captured["tools"] = tools

        class _FakeServer:
            def run(self, transport):
                pass
        return _FakeServer()

    monkeypatch.setattr(mod, "build_mcp_server", fake_build_mcp_server)
    mod.main()
    assert captured["tools"].instance is scoped_instance


def test_main_uses_instance_as_is_when_deployment_name_unset(mod, world, monkeypatch):
    monkeypatch.setenv("DAGSTER_HOME", str(world["home"]))
    monkeypatch.delenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", raising=False)
    monkeypatch.setattr(mod.dg.DagsterInstance, "get", staticmethod(lambda: world["instance"]))

    captured = {}

    def fake_build_mcp_server(tools):
        captured["tools"] = tools

        class _FakeServer:
            def run(self, transport):
                pass
        return _FakeServer()

    monkeypatch.setattr(mod, "build_mcp_server", fake_build_mcp_server)
    mod.main()
    assert captured["tools"].instance is world["instance"]


def test_mcp_server_shape_resource_passthrough_behaves_identically_to_dagster_workflow_tools(mod, monkeypatch):
    """Behavioral drift check: same fields, and model_dump() does the exact same
    live-env-passthrough merge given the same inputs."""
    import importlib.util
    import pathlib

    theirs_path = pathlib.Path(__file__).resolve().parent.parent.parent / "dagster_workflow_tools" / "component.py"
    spec = importlib.util.spec_from_file_location("dagster_workflow_tools_for_drift_check", theirs_path)
    theirs_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(theirs_mod)

    assert set(mod._McpServerShapeResource.model_fields) == set(theirs_mod._McpServerShapeResource.model_fields)

    monkeypatch.setenv("DRIFT_CHECK_VAR", "secret-value")
    kwargs = dict(name="x", type="stdio", command=["python"], env={"A": "1"}, live_env_passthrough=["DRIFT_CHECK_VAR", "UNSET_VAR"])
    mine = mod._McpServerShapeResource(**kwargs)
    theirs = theirs_mod._McpServerShapeResource(**kwargs)
    assert mine.model_dump() == theirs.model_dump() == {
        "name": "x", "type": "stdio", "command": ["python"], "url": None,
        "env": {"A": "1", "DRIFT_CHECK_VAR": "secret-value"}, "headers": None, "headers_env": None,
    }
