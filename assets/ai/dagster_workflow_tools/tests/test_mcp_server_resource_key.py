"""mcp_server_resource_key: registers a second, MCPServerResource-shaped resource built
from this component's own settings, so any mcp_servers-aware component can reference this
toolset via server_resource_key instead of hand-writing an inline stdio command+env entry."""
import json

import pytest


def test_unset_mcp_server_resource_key_registers_only_the_existing_resource(mod, instance, defs_module_spec):
    component = mod.DagsterWorkflowToolsComponent(resource_key="wf_tools", definitions_module=defs_module_spec)
    defs = component.build_defs(None)
    assert set(defs.resources) == {"wf_tools"}


def test_mcp_server_resource_key_requires_dagster_home_in_env(mod, defs_module_spec, monkeypatch):
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    component = mod.DagsterWorkflowToolsComponent(
        resource_key="wf_tools", definitions_module=defs_module_spec, mcp_server_resource_key="wf_mcp",
    )
    with pytest.raises(ValueError, match="DAGSTER_HOME"):
        component.build_defs(None)


def test_mcp_server_resource_key_registers_stdio_mcp_shaped_resource(mod, instance, defs_module_spec, monkeypatch):
    monkeypatch.setenv("DAGSTER_HOME", instance.root_directory)
    component = mod.DagsterWorkflowToolsComponent(
        resource_key="wf_tools", definitions_module=defs_module_spec, mcp_server_resource_key="wf_mcp",
    )
    defs = component.build_defs(None)
    assert set(defs.resources) == {"wf_tools", "wf_mcp"}

    mcp_resource = defs.resources["wf_mcp"]
    cfg = mcp_resource.model_dump()
    assert cfg["name"] == "wf_mcp"
    assert cfg["type"] == "stdio"
    assert cfg["command"][:2] == ["python", "-m"]
    assert cfg["command"][2] == mod.__spec__.name  # the real importable module path in this process
    assert cfg["env"]["DAGSTER_HOME"] == instance.root_directory
    assert cfg["env"]["DAGSTER_WORKFLOW_TOOLS_DEFINITIONS_MODULE"] == defs_module_spec
    assert "DAGSTER_WORKFLOW_TOOLS_ALLOW" not in cfg["env"]


def test_mcp_server_resource_key_env_includes_allow_lists_and_default_tags(mod, instance, defs_module_spec, monkeypatch):
    monkeypatch.setenv("DAGSTER_HOME", instance.root_directory)
    component = mod.DagsterWorkflowToolsComponent(
        resource_key="wf_tools", definitions_module=defs_module_spec, mcp_server_resource_key="wf_mcp",
        allow=["materialize_assets"], allow_assets=["a"], allow_jobs=["named_job"],
        default_tags={"triggered_by": "support-agent"},
    )
    defs = component.build_defs(None)
    env = defs.resources["wf_mcp"].model_dump()["env"]
    assert env["DAGSTER_WORKFLOW_TOOLS_ALLOW"] == "materialize_assets"
    assert env["DAGSTER_WORKFLOW_TOOLS_ALLOW_ASSETS"] == "a"
    assert env["DAGSTER_WORKFLOW_TOOLS_ALLOW_JOBS"] == "named_job"
    assert json.loads(env["DAGSTER_WORKFLOW_TOOLS_DEFAULT_TAGS"]) == {"triggered_by": "support-agent"}


def test_main_reads_default_tags_from_env(mod, instance, defs_module_spec, monkeypatch):
    """Full settings parity check: the MCP-entrypoint path (main()) must support every
    setting the in-process resource_key path supports, including default_tags."""
    monkeypatch.setenv("DAGSTER_HOME", instance.root_directory)
    monkeypatch.setenv("DAGSTER_WORKFLOW_TOOLS_DEFINITIONS_MODULE", defs_module_spec)
    monkeypatch.setenv("DAGSTER_WORKFLOW_TOOLS_DEFAULT_TAGS", json.dumps({"triggered_by": "support-agent"}))
    monkeypatch.setattr(mod.dg.DagsterInstance, "get", staticmethod(lambda: instance))

    captured = {}

    def fake_build_mcp_server(tools):
        captured["tools"] = tools

        class _FakeServer:
            def run(self, transport):
                pass
        return _FakeServer()

    monkeypatch.setattr(mod, "build_mcp_server", fake_build_mcp_server)
    mod.main()
    assert captured["tools"].default_tags == {"triggered_by": "support-agent"}
