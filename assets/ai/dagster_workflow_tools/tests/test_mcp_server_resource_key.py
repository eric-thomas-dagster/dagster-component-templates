"""mcp_server_resource_key: registers a second, MCPServerResource-shaped resource built
from this component's own settings, so any mcp_servers-aware component can reference this
toolset via server_resource_key instead of hand-writing an inline stdio command+env entry."""
import json
import sys

import pytest


def test_unset_mcp_server_resource_key_registers_only_the_existing_resource(mod, instance, defs_module_spec):
    component = mod.DagsterWorkflowToolsComponent(resource_key="wf_tools", definitions_module=defs_module_spec)
    defs = component.build_defs(None)
    assert set(defs.resources) == {"wf_tools"}


def test_build_defs_never_reads_dagster_home_at_build_time(mod, defs_module_spec, monkeypatch):
    """DAGSTER_HOME is read at run time (model_dump() call), not at component load --
    build_defs must succeed even when it's completely unset."""
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    component = mod.DagsterWorkflowToolsComponent(
        resource_key="wf_tools", definitions_module=defs_module_spec, mcp_server_resource_key="wf_mcp",
    )
    defs = component.build_defs(None)  # must not raise
    cfg = defs.resources["wf_mcp"].model_dump()
    assert "DAGSTER_HOME" not in cfg["env"]


def test_dagster_home_is_read_fresh_at_model_dump_time_not_build_time(mod, instance, defs_module_spec, monkeypatch):
    """The resource object is built once (component load); DAGSTER_HOME in its model_dump()
    output must reflect whatever's in the environment at CALL time, even if that's set
    AFTER build_defs already ran -- proving it's genuinely read at run time."""
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    component = mod.DagsterWorkflowToolsComponent(
        resource_key="wf_tools", definitions_module=defs_module_spec, mcp_server_resource_key="wf_mcp",
    )
    defs = component.build_defs(None)
    resource = defs.resources["wf_mcp"]
    assert "DAGSTER_HOME" not in resource.model_dump()["env"]

    monkeypatch.setenv("DAGSTER_HOME", instance.root_directory)
    assert resource.model_dump()["env"]["DAGSTER_HOME"] == instance.root_directory


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
    assert cfg["command"][:2] == [sys.executable, "-m"]
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


def test_mcp_extra_env_vars_copied_fresh_at_model_dump_time(mod, instance, defs_module_spec, monkeypatch):
    """A Dagster+ agent's instance config commonly needs more than DAGSTER_HOME (e.g.
    DAGSTER_CLOUD_AGENT_LABEL) -- mcp_extra_env_vars names extra vars to forward, read
    fresh every time model_dump() is called, same as DAGSTER_HOME itself."""
    monkeypatch.setenv("DAGSTER_HOME", instance.root_directory)
    monkeypatch.delenv("DAGSTER_CLOUD_AGENT_LABEL", raising=False)
    component = mod.DagsterWorkflowToolsComponent(
        resource_key="wf_tools", definitions_module=defs_module_spec, mcp_server_resource_key="wf_mcp",
        mcp_extra_env_vars=["DAGSTER_CLOUD_AGENT_LABEL"],
    )
    resource = component.build_defs(None).resources["wf_mcp"]
    assert "DAGSTER_CLOUD_AGENT_LABEL" not in resource.model_dump()["env"]

    monkeypatch.setenv("DAGSTER_CLOUD_AGENT_LABEL", "my-agent")
    cfg = resource.model_dump()
    assert cfg["env"]["DAGSTER_CLOUD_AGENT_LABEL"] == "my-agent"
    assert cfg["env"]["DAGSTER_HOME"] == instance.root_directory


def test_mcp_extra_env_vars_csv_string_coerced_to_list(mod, instance, defs_module_spec, monkeypatch):
    monkeypatch.setenv("DAGSTER_HOME", instance.root_directory)
    monkeypatch.setenv("FOO", "bar")
    component = mod.DagsterWorkflowToolsComponent(
        resource_key="wf_tools", definitions_module=defs_module_spec, mcp_server_resource_key="wf_mcp",
        mcp_extra_env_vars="FOO",
    )
    cfg = component.build_defs(None).resources["wf_mcp"].model_dump()
    assert cfg["env"]["FOO"] == "bar"


def test_live_env_passthrough_field_not_leaked_into_model_dump_output(mod, instance, defs_module_spec, monkeypatch):
    monkeypatch.setenv("DAGSTER_HOME", instance.root_directory)
    component = mod.DagsterWorkflowToolsComponent(
        resource_key="wf_tools", definitions_module=defs_module_spec, mcp_server_resource_key="wf_mcp",
    )
    cfg = component.build_defs(None).resources["wf_mcp"].model_dump()
    assert "live_env_passthrough" not in cfg
