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


def test_dagster_cloud_deployment_name_passed_through_automatically(mod, instance, defs_module_spec, monkeypatch):
    """No need to list DAGSTER_CLOUD_DEPLOYMENT_NAME in mcp_extra_env_vars by hand -- it's
    forwarded the same way DAGSTER_HOME already is, since main() needs it to scope a
    Dagster+ agent's multi-deployment instance down to one deployment."""
    monkeypatch.setenv("DAGSTER_HOME", instance.root_directory)
    monkeypatch.delenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", raising=False)
    component = mod.DagsterWorkflowToolsComponent(
        resource_key="wf_tools", definitions_module=defs_module_spec, mcp_server_resource_key="wf_mcp",
    )
    resource = component.build_defs(None).resources["wf_mcp"]
    assert "DAGSTER_CLOUD_DEPLOYMENT_NAME" not in resource.model_dump()["env"]

    monkeypatch.setenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "prod")
    cfg = resource.model_dump()
    assert cfg["env"]["DAGSTER_CLOUD_DEPLOYMENT_NAME"] == "prod"


def test_main_scopes_instance_to_deployment_when_dagster_cloud_deployment_name_set(mod, defs_module_spec, monkeypatch):
    """Reproduces the real bug: inside a Dagster+ agent process, dg.DagsterInstance.get()
    builds the AGENT's own instance, which serves every deployment it manages -- launching
    a run against it fails with "Organization has multiple deployments, but no deployment
    specified". main() must call ref_for_deployment(name) to scope down to one."""
    monkeypatch.setenv("DAGSTER_HOME", "/tmp/unused-for-this-test")
    monkeypatch.setenv("DAGSTER_WORKFLOW_TOOLS_DEFINITIONS_MODULE", defs_module_spec)
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


def test_main_uses_instance_as_is_when_deployment_name_unset(mod, instance, defs_module_spec, monkeypatch):
    """Local dev / a non-Dagster+ instance: no DAGSTER_CLOUD_DEPLOYMENT_NAME, so main() must
    not attempt to scope anything -- the plain instance is used as before."""
    monkeypatch.setenv("DAGSTER_HOME", instance.root_directory)
    monkeypatch.setenv("DAGSTER_WORKFLOW_TOOLS_DEFINITIONS_MODULE", defs_module_spec)
    monkeypatch.delenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", raising=False)
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
    assert captured["tools"].instance is instance


def test_live_env_passthrough_reaches_a_spawned_process_with_no_inherited_env(mod, monkeypatch):
    """Models the real failure mode directly: MCP's stdio client spawns the server with
    ONLY the resource's own env dict -- no inheritance from the parent process. Spawn a
    child process that way (env=spec['env'], completely replacing the environment -- the
    same shape subprocess.Popen(..., env=X) and the real mcp stdio client both use) while
    the named var is set in THIS test process, and confirm the child can see it."""
    import subprocess

    monkeypatch.setenv("TEST_PASSTHROUGH_VAR", "reached-child")
    resource = mod._McpServerShapeResource(
        name="x", type="stdio", command=["ignored"], env={"BASE": "1"},
        live_env_passthrough=["TEST_PASSTHROUGH_VAR"],
    )
    spec = resource.model_dump()
    result = subprocess.run(
        [sys.executable, "-c", "import os, sys; sys.stdout.write(os.environ.get('TEST_PASSTHROUGH_VAR', ''))"],
        env=spec["env"], capture_output=True, text=True, timeout=10,
    )
    assert result.stdout == "reached-child"


def test_without_live_env_passthrough_var_does_not_reach_spawned_process(mod, monkeypatch):
    """Negative control -- proves the failure mode is real: without listing the var in
    live_env_passthrough, a process spawned with a stripped environment genuinely cannot
    see it, even though it's set in the parent test process."""
    import subprocess

    monkeypatch.setenv("TEST_PASSTHROUGH_VAR", "reached-child")
    resource = mod._McpServerShapeResource(name="x", type="stdio", command=["ignored"], env={"BASE": "1"})
    spec = resource.model_dump()
    result = subprocess.run(
        [sys.executable, "-c", "import os, sys; sys.stdout.write(os.environ.get('TEST_PASSTHROUGH_VAR', ''))"],
        env=spec["env"], capture_output=True, text=True, timeout=10,
    )
    assert result.stdout == ""
