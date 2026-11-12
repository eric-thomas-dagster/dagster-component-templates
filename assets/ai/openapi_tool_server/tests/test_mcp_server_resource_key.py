"""mcp_server_resource_key (mode='mcp_server' only): registers a second, stdio
MCP-server-shaped resource built from this component's own settings, so any
mcp_servers-aware component can reference this OpenAPI spec via server_resource_key
instead of hand-writing an inline command+env entry. Constructing OpenApiMcpServerResource
itself never imports fastmcp (only build_server() does, lazily), so these tests don't
need fastmcp installed."""
import sys

import pytest

from .conftest import SAMPLE_SPEC, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _patch_fetch_spec(mod, monkeypatch):
    monkeypatch.setattr(mod, "_fetch_spec", lambda *a, **k: SAMPLE_SPEC)


def _component(mod, **kw):
    return mod.OpenApiToolServerComponent(mode="mcp_server", spec_url="http://fake-spec", **kw)


def test_requires_mcp_server_mode(mod):
    with pytest.raises(ValueError, match="mode='mcp_server'"):
        mod.OpenApiToolServerComponent(mode="assets", spec_url="http://fake-spec", mcp_server_resource_key="x")


def test_unset_mcp_server_resource_key_registers_only_the_existing_resource(mod):
    component = _component(mod, resource_key="petstore_mcp")
    defs = component.build_defs(None)
    assert set(defs.resources) == {"petstore_mcp"}


def test_mcp_server_resource_key_registers_stdio_mcp_shaped_resource(mod):
    component = _component(
        mod, resource_key="petstore_mcp", mcp_server_resource_key="petstore_mcp_key",
        operation_ids=["getPetById", "listPets"], server_name="Petstore",
    )
    defs = component.build_defs(None)
    assert set(defs.resources) == {"petstore_mcp", "petstore_mcp_key"}

    cfg = defs.resources["petstore_mcp_key"].model_dump()
    assert cfg["name"] == "petstore_mcp_key"
    assert cfg["type"] == "stdio"
    assert cfg["command"][:2] == [sys.executable, "-m"]
    assert cfg["command"][2] == mod.__spec__.name
    assert cfg["env"]["OPENAPI_TOOL_SERVER_SPEC_URL"] == "http://fake-spec"
    assert cfg["env"]["OPENAPI_TOOL_SERVER_SERVER_NAME"] == "Petstore"
    assert cfg["env"]["OPENAPI_TOOL_SERVER_OPERATION_IDS"] == "getPetById,listPets"
    assert "OPENAPI_TOOL_SERVER_API_KEY_ENV_VAR" not in cfg["env"]


def test_mcp_server_resource_key_env_includes_api_key_env_var_name_when_set(mod):
    component = _component(mod, mcp_server_resource_key="x", api_key_env_var="PETSTORE_API_KEY")
    cfg = component.build_defs(None).resources["x"].model_dump()
    assert cfg["env"]["OPENAPI_TOOL_SERVER_API_KEY_ENV_VAR"] == "PETSTORE_API_KEY"


def test_live_env_passthrough_reads_api_base_url_and_api_key_fresh_at_model_dump_time(mod, monkeypatch):
    """The actual secret VALUES (not just which env var names them) are read fresh every
    time model_dump() is called -- not baked in at build_defs (component load) time."""
    component = _component(
        mod, mcp_server_resource_key="x",
        api_base_url_env_var="PETSTORE_BASE_URL", api_key_env_var="PETSTORE_API_KEY",
    )
    monkeypatch.delenv("PETSTORE_BASE_URL", raising=False)
    monkeypatch.delenv("PETSTORE_API_KEY", raising=False)
    resource = component.build_defs(None).resources["x"]

    assert "PETSTORE_BASE_URL" not in resource.model_dump()["env"]
    assert "PETSTORE_API_KEY" not in resource.model_dump()["env"]

    monkeypatch.setenv("PETSTORE_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("PETSTORE_API_KEY", "secret-token")
    cfg = resource.model_dump()
    assert cfg["env"]["PETSTORE_BASE_URL"] == "https://api.example.com"
    assert cfg["env"]["PETSTORE_API_KEY"] == "secret-token"


def test_live_env_passthrough_reaches_a_spawned_process_with_no_inherited_env(mod, monkeypatch):
    """Models the real failure mode directly: MCP's stdio client spawns the server with
    ONLY the resource's own env dict -- no inheritance from the parent process. Spawn a
    child process that way (env=spec['env'], completely replacing the environment) while
    the named var is set in THIS test process, and confirm the child can see it."""
    import subprocess

    monkeypatch.setenv("TEST_PASSTHROUGH_VAR", "reached-child")
    component = _component(mod, mcp_server_resource_key="x", api_key_env_var="TEST_PASSTHROUGH_VAR")
    spec = component.build_defs(None).resources["x"].model_dump()
    result = subprocess.run(
        [sys.executable, "-c", "import os, sys; sys.stdout.write(os.environ.get('TEST_PASSTHROUGH_VAR', ''))"],
        env=spec["env"], capture_output=True, text=True, timeout=10,
    )
    assert result.stdout == "reached-child"


def test_live_env_passthrough_field_not_leaked_into_model_dump_output(mod):
    component = _component(mod, mcp_server_resource_key="x")
    cfg = component.build_defs(None).resources["x"].model_dump()
    assert "live_env_passthrough" not in cfg


def test_main_requires_spec_url_or_spec_path(mod, monkeypatch):
    monkeypatch.delenv("OPENAPI_TOOL_SERVER_SPEC_URL", raising=False)
    monkeypatch.delenv("OPENAPI_TOOL_SERVER_SPEC_PATH", raising=False)
    with pytest.raises(SystemExit, match="OPENAPI_TOOL_SERVER_SPEC_URL"):
        mod.main()


def test_main_builds_matching_resource_from_env(mod, monkeypatch):
    monkeypatch.setenv("OPENAPI_TOOL_SERVER_SPEC_URL", "http://fake-spec")
    monkeypatch.setenv("OPENAPI_TOOL_SERVER_OPERATION_IDS", "getPetById,listPets")
    monkeypatch.setenv("OPENAPI_TOOL_SERVER_SERVER_NAME", "Petstore")

    captured = {}

    class _FakeServer:
        def run(self, transport):
            captured["ran"] = True

    def fake_build_server(self):
        captured["resource"] = self
        return _FakeServer()

    monkeypatch.setattr(mod.OpenApiMcpServerResource, "build_server", fake_build_server)
    mod.main()

    assert captured["ran"] is True
    assert captured["resource"].server_name == "Petstore"
    assert captured["resource"].operation_ids == ["getPetById", "listPets"]
    assert captured["resource"].spec == SAMPLE_SPEC
