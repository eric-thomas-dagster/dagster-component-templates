"""server_resource_key on mcp_call's `server:` and tool_use_loop's `mcp_servers:` entries: lets a
step share a registered MCPServerResource instead of repeating the same inline name/type/command/
url/env/headers/headers_env spec -- same pattern as mcp_tool_call's own server_resource_key.
Exactly one of inline (name + transport fields) or server_resource_key is required per entry,
enforced by `_resolve_mcp_server_cfg`. `_collect_mcp_server_resource_keys` is the static (build_defs
time) union used for `required_resource_keys`, recursing into `sub_pipeline` steps.

Real small stdio MCP server (fixtures/mini_mcp_server.py), same one test_mcp_client_contract.py
uses -- nothing about the MCP call itself is mocked.
"""
import pathlib
import shutil
import sys

import dagster as dg
import pytest

from .conftest import new_state

FIXTURE = pathlib.Path(__file__).resolve().parent / "fixtures" / "mini_mcp_server.py"


@pytest.fixture
def mod(fake_litellm):
    from .conftest import load_component_module
    return load_component_module()


@pytest.fixture
def server_py(tmp_path_factory):
    d = tmp_path_factory.mktemp("mini_mcp_src")
    dst = d / "mini_mcp_server.py"
    shutil.copy(FIXTURE, dst)
    return str(dst)


def _ctx(resources):
    import logging
    from types import SimpleNamespace
    return SimpleNamespace(log=logging.getLogger("agentic_test"), resources=SimpleNamespace(**resources))


def test_resolve_mcp_server_cfg_requires_exactly_one_of_inline_and_resource_key(mod):
    with pytest.raises(ValueError, match="exactly one"):
        mod._resolve_mcp_server_cfg({}, _ctx({}), where="mcp_call step 'x'")
    with pytest.raises(ValueError, match="exactly one"):
        mod._resolve_mcp_server_cfg(
            {"name": "inline", "type": "stdio", "command": ["x"], "server_resource_key": "shared"},
            _ctx({}), where="mcp_call step 'x'",
        )


def test_resolve_mcp_server_cfg_resolves_the_resource(mod):
    from resources.mcp_server_resource.component import MCPServerResource

    resource = MCPServerResource(name="dgp", type="http", url="http://localhost:1/mcp")
    resolved = mod._resolve_mcp_server_cfg({"server_resource_key": "shared_mcp"}, _ctx({"shared_mcp": resource}), where="x")
    assert resolved["name"] == "dgp" and resolved["url"] == "http://localhost:1/mcp"


def test_resolve_mcp_server_cfg_inline_still_works(mod):
    cfg = {"name": "inline-srv", "type": "http", "url": "http://x"}
    resolved = mod._resolve_mcp_server_cfg(cfg, _ctx({}), where="x")
    assert resolved == cfg


def test_collect_mcp_server_resource_keys_unions_mcp_call_and_tool_use_loop_and_recurses_into_sub_pipeline(mod):
    steps = [
        {"id": "a", "op": "mcp_call", "server": {"server_resource_key": "srv_a"}},
        {"id": "b", "op": "mcp_call", "server": {"name": "inline", "type": "stdio", "command": ["x"]}},
        {"id": "c", "op": "tool_use_loop", "mcp_servers": [
            {"server_resource_key": "srv_b"}, {"name": "inline2", "type": "stdio", "command": ["y"]},
        ]},
        {"id": "d", "op": "sub_pipeline", "steps": [
            {"id": "e", "op": "mcp_call", "server": {"server_resource_key": "srv_c"}},
        ]},
    ]
    assert mod._collect_mcp_server_resource_keys(steps) == {"srv_a", "srv_b", "srv_c"}


def test_mcp_call_step_server_resource_key_resolves_at_materialization_against_a_real_server(mod, server_py):
    from resources.mcp_server_resource.component import MCPServerResource

    component = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "t"},
        steps=[{"id": "g", "op": "mcp_call", "server": {"server_resource_key": "shared_mcp"},
                "mcp_tool_name": "echo", "tool_args": {"text": "hi", "times": 2}}],
        outputs={"assets": ["g"]},
    )
    defs = component.build_defs(None)
    resource = MCPServerResource(name="mini", type="stdio", command=[sys.executable, server_py, "stdio"])
    result = dg.materialize(defs.assets, resources={"shared_mcp": resource})
    assert result.success


def test_mcp_call_step_inline_server_still_works(mod, server_py):
    component = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "t"},
        steps=[{"id": "g", "op": "mcp_call",
                "server": {"name": "mini", "type": "stdio", "command": [sys.executable, server_py, "stdio"]},
                "mcp_tool_name": "echo", "tool_args": {"text": "hi"}}],
        outputs={"assets": ["g"]},
    )
    defs = component.build_defs(None)
    result = dg.materialize(defs.assets)
    assert result.success


def test_required_resource_keys_only_includes_shared_entries(mod):
    component = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "t"},
        steps=[
            {"id": "g", "op": "mcp_call", "server": {"name": "inline", "type": "stdio", "command": ["x"]},
             "mcp_tool_name": "echo"},
            {"id": "h", "op": "mcp_call", "server": {"server_resource_key": "shared_mcp"}, "mcp_tool_name": "echo"},
        ],
        outputs={"assets": ["g", "h"]},
    )
    defs = component.build_defs(None)
    asset_def = defs.assets[0]
    assert "shared_mcp" in asset_def.required_resource_keys
