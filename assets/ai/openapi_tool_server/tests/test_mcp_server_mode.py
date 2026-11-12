"""mode='mcp_server': builds a real fastmcp.FastMCP server from the spec, with the
operation_ids allow-list enforced via route_map_fn. These tests run against the REAL
installed fastmcp package (skipped if unavailable) -- no live server, no network: listing
a FastMCP server's tools only enumerates in-memory objects built from the spec at
construction time.
"""
import asyncio

import dagster as dg
import pytest

from .conftest import SAMPLE_SPEC, SAMPLE_SPEC_RELATIVE_SERVER, load_component_module

fastmcp = pytest.importorskip("fastmcp")


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _patch_fetch_spec(mod, monkeypatch):
    monkeypatch.setattr(mod, "_fetch_spec", lambda *a, **k: SAMPLE_SPEC)


def test_route_map_fn_allowlist_excludes_non_listed_operations(mod):
    """Direct verification of the allow-list logic against a real OpenAPI spec dict --
    confirms route_map_fn returns MCPType.EXCLUDE (not None -- None means 'no override' and
    everything defaults to TOOL) for operations outside operation_ids."""
    resource = mod.OpenApiMcpServerResource(spec=SAMPLE_SPEC, server_name="Test", operation_ids=["getPetById", "listPets"])
    server = resource.build_server()
    tools = asyncio.run(server.list_tools())
    names = {t.name for t in tools}
    assert names == {"getPetById", "listPets"}
    assert "createPet" not in names


def test_no_allowlist_exposes_every_operation(mod):
    resource = mod.OpenApiMcpServerResource(spec=SAMPLE_SPEC, server_name="Test", operation_ids=None)
    server = resource.build_server()
    tools = asyncio.run(server.list_tools())
    names = {t.name for t in tools}
    assert names == {"getPetById", "listPets", "createPet"}


def test_build_defs_emits_resource_and_inspection_asset(mod):
    component = mod.OpenApiToolServerComponent(
        mode="mcp_server", spec_url="x", operation_ids=["getPetById"],
        resource_key="my_mcp_server", inspection_asset_name="my_tools",
    )
    defs = component.build_defs(None)
    assert len(defs.assets) == 1
    asset_def = defs.assets[0]
    key = next(iter(asset_def.keys))
    assert key.to_user_string() == "my_tools"
    tags = asset_def.specs_by_key[key].tags
    # Tag values are sanitized (Dagster's strict tag-value validation rejects spaces) --
    # 'OpenAPI Tool Server' (the server_name/label used elsewhere, e.g. metadata) becomes
    # 'OpenAPI_Tool_Server' as a tag value.
    assert tags["dagster-community/mcp-server"] == "OpenAPI_Tool_Server"
    assert tags["dagster-community/tool-source"] == "openapi"
    assert "my_mcp_server" in defs.resources


def test_inspection_asset_materializes_with_tool_list_metadata(mod):
    component = mod.OpenApiToolServerComponent(
        mode="mcp_server", spec_url="x", operation_ids=["getPetById", "listPets"],
    )
    defs = component.build_defs(None)
    result = dg.materialize(
        defs.assets,
        resources={"io_manager": dg.InMemoryIOManager(), component.resource_key: defs.resources[component.resource_key]},
    )
    assert result.success
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert md["tool_count"].value == 2
    assert set(md["tool_names"].data) == {"getPetById", "listPets"}
    assert md["operation_ids_allowlist"].data == ["getPetById", "listPets"]


@pytest.mark.parametrize(
    "spec_url,server_url,expected",
    [
        ("https://petstore3.swagger.io/api/v3/openapi.json", "/api/v3", "https://petstore3.swagger.io/api/v3"),
        ("https://petstore3.swagger.io/api/v3/openapi.json", "https://other.example.com/v1", "https://other.example.com/v1"),
        (None, "/api/v3", ""),
    ],
)
def test_resolve_server_base_url(mod, spec_url, server_url, expected):
    assert mod._resolve_server_base_url(spec_url, server_url) == expected


def test_relative_server_url_resolved_in_built_client(mod, monkeypatch):
    """Regression test for the same Petstore bug fixed in mode='assets': mode='mcp_server'
    built its httpx client's base_url from the raw (possibly relative, schemeless)
    servers[].url too -- now resolved against spec_url first."""
    captured = {}
    try:
        import httpx2 as _httpx  # component tries this first -- patch whichever it'll actually use
    except ImportError:
        import httpx as _httpx

    real_async_client = _httpx.AsyncClient

    class _CapturingAsyncClient(real_async_client):
        def __init__(self, *args, **kwargs):
            captured["base_url"] = str(kwargs.get("base_url"))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(_httpx, "AsyncClient", _CapturingAsyncClient)

    resource = mod.OpenApiMcpServerResource(
        spec=SAMPLE_SPEC_RELATIVE_SERVER,
        spec_url="https://petstore3.swagger.io/api/v3/openapi.json",
        server_name="Test",
    )
    resource.build_server()
    assert captured["base_url"] == "https://petstore3.swagger.io/api/v3"


def test_fastmcp_missing_raises_actionable_error(mod, monkeypatch):
    import builtins

    real_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "fastmcp":
            raise ImportError("simulated: fastmcp not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    resource = mod.OpenApiMcpServerResource(spec=SAMPLE_SPEC, server_name="Test", operation_ids=None)
    with pytest.raises(ImportError, match="fastmcp"):
        resource.build_server()
