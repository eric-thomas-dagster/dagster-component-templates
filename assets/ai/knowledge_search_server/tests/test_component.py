"""build_defs: always registers the KnowledgeSearchResource + an inspection asset tagged
dagster-community/knowledge-search; mcp_server_resource_key additionally registers a
stdio MCP-server-shaped resource built from this component's own settings."""
import sys

import dagster as dg
import pytest


def _component(mod, **kw):
    return mod.KnowledgeSearchServerComponent(collection_name="kb", **kw)


def test_build_defs_registers_resource_and_inspection_asset(mod):
    component = _component(mod, resource_key="ks")
    defs = component.build_defs(None)
    assert set(defs.resources) == {"ks"}
    assert len(defs.assets) == 1
    asset_def = defs.assets[0]
    key = next(iter(asset_def.keys))
    assert key.to_user_string() == "knowledge_search_tools"


def test_inspection_asset_tagged_knowledge_search(mod):
    component = _component(mod, resource_key="ks")
    defs = component.build_defs(None)
    asset_def = defs.assets[0]
    key = next(iter(asset_def.keys))
    tags = asset_def.specs_by_key[key].tags
    assert tags["dagster-community/knowledge-search"] == "ks"


def test_inspection_asset_tag_uses_knowledge_base_label_when_set(mod):
    component = _component(mod, resource_key="ks", knowledge_base="docs_kb")
    defs = component.build_defs(None)
    asset_def = defs.assets[0]
    key = next(iter(asset_def.keys))
    tags = asset_def.specs_by_key[key].tags
    assert tags["dagster-community/knowledge-search"] == "docs_kb"
    assert tags["dagster-community/knowledge-base-name"] == "docs_kb"


def test_inspection_asset_materializes_with_settings_metadata(mod):
    component = _component(mod, resource_key="ks", provider="chromadb", embed_provider="sentence_transformers")
    defs = component.build_defs(None)
    result = dg.materialize(defs.assets, resources={"io_manager": dg.InMemoryIOManager(), "ks": defs.resources["ks"]})
    assert result.success
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert md["provider"].value == "chromadb"
    assert md["collection_name"].value == "kb"
    assert md["embed_provider"].value == "sentence_transformers"


def test_unset_mcp_server_resource_key_registers_only_the_search_resource(mod):
    component = _component(mod, resource_key="ks")
    defs = component.build_defs(None)
    assert set(defs.resources) == {"ks"}


def test_mcp_server_resource_key_registers_stdio_mcp_shaped_resource(mod):
    component = _component(
        mod, resource_key="ks", mcp_server_resource_key="ks_mcp",
        provider="chromadb", embed_provider="sentence_transformers", embed_model="all-MiniLM-L6-v2",
    )
    defs = component.build_defs(None)
    assert set(defs.resources) == {"ks", "ks_mcp"}

    cfg = defs.resources["ks_mcp"].model_dump()
    assert cfg["name"] == "ks_mcp"
    assert cfg["type"] == "stdio"
    assert cfg["command"][:2] == [sys.executable, "-m"]
    assert cfg["command"][2] == mod.__spec__.name
    assert cfg["env"]["KNOWLEDGE_SEARCH_PROVIDER"] == "chromadb"
    assert cfg["env"]["KNOWLEDGE_SEARCH_COLLECTION_NAME"] == "kb"
    assert cfg["env"]["KNOWLEDGE_SEARCH_EMBED_MODEL"] == "all-MiniLM-L6-v2"
    assert "KNOWLEDGE_SEARCH_CONNECTION_STRING" not in cfg["env"]


def test_mcp_server_resource_key_env_includes_optional_settings_when_set(mod):
    component = _component(
        mod, resource_key="ks", mcp_server_resource_key="ks_mcp",
        connection_string="./chroma_db", api_key_env_var="PINECONE_KEY", embed_api_key_env_var="OPENAI_KEY",
    )
    env = component.build_defs(None).resources["ks_mcp"].model_dump()["env"]
    assert env["KNOWLEDGE_SEARCH_CONNECTION_STRING"] == "./chroma_db"
    assert env["KNOWLEDGE_SEARCH_API_KEY_ENV_VAR"] == "PINECONE_KEY"
    assert env["KNOWLEDGE_SEARCH_EMBED_API_KEY_ENV_VAR"] == "OPENAI_KEY"


def test_main_builds_matching_resource_from_env(mod, monkeypatch):
    """Full settings parity: the MCP-entrypoint path (main()) must build the exact same
    resource configuration the mcp_server_resource_key env vars encode."""
    monkeypatch.setenv("KNOWLEDGE_SEARCH_COLLECTION_NAME", "kb")
    monkeypatch.setenv("KNOWLEDGE_SEARCH_PROVIDER", "chromadb")
    monkeypatch.setenv("KNOWLEDGE_SEARCH_EMBED_MODEL", "all-MiniLM-L6-v2")

    captured = {}

    def fake_build_mcp_server(resource, server_name="knowledge-search"):
        captured["resource"] = resource

        class _FakeServer:
            def run(self, transport):
                pass
        return _FakeServer()

    monkeypatch.setattr(mod, "build_mcp_server", fake_build_mcp_server)
    mod.main()
    assert captured["resource"].provider == "chromadb"
    assert captured["resource"].collection_name == "kb"
    assert captured["resource"].embed_model == "all-MiniLM-L6-v2"


def test_main_requires_collection_name(mod, monkeypatch):
    monkeypatch.delenv("KNOWLEDGE_SEARCH_COLLECTION_NAME", raising=False)
    with pytest.raises(SystemExit, match="KNOWLEDGE_SEARCH_COLLECTION_NAME"):
        mod.main()
