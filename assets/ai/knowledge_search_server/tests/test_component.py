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
    assert md["tool_names"].value == ["search_knowledge"]
    assert "search_knowledge" in md["tool_descriptions"].value


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


def test_api_key_env_var_names_wired_into_live_env_passthrough(mod, monkeypatch):
    """The real bug: a search that needs a real API key failed every time, because the
    value was never forwarded to the spawned stdio server -- only the NAME was baked into
    env (KNOWLEDGE_SEARCH_*_ENV_VAR above). live_env_passthrough is what actually copies
    the VALUE in, fresh, at run time."""
    monkeypatch.delenv("PINECONE_KEY", raising=False)
    component = _component(
        mod, resource_key="ks", mcp_server_resource_key="ks_mcp",
        api_key_env_var="PINECONE_KEY", embed_api_key_env_var="OPENAI_KEY",
    )
    resource = component.build_defs(None).resources["ks_mcp"]
    assert "PINECONE_KEY" not in resource.model_dump()["env"]

    monkeypatch.setenv("PINECONE_KEY", "secret-pc")
    monkeypatch.setenv("OPENAI_KEY", "secret-oai")
    env = resource.model_dump()["env"]
    assert env["PINECONE_KEY"] == "secret-pc"
    assert env["OPENAI_KEY"] == "secret-oai"


def test_live_env_passthrough_reaches_a_spawned_process_with_no_inherited_env(mod, monkeypatch):
    """Models the real failure mode directly: MCP's stdio client spawns the server with
    ONLY the resource's own env dict -- no inheritance from the parent process. Spawn a
    child process that way (env=spec['env'], completely replacing the environment) while
    the named var is set in THIS test process, and confirm the child can see it."""
    import subprocess

    monkeypatch.setenv("TEST_PASSTHROUGH_VAR", "reached-child")
    component = _component(
        mod, resource_key="ks", mcp_server_resource_key="ks_mcp", embed_api_key_env_var="TEST_PASSTHROUGH_VAR",
    )
    spec = component.build_defs(None).resources["ks_mcp"].model_dump()
    result = subprocess.run(
        [sys.executable, "-c", "import os, sys; sys.stdout.write(os.environ.get('TEST_PASSTHROUGH_VAR', ''))"],
        env=spec["env"], capture_output=True, text=True, timeout=10,
    )
    assert result.stdout == "reached-child"


def test_search_raises_naming_the_exact_missing_embed_api_key_env_var(mod, monkeypatch):
    monkeypatch.delenv("MISSING_OPENAI_KEY", raising=False)
    resource = mod.KnowledgeSearchResource(
        provider="chromadb", collection_name="kb", embed_provider="openai", embed_api_key_env_var="MISSING_OPENAI_KEY",
    )
    with pytest.raises(ValueError, match="MISSING_OPENAI_KEY"):
        resource.search("q", top_k=1)


def test_search_raises_naming_the_exact_missing_vector_store_api_key_env_var(mod, monkeypatch):
    """pinecone always needs a key -- falls back to the conventional PINECONE_API_KEY name
    when api_key_env_var isn't explicitly set, same convention _query_vector_store itself
    already uses."""
    monkeypatch.delenv("PINECONE_API_KEY", raising=False)
    monkeypatch.setenv("SENTENCE_TRANSFORMERS_NEEDS_NO_KEY", "irrelevant")
    resource = mod.KnowledgeSearchResource(provider="pinecone", collection_name="kb")
    with pytest.raises(ValueError, match="PINECONE_API_KEY"):
        resource.search("q", top_k=1)


def test_safe_search_returns_clear_error_instead_of_crashing(mod, monkeypatch):
    """What the MCP tool itself actually calls -- a missing key must come back as
    {"error": "..."} naming the variable, not propagate as an exception."""
    monkeypatch.delenv("MISSING_OPENAI_KEY", raising=False)
    resource = mod.KnowledgeSearchResource(
        provider="chromadb", collection_name="kb", embed_provider="openai", embed_api_key_env_var="MISSING_OPENAI_KEY",
    )
    result = mod._safe_search(resource, "q", 1)
    assert "error" in result
    assert "MISSING_OPENAI_KEY" in result["error"]


def test_safe_search_passes_through_on_success(mod, monkeypatch):
    monkeypatch.setattr(mod, "_embed_query", lambda *a, **k: [0.1])
    monkeypatch.setattr(mod, "_query_vector_store", lambda *a, **k: [])
    resource = mod.KnowledgeSearchResource(provider="chromadb", collection_name="kb")
    result = mod._safe_search(resource, "q", 1)
    assert result == {"chunks": []}


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
