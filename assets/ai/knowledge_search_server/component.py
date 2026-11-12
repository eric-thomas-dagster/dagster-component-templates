"""KnowledgeSearchServerComponent -- lets an agent search a knowledge base built by
ContextEngineeringPipelineComponent (an `embed` + `write_vector_store` step pair), via a
`search_knowledge(query, top_k=5)` tool returning each chunk's text, score, source, doc
id, and chunk id as JSON.

Same dual-mode shape as `dagster_workflow_tools`:
  1. An importable `KnowledgeSearchResource` (embed-query + vector-store-query logic
     adapted from `resources/context_receipt_store`'s own `KnowledgeSearch` -- its
     `_embed_query`/`_query_vector_store` helpers are copied verbatim here, drift-tested
     against that file; its `priors`/exploration/receipt-building ceremony is NOT carried
     over, since that's specific to the receipt-learning-loop feature this component
     doesn't integrate with) registered under `resource_key`.
  2. A self-contained stdio MCP server: `python -m <dotted.path.to.this.module>`.
`mcp_server_resource_key`, if set, ALSO registers a stdio MCP-server-shaped resource
(same convention as `dagster_workflow_tools`' own `mcp_server_resource_key`) so any
mcp_servers-aware component can reference this server via `server_resource_key`.

Needs its own explicit embed/vector-store settings (provider, connection_string,
collection_name, embed_provider, embed_model) -- there is no way to read another
component's step config from just an asset name across component boundaries at build
time (the same constraint already documented for `dagster_workflow_tools`/
`skill_resource` this session; `context_engineering_pipeline`'s own `reuse_from` knob
takes the same explicit shape rather than resolving another asset's config by name).
`knowledge_base` is an optional UI-linking label only -- the literal value of the
`dagster-community/knowledge-base-name` tag `context_engineering_pipeline` already
stamps on its own final step -- not something this component reads or validates.

Known limitation (inherited from context_engineering_pipeline, not introduced here): for
non-chromadb vector stores (pinecone/qdrant), a chunk's raw text is only retrievable if
the upstream `write_vector_store` step's `metadata_columns` explicitly included the text
column -- that step excludes it from metadata by default. chromadb always has the text
via its own `documents` field regardless of `metadata_columns`.

Always emits one small inspection asset (`inspection_asset_name`), tagged
`dagster-community/knowledge-search`, reporting the configured provider/collection/embed
settings as metadata -- resources have no tag mechanism of their own (confirmed via
direct `ResourceDefinition`/`ConfigurableResource` introspection), so this is the only
catalog-visible surface for "a knowledge-search server is configured here", mirroring
`openapi_tool_server`'s own `mode='mcp_server'` inspection-asset precedent.
"""
import logging
import os
import sys
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from dagster import AssetExecutionContext, AssetKey, Definitions, MetadataValue, asset
from pydantic import Field

# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


# BEGIN knowledge-search core (copied verbatim from resources/context_receipt_store's
# own _embed_query/_query_vector_store -- see tests/test_drift.py)
def _embed_query(query: str, provider: str, model: Optional[str], api_key_env_var: Optional[str]) -> list[float]:
    """Embeds ONE query string with the same provider/model an index was built with --
    mirrors `context_engineering_pipeline`'s own `_do_embed_raw` provider dispatch (single
    query, not batched, since a gateway answers one question at a time)."""
    import os

    if provider == "sentence_transformers":
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError("embed_provider='sentence_transformers' requires: pip install sentence-transformers")
        model_name = model or "all-MiniLM-L6-v2"
        return SentenceTransformer(model_name).encode([query], normalize_embeddings=True, show_progress_bar=False)[0].tolist()

    if provider == "openai":
        try:
            import openai
        except ImportError:
            raise ImportError("embed_provider='openai' requires: pip install openai")
        client = openai.OpenAI(api_key=os.environ.get(api_key_env_var or "OPENAI_API_KEY"))
        resp = client.embeddings.create(model=model or "text-embedding-3-small", input=[query])
        return resp.data[0].embedding

    if provider == "cohere":
        try:
            import cohere
        except ImportError:
            raise ImportError("embed_provider='cohere' requires: pip install cohere")
        client = cohere.Client(api_key=os.environ.get(api_key_env_var or "COHERE_API_KEY"))
        resp = client.embed(texts=[query], model=model or "embed-english-v3.0", input_type="search_query")
        return resp.embeddings[0]

    if provider == "litellm":
        try:
            import litellm
        except ImportError:
            raise ImportError("embed_provider='litellm' requires: pip install litellm")
        api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
        resp = litellm.embedding(model=model or "text-embedding-3-small", input=[query], api_key=api_key)
        return resp.data[0]["embedding"]

    raise ValueError(f"KnowledgeSearch: unsupported embed_provider {provider!r}. Valid: sentence_transformers, openai, cohere, litellm.")


def _query_vector_store(
    embedding: list[float], *, provider: str, connection_string: Optional[str], collection_name: str,
    top_k: int, api_key_env_var: Optional[str],
) -> list[dict[str, Any]]:
    """Queries chromadb/pinecone/qdrant for the nearest `top_k` vectors. Returns a list of
    `{"id", "metadata", "distance_or_score"}` dicts -- the raw provider result, not yet
    quarantine-filtered or reranked (`KnowledgeSearch.search` does that)."""
    import os

    if provider == "chromadb":
        import chromadb
        client = chromadb.PersistentClient(path=connection_string or "./chroma_db")
        collection = client.get_collection(name=collection_name)
        res = collection.query(query_embeddings=[embedding], n_results=top_k)
        out = []
        for i in range(len(res["ids"][0])):
            out.append({
                "id": res["ids"][0][i],
                "metadata": (res.get("metadatas") or [[]])[0][i] if res.get("metadatas") else {},
                "document": (res.get("documents") or [[]])[0][i] if res.get("documents") else None,
                "score": None,
            })
        return out

    if provider == "pinecone":
        from pinecone import Pinecone
        pc = Pinecone(api_key=os.environ.get(api_key_env_var or "PINECONE_API_KEY"))
        index = pc.Index(collection_name)
        resp = index.query(vector=embedding, top_k=top_k, include_metadata=True)
        return [{"id": m["id"], "metadata": m.get("metadata") or {}, "document": None, "score": m.get("score")} for m in resp["matches"]]

    if provider == "qdrant":
        from qdrant_client import QdrantClient
        client = QdrantClient(url=connection_string or "localhost", api_key=os.environ.get(api_key_env_var) if api_key_env_var else None)
        hits = client.search(collection_name=collection_name, query_vector=embedding, limit=top_k)
        return [{"id": h.id, "metadata": h.payload or {}, "document": None, "score": h.score} for h in hits]

    raise ValueError(f"KnowledgeSearch: unsupported provider {provider!r}. Valid: chromadb, pinecone, qdrant.")
# END knowledge-search core


class KnowledgeSearchResource(dg.ConfigurableResource):
    """Embeds a query with the same provider/model a knowledge base was built with, queries
    its vector store for the nearest chunks, and returns them as plain JSON-able dicts --
    `{"chunks": [{"chunk_id", "text", "score", "source", "doc_id"}, ...]}`. `source` is the
    `source_asset_key` metadata `write_vector_store` stamps (which pipeline/asset built this
    chunk); `doc_id` is its `source_id` metadata (which upstream document/row the chunk came
    from -- distinct from `chunk_id` when one document produced multiple chunks)."""

    provider: Literal["chromadb", "pinecone", "qdrant"] = Field(default="chromadb", description="Vector store the knowledge base lives in.")
    connection_string: Optional[str] = Field(default=None, description="chromadb: local path (default './chroma_db'). qdrant: server URL (default 'localhost'). Unused for pinecone.")
    collection_name: str = Field(description="Collection/index name the knowledge base was written to.")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var holding the vector store's API key (pinecone/qdrant).")
    embed_provider: Literal["sentence_transformers", "openai", "cohere", "litellm"] = Field(default="sentence_transformers", description="Must match the embed provider the knowledge base was built with.")
    embed_model: Optional[str] = Field(default=None, description="Must match the embed model the knowledge base was built with.")
    embed_api_key_env_var: Optional[str] = Field(default=None, description="Env var holding the embed provider's API key (openai/cohere/litellm).")
    text_metadata_key: str = Field(default="chunk_text", description="Metadata key holding chunk text for non-chromadb stores (chromadb always has it via its own documents field regardless). Must match write_vector_store's own text_column if it was customized.")

    def search(self, query: str, top_k: int = 5) -> dict:
        embedding = _embed_query(query, self.embed_provider, self.embed_model, self.embed_api_key_env_var)
        hits = _query_vector_store(
            embedding, provider=self.provider, connection_string=self.connection_string,
            collection_name=self.collection_name, top_k=top_k, api_key_env_var=self.api_key_env_var,
        )
        chunks = []
        for h in hits:
            meta = h.get("metadata") or {}
            text = h.get("document") if h.get("document") is not None else meta.get(self.text_metadata_key)
            chunks.append({
                "chunk_id": meta.get("chunk_id", h["id"]),
                "text": text,
                "score": h.get("score"),
                "source": meta.get("source_asset_key"),
                "doc_id": meta.get("source_id"),
            })
        return {"chunks": chunks}


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource (name/type/command/url/env/headers/
    headers_env) -- inlined here rather than imported from resources/mcp_server_resource
    (confirmed not importable across components). Same field shape as
    dagster_workflow_tools' own copy of this class (structurally checked in
    tests/test_drift.py) -- deliberately NOT byte-identical, since that copy overrides
    model_dump() to inject DAGSTER_HOME at run time, a quirk this component doesn't need
    (all of its settings are fully known at build time)."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None


def _this_module_name() -> str:
    """The real importable dotted path for the `python -m <this>` stdio server invocation.
    `__name__` alone is unreliable: it's literally `"__main__"` both for a bare
    `python component.py` invocation AND for `python -m dotted.path`. `__spec__.name` is the
    fix (same mechanism/rationale as dagster_workflow_tools' own `_this_module_name`,
    duplicated here -- not imported, per this repo's self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "knowledge_search_server's stdio MCP server must be run as "
        "`python -m <dotted.path.to.this.module>` (e.g. `python -m "
        "dagster_community_components.assets.ai.knowledge_search_server.component`), not "
        "`python component.py` directly -- the spawned server's own command needs a real "
        "importable module name (available via __spec__.name). See README.md."
    )


class KnowledgeSearchServerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers a `KnowledgeSearchResource` (default key `knowledge_search`) and, always,
    a small inspection asset reporting its settings. Run THIS FILE as the stdio MCP server
    for agentic_pipeline (`python -m dagster_community_components.assets.ai.
    knowledge_search_server.component`) -- see README.md."""

    resource_key: str = Field(default="knowledge_search", description="Dagster resource key the in-process KnowledgeSearchResource is registered under.")
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/env "
            "shape as MCPServerResource) under this key, built from this component's own settings -- "
            "python -m this module, with the KNOWLEDGE_SEARCH_* env vars set from provider/"
            "connection_string/collection_name/api_key_env_var/embed_provider/embed_model/"
            "embed_api_key_env_var/text_metadata_key. Any mcp_servers-aware component (mcp_tool_call, "
            "mcp_tool_picker, the agent components, agentic_pipeline) can then reference it via "
            "server_resource_key instead of hand-writing an inline command+env entry. Leave unset to "
            "skip this."
        ),
    )
    knowledge_base: Optional[str] = Field(
        default=None,
        description=(
            "Optional UI-linking label only -- the asset name of the context_engineering_pipeline "
            "step that built this index (its dagster-community/knowledge-base-name tag value). This "
            "component does not read that asset's config from this -- it still needs its own explicit "
            "provider/connection_string/collection_name/embed_provider/embed_model below, since there's "
            "no way to read another component's step config across component boundaries at build time."
        ),
    )
    provider: Literal["chromadb", "pinecone", "qdrant"] = Field(default="chromadb", description="Vector store the knowledge base lives in.")
    connection_string: Optional[str] = Field(default=None, description="chromadb: local path (default './chroma_db'). qdrant: server URL (default 'localhost'). Unused for pinecone.")
    collection_name: str = Field(description="Collection/index name the knowledge base was written to (write_vector_store's own collection_name).")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var holding the vector store's API key (pinecone/qdrant).")
    embed_provider: Literal["sentence_transformers", "openai", "cohere", "litellm"] = Field(default="sentence_transformers", description="Must match the embed provider the knowledge base was built with (the embed step's own provider).")
    embed_model: Optional[str] = Field(default=None, description="Must match the embed model the knowledge base was built with (the embed step's own model).")
    embed_api_key_env_var: Optional[str] = Field(default=None, description="Env var holding the embed provider's API key (openai/cohere/litellm).")
    text_metadata_key: str = Field(default="chunk_text", description="Metadata key holding chunk text for non-chromadb stores. Must match write_vector_store's own text_column if it was customized -- and that column must have been included in metadata_columns, since write_vector_store excludes the text column from metadata by default.")
    inspection_asset_name: str = Field(default="knowledge_search_tools", description="Asset key for the inspection asset reporting this server's configured settings.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, Any]] = Field(default=None, description="Extra asset tags, applied to the inspection asset.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners",), csv=(), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Knowledge Search Server', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> Definitions:
        resource = KnowledgeSearchResource(
            provider=self.provider,
            connection_string=self.connection_string,
            collection_name=self.collection_name,
            api_key_env_var=self.api_key_env_var,
            embed_provider=self.embed_provider,
            embed_model=self.embed_model,
            embed_api_key_env_var=self.embed_api_key_env_var,
            text_metadata_key=self.text_metadata_key,
        )
        resources: Dict[str, Any] = {self.resource_key: resource}

        if self.mcp_server_resource_key:
            mcp_env = {
                "KNOWLEDGE_SEARCH_PROVIDER": self.provider,
                "KNOWLEDGE_SEARCH_COLLECTION_NAME": self.collection_name,
                "KNOWLEDGE_SEARCH_EMBED_PROVIDER": self.embed_provider,
                "KNOWLEDGE_SEARCH_TEXT_METADATA_KEY": self.text_metadata_key,
            }
            if self.connection_string:
                mcp_env["KNOWLEDGE_SEARCH_CONNECTION_STRING"] = self.connection_string
            if self.api_key_env_var:
                mcp_env["KNOWLEDGE_SEARCH_API_KEY_ENV_VAR"] = self.api_key_env_var
            if self.embed_model:
                mcp_env["KNOWLEDGE_SEARCH_EMBED_MODEL"] = self.embed_model
            if self.embed_api_key_env_var:
                mcp_env["KNOWLEDGE_SEARCH_EMBED_API_KEY_ENV_VAR"] = self.embed_api_key_env_var
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key,
                type="stdio",
                command=["python", "-m", _this_module_name()],
                env=mcp_env,
            )

        tags = dict(self.asset_tags or {})
        tags.setdefault("dagster-community/knowledge-search", self.knowledge_base or self.resource_key)
        if self.knowledge_base:
            tags.setdefault("dagster-community/knowledge-base-name", self.knowledge_base)

        provider, collection_name = self.provider, self.collection_name
        embed_provider, embed_model = self.embed_provider, self.embed_model
        knowledge_base = self.knowledge_base

        @asset(
            key=AssetKey.from_user_string(self.inspection_asset_name),
            group_name=self.group_name,
            description=self.description or "Reports the knowledge-search server's configured provider/collection/embed settings.",
            owners=self.owners or [],
            tags=tags,
        )
        def _inspection_asset(context: AssetExecutionContext):
            md = {
                "provider": MetadataValue.text(provider),
                "collection_name": MetadataValue.text(collection_name),
                "embed_provider": MetadataValue.text(embed_provider),
                "embed_model": MetadataValue.text(embed_model or "(default)"),
            }
            if knowledge_base:
                md["knowledge_base"] = MetadataValue.text(knowledge_base)
            context.add_output_metadata(md)

        return Definitions(assets=[_inspection_asset], resources=resources)


# ── stdio MCP server entrypoint:  python -m <dotted.path.to.this.module>  ───────────────
def build_mcp_server(resource: KnowledgeSearchResource, server_name: str = "knowledge-search"):
    """MCP server (official `mcp` package) exposing search_knowledge. Works on mcp 1.x and
    2.x (same compat shim as dagster_instance_tools'/dagster_workflow_tools' own
    build_mcp_server)."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server(server_name)

    @mcp.tool()
    def search_knowledge(query: str, top_k: int = 5) -> dict:
        """Search the knowledge base for chunks relevant to `query`. Returns
        {"chunks": [{"chunk_id", "text", "score", "source", "doc_id"}, ...]}."""
        return resource.search(query, top_k=top_k)

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: KNOWLEDGE_SEARCH_COLLECTION_NAME (required), KNOWLEDGE_SEARCH_PROVIDER (default
    chromadb), KNOWLEDGE_SEARCH_CONNECTION_STRING, KNOWLEDGE_SEARCH_API_KEY_ENV_VAR,
    KNOWLEDGE_SEARCH_EMBED_PROVIDER (default sentence_transformers),
    KNOWLEDGE_SEARCH_EMBED_MODEL, KNOWLEDGE_SEARCH_EMBED_API_KEY_ENV_VAR,
    KNOWLEDGE_SEARCH_TEXT_METADATA_KEY (default chunk_text)."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    collection_name = os.environ.get("KNOWLEDGE_SEARCH_COLLECTION_NAME")
    if not collection_name:
        raise SystemExit("KNOWLEDGE_SEARCH_COLLECTION_NAME must be set")
    resource = KnowledgeSearchResource(
        provider=os.environ.get("KNOWLEDGE_SEARCH_PROVIDER", "chromadb"),
        connection_string=os.environ.get("KNOWLEDGE_SEARCH_CONNECTION_STRING"),
        collection_name=collection_name,
        api_key_env_var=os.environ.get("KNOWLEDGE_SEARCH_API_KEY_ENV_VAR"),
        embed_provider=os.environ.get("KNOWLEDGE_SEARCH_EMBED_PROVIDER", "sentence_transformers"),
        embed_model=os.environ.get("KNOWLEDGE_SEARCH_EMBED_MODEL"),
        embed_api_key_env_var=os.environ.get("KNOWLEDGE_SEARCH_EMBED_API_KEY_ENV_VAR"),
        text_metadata_key=os.environ.get("KNOWLEDGE_SEARCH_TEXT_METADATA_KEY", "chunk_text"),
    )
    build_mcp_server(resource).run(transport="stdio")


if __name__ == "__main__":
    main()
