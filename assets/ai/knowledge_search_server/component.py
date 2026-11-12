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


def _build_s3vectors_client(region_name: str) -> Any:
    """Isolated client construction -- tests monkeypatch this to return a fake client instead of calling the real
    (paid, AWS-account-specific) API. Mirrors vector_store_writer's/vector_store_query's own
    `_build_s3vectors_client` (same reasoning: duplicated per this repo's self-contained-component convention,
    structurally checked against them in tests/test_drift.py)."""
    import boto3

    return boto3.client("s3vectors", region_name=region_name)


class _DemoS3VectorsClient:
    """Deterministic, zero-cost stand-in for `boto3.client("s3vectors")`'s query path. Reads the same local JSON
    file format VectorStoreWriterComponent's demo mode writes, and brute-force cosine-ranks against it. Mirrors
    vector_store_query's own `_DemoS3VectorsClient` (checked against it in tests/test_drift.py)."""

    def __init__(self, path: str):
        self._path = path

    def query_vectors(self, vectorBucketName: str, indexName: str, topK: int, queryVector: Dict[str, Any],
                       filter: Optional[Dict[str, Any]] = None, returnMetadata: bool = True, returnDistance: bool = True, **_kw) -> Dict[str, Any]:
        import json
        import math
        import os

        if not os.path.exists(self._path):
            return {"vectors": []}
        with open(self._path) as f:
            data = json.load(f)
        index = (data.get("indexes") or {}).get(indexName, {"vectors": {}})
        query = queryVector["float32"]
        query_norm = math.sqrt(sum(x * x for x in query)) or 1.0

        scored = []
        for key, entry in index["vectors"].items():
            metadata = entry.get("metadata", {})
            if filter and not all(metadata.get(k) == v for k, v in filter.items()):
                continue
            vec = entry["data"]["float32"]
            dot = sum(a * b for a, b in zip(query, vec))
            vec_norm = math.sqrt(sum(x * x for x in vec)) or 1.0
            cosine_distance = 1.0 - (dot / (query_norm * vec_norm))
            scored.append((cosine_distance, key, metadata))

        scored.sort(key=lambda t: t[0])
        results = []
        for distance, key, metadata in scored[:topK]:
            item = {"key": key}
            if returnMetadata:
                item["metadata"] = metadata
            if returnDistance:
                item["distance"] = distance
            results.append(item)
        return {"vectors": results}


def _query_vector_store(
    embedding: list[float], *, provider: str, connection_string: Optional[str], collection_name: str,
    top_k: int, api_key_env_var: Optional[str],
    vector_bucket_name: Optional[str] = None, aws_region: Optional[str] = None, demo_mode: bool = False,
) -> list[dict[str, Any]]:
    """Queries chromadb/pinecone/qdrant/faiss/s3vectors for the nearest `top_k` vectors. Returns a list of
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

    if provider == "faiss":
        import faiss
        import pickle
        import numpy as np

        index_path = connection_string or f"./{collection_name}.faiss"
        metadata_path = index_path.replace(".faiss", "_metadata.pkl")
        faiss_index = faiss.read_index(index_path)
        query_vector = np.array([embedding]).astype("float32")
        distances, indices = faiss_index.search(query_vector, top_k)
        with open(metadata_path, "rb") as f:
            metadata = pickle.load(f)
        return [
            {"id": int(doc_idx), "metadata": metadata[doc_idx] if doc_idx < len(metadata) else {}, "document": None, "score": float(distances[0][i])}
            for i, doc_idx in enumerate(indices[0])
        ]

    if provider == "s3vectors":
        if not vector_bucket_name:
            raise ValueError("KnowledgeSearch: provider='s3vectors' requires vector_bucket_name.")
        client = _DemoS3VectorsClient(connection_string or f"./{vector_bucket_name}_{collection_name}_s3vectors_demo.json") if demo_mode else _build_s3vectors_client(aws_region)
        resp = client.query_vectors(
            vectorBucketName=vector_bucket_name, indexName=collection_name, topK=top_k,
            queryVector={"float32": [float(x) for x in embedding]}, returnMetadata=True, returnDistance=True,
        )
        return [{"id": v["key"], "metadata": v.get("metadata") or {}, "document": None, "score": v.get("distance")} for v in resp.get("vectors", [])]

    raise ValueError(f"KnowledgeSearch: unsupported provider {provider!r}. Valid: chromadb, pinecone, qdrant, faiss, s3vectors.")
# END knowledge-search core


_EMBED_DEFAULT_API_KEY_ENV_VAR = {"openai": "OPENAI_API_KEY", "cohere": "COHERE_API_KEY"}


def _check_required_env_var(var_name: Optional[str], *, label: str) -> None:
    """Raise a clear, specific error naming the exact missing env var -- rather than letting an
    auth failure surface as a generic "Error executing tool" message with no actionable detail.
    No-ops when var_name is None (sentence_transformers needs no key; qdrant may run
    unauthenticated if api_key_env_var was left unset on purpose)."""
    if var_name and not os.environ.get(var_name):
        raise ValueError(
            f"KnowledgeSearch: {label} env var {var_name!r} is not set in this process's "
            "environment. If this is a stdio MCP server, confirm it's listed in "
            "live_env_passthrough so its value is copied in from the spawning process at run time."
        )


class KnowledgeSearchResource(dg.ConfigurableResource):
    """Embeds a query with the same provider/model a knowledge base was built with, queries
    its vector store for the nearest chunks, and returns them as plain JSON-able dicts --
    `{"chunks": [{"chunk_id", "text", "score", "source", "doc_id"}, ...]}`. `source` is the
    `source_asset_key` metadata `write_vector_store` stamps (which pipeline/asset built this
    chunk); `doc_id` is its `source_id` metadata (which upstream document/row the chunk came
    from -- distinct from `chunk_id` when one document produced multiple chunks)."""

    provider: Literal["chromadb", "pinecone", "qdrant", "faiss", "s3vectors"] = Field(default="chromadb", description="Vector store the knowledge base lives in.")
    connection_string: Optional[str] = Field(default=None, description="chromadb: local path (default './chroma_db'). qdrant: server URL (default 'localhost'). faiss: local .faiss index path (default './{collection_name}.faiss'). s3vectors demo_mode: local JSON demo-file path. Unused for pinecone/s3vectors (non-demo).")
    vector_bucket_name: Optional[str] = Field(default=None, description="S3 Vectors bucket name. Required when provider='s3vectors'. Ignored by every other provider.")
    aws_region: Optional[str] = Field(default=None, description="AWS region for the s3vectors client. Required when provider='s3vectors' and demo_mode=false. Ignored by every other provider.")
    demo_mode: bool = Field(default=True, description="Only affects provider='s3vectors' -- query a local JSON-file-backed fake (written by VectorStoreWriterComponent's own demo_mode) instead of a real AWS account. No effect on chromadb/pinecone/qdrant/faiss.")
    collection_name: str = Field(description="Collection/index name the knowledge base was written to.")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var holding the vector store's API key (pinecone/qdrant).")
    embed_provider: Literal["sentence_transformers", "openai", "cohere", "litellm"] = Field(default="sentence_transformers", description="Must match the embed provider the knowledge base was built with.")
    embed_model: Optional[str] = Field(default=None, description="Must match the embed model the knowledge base was built with.")
    embed_api_key_env_var: Optional[str] = Field(default=None, description="Env var holding the embed provider's API key (openai/cohere/litellm).")
    text_metadata_key: str = Field(default="chunk_text", description="Metadata key holding chunk text for non-chromadb stores (chromadb always has it via its own documents field regardless). Must match write_vector_store's own text_column if it was customized.")

    def search(self, query: str, top_k: int = 5) -> dict:
        _check_required_env_var(
            self.embed_api_key_env_var or _EMBED_DEFAULT_API_KEY_ENV_VAR.get(self.embed_provider),
            label="embed API key",
        )
        _check_required_env_var(
            self.api_key_env_var or ("PINECONE_API_KEY" if self.provider == "pinecone" else None),
            label="vector store API key",
        )
        embedding = _embed_query(query, self.embed_provider, self.embed_model, self.embed_api_key_env_var)
        hits = _query_vector_store(
            embedding, provider=self.provider, connection_string=self.connection_string,
            collection_name=self.collection_name, top_k=top_k, api_key_env_var=self.api_key_env_var,
            vector_bucket_name=self.vector_bucket_name, aws_region=self.aws_region, demo_mode=self.demo_mode,
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
    tests/test_drift.py), including `live_env_passthrough` -- NOT everything here is
    "known at build time" (an earlier version of this docstring claimed it was): the vector
    store / embed API key env var NAMES are build-time config, but their VALUES are not --
    MCP's stdio client does not inherit the rest of the spawning process's environment, so a
    search that needs a real API key fails at run time unless that value is explicitly
    copied in, the same DAGSTER_HOME problem `dagster_workflow_tools` already had."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None
    live_env_passthrough: Optional[List[str]] = None

    def model_dump(self, *args, **kwargs):
        dumped = super().model_dump(*args, **kwargs)
        passthrough = dumped.pop("live_env_passthrough", None)
        if dumped.get("type") == "stdio" and passthrough:
            env = dict(dumped.get("env") or {})
            for var_name in passthrough:
                value = os.environ.get(var_name)
                if value:
                    env[var_name] = value
            dumped["env"] = env
        return dumped


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
    provider: Literal["chromadb", "pinecone", "qdrant", "faiss", "s3vectors"] = Field(default="chromadb", description="Vector store the knowledge base lives in.")
    connection_string: Optional[str] = Field(default=None, description="chromadb: local path (default './chroma_db'). qdrant: server URL (default 'localhost'). faiss: local .faiss index path (default './{collection_name}.faiss'). s3vectors demo_mode: local JSON demo-file path. Unused for pinecone/s3vectors (non-demo).")
    vector_bucket_name: Optional[str] = Field(default=None, description="S3 Vectors bucket name. Required when provider='s3vectors'. Ignored by every other provider.")
    aws_region: Optional[str] = Field(default=None, description="AWS region for the s3vectors client. Required when provider='s3vectors' and demo_mode=false. Ignored by every other provider.")
    demo_mode: bool = Field(default=True, description="Only affects provider='s3vectors' -- query a local JSON-file-backed fake (written by VectorStoreWriterComponent's own demo_mode) instead of a real AWS account. No effect on chromadb/pinecone/qdrant/faiss.")
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
        if self.provider == "s3vectors":
            if not self.vector_bucket_name:
                raise ValueError("KnowledgeSearchServerComponent: provider='s3vectors' requires vector_bucket_name.")
            if not self.demo_mode and not self.aws_region:
                raise ValueError("KnowledgeSearchServerComponent: provider='s3vectors' with demo_mode=false requires aws_region.")
        resource = KnowledgeSearchResource(
            provider=self.provider,
            connection_string=self.connection_string,
            collection_name=self.collection_name,
            api_key_env_var=self.api_key_env_var,
            embed_provider=self.embed_provider,
            embed_model=self.embed_model,
            embed_api_key_env_var=self.embed_api_key_env_var,
            text_metadata_key=self.text_metadata_key,
            vector_bucket_name=self.vector_bucket_name,
            aws_region=self.aws_region,
            demo_mode=self.demo_mode,
        )
        resources: Dict[str, Any] = {self.resource_key: resource}

        if self.mcp_server_resource_key:
            mcp_env = {
                "KNOWLEDGE_SEARCH_PROVIDER": self.provider,
                "KNOWLEDGE_SEARCH_COLLECTION_NAME": self.collection_name,
                "KNOWLEDGE_SEARCH_EMBED_PROVIDER": self.embed_provider,
                "KNOWLEDGE_SEARCH_TEXT_METADATA_KEY": self.text_metadata_key,
                "KNOWLEDGE_SEARCH_DEMO_MODE": "1" if self.demo_mode else "0",
            }
            if self.connection_string:
                mcp_env["KNOWLEDGE_SEARCH_CONNECTION_STRING"] = self.connection_string
            if self.vector_bucket_name:
                mcp_env["KNOWLEDGE_SEARCH_VECTOR_BUCKET_NAME"] = self.vector_bucket_name
            if self.aws_region:
                mcp_env["KNOWLEDGE_SEARCH_AWS_REGION"] = self.aws_region
            if self.api_key_env_var:
                mcp_env["KNOWLEDGE_SEARCH_API_KEY_ENV_VAR"] = self.api_key_env_var
            if self.embed_model:
                mcp_env["KNOWLEDGE_SEARCH_EMBED_MODEL"] = self.embed_model
            if self.embed_api_key_env_var:
                mcp_env["KNOWLEDGE_SEARCH_EMBED_API_KEY_ENV_VAR"] = self.embed_api_key_env_var
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key,
                type="stdio",
                command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env,
                # API key VALUES are never baked into mcp_env above (only non-secret settings
                # are) -- live_env_passthrough copies them in fresh from this process's own
                # environment at run time, same mechanism dagster_workflow_tools uses for
                # DAGSTER_HOME. Without this, a provider that needs a real key fails at
                # search time no matter how the component itself is configured.
                live_env_passthrough=[v for v in (self.api_key_env_var, self.embed_api_key_env_var) if v],
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
                "tool_names": MetadataValue.json(["search_knowledge"]),
                "tool_descriptions": MetadataValue.json({
                    "search_knowledge": "Search the knowledge base for chunks relevant to a query. Returns "
                                        "{chunks: [{chunk_id, text, score, source, doc_id}, ...]} on success, or "
                                        "{error: ...} naming exactly what's misconfigured.",
                }),
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
        {"chunks": [{"chunk_id", "text", "score", "source", "doc_id"}, ...]} on success, or
        {"error": "..."} naming exactly what's misconfigured (e.g. a missing API key env
        var) rather than a generic tool-call failure."""
        return _safe_search(resource, query, top_k)

    return mcp


def _safe_search(resource: "KnowledgeSearchResource", query: str, top_k: int) -> dict:
    """Catches any exception from resource.search() and returns {"error": "..."} instead of
    letting it propagate as FastMCP's own generic "Error executing tool" message -- a
    separate, directly-testable function rather than inline in the tool closure above."""
    try:
        return resource.search(query, top_k=top_k)
    except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
        return {"error": f"{type(e).__name__}: {e}"}


def main(argv: Optional[List[str]] = None) -> None:
    """Env: KNOWLEDGE_SEARCH_COLLECTION_NAME (required), KNOWLEDGE_SEARCH_PROVIDER (default
    chromadb), KNOWLEDGE_SEARCH_CONNECTION_STRING, KNOWLEDGE_SEARCH_API_KEY_ENV_VAR,
    KNOWLEDGE_SEARCH_EMBED_PROVIDER (default sentence_transformers),
    KNOWLEDGE_SEARCH_EMBED_MODEL, KNOWLEDGE_SEARCH_EMBED_API_KEY_ENV_VAR,
    KNOWLEDGE_SEARCH_TEXT_METADATA_KEY (default chunk_text),
    KNOWLEDGE_SEARCH_VECTOR_BUCKET_NAME / KNOWLEDGE_SEARCH_AWS_REGION (provider=s3vectors),
    KNOWLEDGE_SEARCH_DEMO_MODE (default 1; provider=s3vectors only)."""
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
        vector_bucket_name=os.environ.get("KNOWLEDGE_SEARCH_VECTOR_BUCKET_NAME"),
        aws_region=os.environ.get("KNOWLEDGE_SEARCH_AWS_REGION"),
        demo_mode=os.environ.get("KNOWLEDGE_SEARCH_DEMO_MODE", "1") != "0",
    )
    build_mcp_server(resource).run(transport="stdio")


if __name__ == "__main__":
    main()
