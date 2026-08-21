"""RAG Pipeline Asset Component.

Complete Retrieval-Augmented Generation pipeline combining query, retrieval, and generation.
"""

import os
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from dagster import (
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class RAGPipelineComponent(Component, Model, Resolvable):
    """One YAML, whole RAG pipeline — retrieve, (optional) rerank, generate.

    Pipeline shape: `personas:` + `retrievers:` + `rerankers:` + `steps:`.
    Every row in the upstream DataFrame's `query_column` gets its own
    trip through the ops chain. Family sibling of AgenticPipeline /
    MLPipeline / PolarsPipeline.

    Ops menu:
      - retrieve         — vector search against a named retriever
      - hybrid_search    — BM25 + vector combined via RRF
      - rerank           — Voyage rerank-2.5 / Cohere rerank-3.5 over docs
      - expand_query     — HyDE — LLM writes a hypothetical answer to embed
      - generate         — LLM answer using retrieved docs as context;
                           full reasoning + prompt-caching support

    Example:
        ```yaml
        type: dagster_community_components.RAGPipelineComponent
        attributes:
          asset_name: docs_qa
          upstream_asset_key: user_questions

          retrievers:
            docs_vs:
              kind: chromadb
              collection_name: product_docs
              path: ./chroma_db
              embedding:
                provider: voyage
                model: voyage-3-large
                input_type: query
                api_key_env_var: VOYAGE_API_KEY

          rerankers:
            voyage_rr:
              provider: voyage
              model: rerank-2.5
              api_key_env_var: VOYAGE_API_KEY

          steps:
            - {id: retrieved, op: retrieve, retriever: docs_vs, top_k: 20}
            - {id: reranked, op: rerank, source: retrieved, reranker: voyage_rr, top_k: 5}
            - id: answered
              op: generate
              source: reranked
              provider: openai
              model: gpt-4o-mini
              api_key_env_var: OPENAI_API_KEY
        ```
    """

    asset_name: str = Field(description="Output asset name")
    include_sources: bool = Field(
        default=True,
        description="Emit the retrieved docs list as `sources_column` on each output row.",
    )
    query_column: Union[str, int] = Field(default="query", description="Column name containing query text")
    answer_column: Union[str, int] = Field(default="answer", description="Column name for generated answers")
    sources_column: Union[str, int] = Field(default="sources", description="Column name for retrieved source documents")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 25 "
            "rows or a sample) for builder UIs."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata. For long DataFrames "
            "(>10x preview_rows), a random sample is used; otherwise head()."
        ),
    )
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with query text")

    # ── OPS-BASED MODE (opt-in via `steps:`) ────────────────────────────
    #
    # When `steps:` is set, the pipeline runs as a chain of named ops
    # (retrieve → rerank → generate → …) instead of the monolithic single-
    # asset shape. Enables multi-hop retrieval, hybrid search, reranking,
    # query expansion, and reasoning-model + prompt-caching support on the
    # generation step. All backward-compat: when `steps:` is empty/unset,
    # the pipeline behaves exactly as before.

    personas: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named reusable LLM sub-configs (ops mode only). Each persona "
            "bundles `{provider, model, api_key_env_var, api_base_env_var, "
            "system_prompt, temperature, max_tokens, reasoning_effort, "
            "thinking_budget, prompt_caching}`. Reference from a step via "
            "`persona: <name>`; the persona's fields are merged into the "
            "step (explicit inline fields win). Applies to `generate`, "
            "`expand_query`, and `refine` ops. Same shape as "
            "AgenticPipelineComponent."
        ),
    )
    retrievers: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named reusable vector-store configs (ops mode only). Each "
            "entry declares `{kind: chromadb|pinecone|qdrant|pgvector|"
            "weaviate, collection_name / index_name, connection / path / "
            "url, embedding: {provider, model, api_key_env_var}}`. "
            "Reference from a step via `retriever: <name>`."
        ),
    )
    rerankers: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named reusable reranker configs (ops mode only). Each entry: "
            "`{provider: voyage|cohere, model, api_key_env_var}`. "
            "Reference from a `rerank` step via `reranker: <name>`."
        ),
    )
    steps: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Ordered ops chain (opt-in — unlocks the ops-based mode). "
            "Each entry: `{id, op, ...op-specific}`. Supported ops: "
            "`retrieve` (vector search across a named retriever), "
            "`hybrid_search` (BM25 + vector combined via RRF), "
            "`rerank` (Voyage rerank-2.5 or Cohere rerank-3.5 over "
            "retrieved docs), `expand_query` (HyDE-style — LLM generates "
            "a hypothetical document to embed instead of the query), "
            "`generate` (final answer using retrieved docs as context). "
            "See README for per-op field reference."
        ),
    )
    query_prompt_template: Optional[str] = Field(
        default=None,
        description=(
            "Ops mode only. Template applied to `query_column` values before "
            "they're fed into the first step. Placeholders: `{query}` "
            "(raw query text), `{partition_key}`, `{partition.<name>}`. "
            "Leave unset (default) to pass queries through unchanged."
        ),
    )

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if not self.steps:
            raise ValueError(
                f"RAGPipelineComponent {self.asset_name!r}: `steps:` is required. "
                "Define a chain of ops — at minimum `retrieve` + `generate`. "
                "See the component README for examples."
            )
        return _build_ops_mode(self)





def _make_openai_client(api_key):
    """Build an OpenAI or AzureOpenAI client based on env vars.

    Set OPENAI_AZURE_ENDPOINT to route through Azure OpenAI Service. Optional:
    OPENAI_AZURE_API_VERSION (default 2024-10-21). For Entra OAuth, set
    OPENAI_AZURE_USE_ENTRA=1 and the standard AZURE_TENANT_ID/CLIENT_ID/
    CLIENT_SECRET env vars (or rely on managed identity in Azure compute).
    """
    import openai as _openai
    azure_endpoint = os.environ.get("OPENAI_AZURE_ENDPOINT")
    if not azure_endpoint:
        return _openai.OpenAI(api_key=api_key)
    api_version = os.environ.get("OPENAI_AZURE_API_VERSION", "2024-10-21")
    if os.environ.get("OPENAI_AZURE_USE_ENTRA") == "1":
        from azure.identity import DefaultAzureCredential, get_bearer_token_provider
        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(),
            "https://cognitiveservices.azure.com/.default",
        )
        return _openai.AzureOpenAI(
            azure_ad_token_provider=token_provider,
            azure_endpoint=azure_endpoint,
            api_version=api_version,
        )
    return _openai.AzureOpenAI(
        api_key=api_key,
        azure_endpoint=azure_endpoint,
        api_version=api_version,
    )


# ═════════════════════════════════════════════════════════════════════
# Ops-based mode (opt-in via `steps:`)
# ═════════════════════════════════════════════════════════════════════
#
# The ops-based mode decomposes RAG into named steps
# (retrieve → rerank → generate → ...) each producing a state dict that
# subsequent steps consume by id. Backed by LiteLLM for the generation
# side and native provider SDKs for the retrieval side. Fully opt-in:
# absent `steps:`, the legacy monolithic mode above runs unchanged.


# ── Persona / retriever / reranker resolution ─────────────────────────

_PERSONA_FIELDS = (
    "provider", "model", "api_key_env_var", "api_base_env_var",
    "system_prompt", "temperature", "max_tokens",
    "reasoning_effort", "thinking_budget", "prompt_caching",
)


def _merge_persona(target: dict, persona: dict) -> dict:
    """Merge persona fields into `target`. Explicit inline fields on
    `target` win — persona is a defaults-provider."""
    merged = dict(target)
    for k in _PERSONA_FIELDS:
        if k in persona and merged.get(k) is None:
            merged[k] = persona[k]
    merged.pop("persona", None)
    return merged


def _resolve_persona(step: dict, personas: Optional[Dict[str, Dict[str, Any]]]) -> dict:
    """If step has `persona: <name>`, merge that persona's fields into step."""
    if not personas or "persona" not in step:
        return step
    name = step["persona"]
    if name not in personas:
        raise ValueError(
            f"step {step.get('id', '?')!r} references persona {name!r} not in "
            f"personas: block. Available: {sorted(personas.keys())}"
        )
    return _merge_persona(step, personas[name])


def _resolve_retriever(name: str, retrievers: Optional[Dict[str, Dict[str, Any]]]) -> dict:
    """Look up a retriever spec by name."""
    if not retrievers or name not in retrievers:
        raise ValueError(
            f"retriever {name!r} not declared in `retrievers:` block. "
            f"Available: {sorted((retrievers or {}).keys())}"
        )
    return retrievers[name]


def _resolve_reranker(name: str, rerankers: Optional[Dict[str, Dict[str, Any]]]) -> dict:
    """Look up a reranker spec by name."""
    if not rerankers or name not in rerankers:
        raise ValueError(
            f"reranker {name!r} not declared in `rerankers:` block. "
            f"Available: {sorted((rerankers or {}).keys())}"
        )
    return rerankers[name]


# ── LLM completion (LiteLLM + reasoning + caching) ────────────────────
#
# Mirrors AgenticPipelineComponent._completion. Kept self-contained per
# the "components are self-contained" convention — no shared helpers.

def _rag_completion(
    *,
    model: str,
    system_prompt: Optional[str],
    user_prompt: str,
    api_key_env_var: Optional[str],
    api_base_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    reasoning_effort: Optional[str] = None,
    thinking_budget: Optional[int] = None,
    prompt_caching: bool = False,
) -> Dict[str, Any]:
    """Thin LiteLLM wrapper; returns
    {"content": str, "usage": {...}, "cost_usd": float, "latency_ms": int,
     "tokens_total": int, "cache_read_tokens": int, "cache_creation_tokens": int}."""
    import time as _time
    try:
        import litellm
    except ImportError:
        raise ImportError("rag_pipeline ops mode requires litellm>=1.30.0")
    litellm.drop_params = True

    m_lower = model.lower()
    is_openai_ish = m_lower.startswith(("gpt-", "o1", "o3", "o4", "openai/", "azure/", "groq/"))
    is_gemini = m_lower.startswith(("gemini/", "google/", "vertex_ai/gemini"))
    is_anthropic = (
        "claude" in m_lower or m_lower.startswith(("anthropic/", "bedrock/anthropic."))
    )

    messages: List[Dict[str, Any]] = []
    if system_prompt:
        if prompt_caching and is_anthropic:
            messages.append({
                "role": "system",
                "content": [
                    {"type": "text", "text": system_prompt,
                     "cache_control": {"type": "ephemeral"}}
                ],
            })
        else:
            messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user_prompt})

    kwargs: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if api_key_env_var and os.environ.get(api_key_env_var):
        kwargs["api_key"] = os.environ[api_key_env_var]
    if api_base_env_var and os.environ.get(api_base_env_var):
        kwargs["api_base"] = os.environ[api_base_env_var]
    if reasoning_effort is not None and (is_openai_ish or is_gemini):
        kwargs["reasoning_effort"] = reasoning_effort
    if thinking_budget is not None:
        if is_gemini:
            kwargs["thinking_budget"] = int(thinking_budget)
        elif is_anthropic:
            kwargs["thinking"] = {"type": "enabled", "budget_tokens": int(thinking_budget)}

    t0 = _time.time()
    response = litellm.completion(**kwargs)
    latency_ms = int((_time.time() - t0) * 1000)

    msg = response.choices[0].message
    content = msg.content or ""
    usage = None
    u = getattr(response, "usage", None)
    if u is not None:
        try:
            usage = u.model_dump()
        except AttributeError:
            try:
                usage = dict(u)
            except (TypeError, ValueError):
                usage = None

    cost_usd = 0.0
    try:
        cost_usd = float(litellm.completion_cost(completion_response=response))
    except Exception:
        cost_usd = 0.0

    tokens_total = 0
    cache_read_tokens = 0
    cache_creation_tokens = 0
    if usage is not None:
        tt = usage.get("total_tokens")
        if isinstance(tt, (int, float)):
            tokens_total = int(tt)
        for k in ("cache_read_input_tokens", "cache_read"):
            v = usage.get(k)
            if isinstance(v, (int, float)):
                cache_read_tokens = int(v)
                break
        for k in ("cache_creation_input_tokens", "cache_creation"):
            v = usage.get(k)
            if isinstance(v, (int, float)):
                cache_creation_tokens = int(v)
                break

    return {
        "content": content,
        "usage": usage,
        "cost_usd": cost_usd,
        "latency_ms": latency_ms,
        "tokens_total": tokens_total,
        "cache_read_tokens": cache_read_tokens,
        "cache_creation_tokens": cache_creation_tokens,
    }


# ── Provider dispatchers ──────────────────────────────────────────────


def _embed_query(embedding_cfg: dict, query: str) -> List[float]:
    """Embed a single query using the retriever's declared embedding provider."""
    provider = (embedding_cfg or {}).get("provider", "openai")
    model = (embedding_cfg or {}).get("model", "text-embedding-3-small")
    api_key = os.environ.get((embedding_cfg or {}).get("api_key_env_var") or "OPENAI_API_KEY")

    if provider == "openai":
        client = _make_openai_client(api_key)
        return client.embeddings.create(model=model, input=[query]).data[0].embedding
    if provider == "voyage":
        try:
            import voyageai
        except ImportError:
            raise ImportError("voyage embedding requires: pip install 'voyageai>=0.3.0'")
        client = voyageai.Client(api_key=api_key)
        input_type = (embedding_cfg or {}).get("input_type", "query")
        result = client.embed(texts=[query], model=model, input_type=input_type)
        return result.embeddings[0]
    if provider == "cohere":
        try:
            import cohere
        except ImportError:
            raise ImportError("cohere embedding requires: pip install cohere")
        client = cohere.Client(api_key=api_key)
        r = client.embed(texts=[query], model=model, input_type="search_query")
        return list(r.embeddings[0])
    if provider == "sentence_transformers":
        from sentence_transformers import SentenceTransformer
        st = SentenceTransformer(model)
        return st.encode([query])[0].tolist()
    raise ValueError(f"unsupported embedding provider: {provider!r}")


def _vector_search(retriever: dict, embedding: List[float], top_k: int) -> List[Dict[str, Any]]:
    """Run a vector search against a named retriever. Returns list of
    `{text, metadata, score}` dicts."""
    kind = retriever.get("kind")
    if kind == "chromadb":
        import chromadb
        client = chromadb.PersistentClient(path=retriever.get("path") or "./chroma_db")
        collection = client.get_collection(name=retriever["collection_name"])
        results = collection.query(query_embeddings=[embedding], n_results=top_k)
        docs = []
        for i in range(len(results["ids"][0])):
            docs.append({
                "text": results["documents"][0][i] if "documents" in results else "",
                "metadata": results["metadatas"][0][i] if "metadatas" in results else {},
                "score": 1.0 - float(results["distances"][0][i]) if "distances" in results else 0.0,
            })
        return docs
    if kind == "pinecone":
        from pinecone import Pinecone
        api_key = os.environ.get(retriever.get("api_key_env_var") or "PINECONE_API_KEY")
        pc = Pinecone(api_key=api_key)
        index = pc.Index(retriever["index_name"])
        r = index.query(
            vector=embedding, top_k=top_k, include_metadata=True,
            namespace=retriever.get("namespace"),
        )
        return [
            {"text": m.get("metadata", {}).get("text", ""),
             "metadata": m.get("metadata", {}),
             "score": float(m.get("score", 0.0))}
            for m in r["matches"]
        ]
    if kind == "qdrant":
        from qdrant_client import QdrantClient
        url_env = retriever.get("url_env_var")
        url = os.environ.get(url_env) if url_env else retriever.get("url")
        api_key = os.environ.get(retriever.get("api_key_env_var") or "QDRANT_API_KEY")
        client = QdrantClient(url=url, api_key=api_key) if url else QdrantClient(path=retriever.get("path") or "./qdrant_db")
        hits = client.search(
            collection_name=retriever["collection_name"],
            query_vector=embedding, limit=top_k,
        )
        return [
            {"text": (h.payload or {}).get("text", ""),
             "metadata": h.payload or {},
             "score": float(h.score)}
            for h in hits
        ]
    if kind == "pgvector":
        # Uses psycopg + a simple ORDER BY <cosine> LIMIT k. Assumes
        # column names {content_column, embedding_column, metadata_column?}.
        import psycopg2
        conn_env = retriever.get("connection_env_var") or "PGVECTOR_URL"
        conn = psycopg2.connect(os.environ[conn_env])
        try:
            table = retriever["table"]
            emb_col = retriever.get("embedding_column", "embedding")
            content_col = retriever.get("content_column", "content")
            meta_col = retriever.get("metadata_column")
            select_cols = f"{content_col}, 1 - ({emb_col} <=> %s::vector) AS score"
            if meta_col:
                select_cols += f", {meta_col}"
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {select_cols} FROM {table} ORDER BY {emb_col} <=> %s::vector LIMIT %s",
                    (embedding, embedding, top_k),
                )
                rows = cur.fetchall()
            docs = []
            for row in rows:
                d = {"text": row[0], "score": float(row[1]), "metadata": {}}
                if meta_col:
                    d["metadata"] = row[2] if isinstance(row[2], dict) else {}
                docs.append(d)
            return docs
        finally:
            conn.close()
    if kind == "weaviate":
        import weaviate
        url_env = retriever.get("url_env_var") or "WEAVIATE_URL"
        client = weaviate.Client(url=os.environ[url_env])
        class_name = retriever["collection_name"]
        result = (client.query
                  .get(class_name, [retriever.get("content_property", "text")])
                  .with_near_vector({"vector": embedding})
                  .with_limit(top_k)
                  .with_additional(["distance"])
                  .do())
        objs = result.get("data", {}).get("Get", {}).get(class_name, [])
        content_prop = retriever.get("content_property", "text")
        return [
            {"text": o.get(content_prop, ""), "metadata": o,
             "score": 1.0 - float(o.get("_additional", {}).get("distance", 0.0))}
            for o in objs
        ]
    raise ValueError(f"unsupported retriever kind: {kind!r}")


def _rerank_docs(reranker: dict, query: str, docs: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    """Rerank candidate docs with Voyage or Cohere. Preserves original
    metadata; overwrites `score` with the reranker's relevance score."""
    provider = reranker.get("provider", "voyage")
    model = reranker.get("model", "rerank-2.5" if provider == "voyage" else "rerank-3.5")
    api_key = os.environ.get(reranker.get("api_key_env_var") or ("VOYAGE_API_KEY" if provider == "voyage" else "COHERE_API_KEY"))
    doc_texts = [d.get("text", "") for d in docs]

    if provider == "voyage":
        try:
            import voyageai
        except ImportError:
            raise ImportError("voyage rerank requires: pip install 'voyageai>=0.3.0'")
        client = voyageai.Client(api_key=api_key)
        r = client.rerank(query=query, documents=doc_texts, model=model, top_k=top_k)
        # r.results is a list of RerankingResult with `.index` + `.relevance_score`
        out = []
        for res in r.results:
            base = docs[res.index]
            out.append({**base, "score": float(res.relevance_score)})
        return out
    if provider == "cohere":
        try:
            import cohere
        except ImportError:
            raise ImportError("cohere rerank requires: pip install cohere")
        client = cohere.Client(api_key=api_key)
        r = client.rerank(query=query, documents=doc_texts, model=model, top_n=top_k)
        out = []
        for res in r.results:
            base = docs[res.index]
            out.append({**base, "score": float(res.relevance_score)})
        return out
    raise ValueError(f"unsupported rerank provider: {provider!r}")


def _bm25_search(docs: List[Dict[str, Any]], query: str, top_k: int) -> List[Dict[str, Any]]:
    """Rank a corpus of already-loaded docs with BM25. Returns top_k."""
    try:
        from rank_bm25 import BM25Okapi
    except ImportError:
        raise ImportError("hybrid_search requires: pip install rank_bm25")
    tokenized_corpus = [d.get("text", "").lower().split() for d in docs]
    bm25 = BM25Okapi(tokenized_corpus)
    scores = bm25.get_scores(query.lower().split())
    ranked = sorted(zip(scores, docs), key=lambda x: x[0], reverse=True)[:top_k]
    return [{**d, "score": float(s)} for s, d in ranked]


def _rrf_combine(ranked_lists: List[List[Dict[str, Any]]], k: int = 60, top_k: int = 10) -> List[Dict[str, Any]]:
    """Reciprocal Rank Fusion — combine N ranked lists into one.
    Dedupes by doc text; assigns each doc a fused score of Σ 1/(k + rank)."""
    fused: Dict[str, Dict[str, Any]] = {}
    for lst in ranked_lists:
        for rank, doc in enumerate(lst, start=1):
            key = doc.get("text", "")[:500]  # cheap dedupe key
            entry = fused.get(key) or {**doc, "score": 0.0}
            entry["score"] = float(entry["score"]) + 1.0 / (k + rank)
            fused[key] = entry
    ordered = sorted(fused.values(), key=lambda d: d["score"], reverse=True)
    return ordered[:top_k]


# ── Ops (5): retrieve / hybrid_search / rerank / expand_query / generate ─


def _current_query(state: Dict[str, Any], step: dict) -> str:
    """Resolve the query string for this step. Steps can override with
    `query_source: <step_id>` to consume a prior step's `query` field
    (used by expand_query → retrieve for HyDE)."""
    src = step.get("query_source")
    if src:
        if src not in state:
            raise ValueError(f"query_source={src!r} not in state; known: {sorted(state)}")
        s = state[src]
        return s.get("query") or s.get("text") or ""
    return state.get("__query__", "")


def _current_docs(state: Dict[str, Any], step: dict) -> List[Dict[str, Any]]:
    """Resolve the doc list to feed a downstream step (rerank / generate)."""
    src = step.get("source")
    if src:
        if src not in state:
            raise ValueError(f"source={src!r} not in state; known: {sorted(state)}")
        return state[src].get("docs") or []
    # Fall back to most recent step that produced docs.
    for k in reversed(list(state.keys())):
        if isinstance(state[k], dict) and state[k].get("docs") is not None:
            return state[k]["docs"]
    return []


def _do_retrieve(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context) -> Dict[str, Any]:
    """Vector search across a named retriever."""
    retriever = _resolve_retriever(step["retriever"], comp.retrievers)
    top_k = int(step.get("top_k", 10))
    query = _current_query(state, step)
    if not query:
        raise ValueError("retrieve: no query available (set __query__ or query_source)")
    emb = _embed_query(retriever.get("embedding") or {}, query)
    docs = _vector_search(retriever, emb, top_k)
    context.log.info(f"[retrieve:{step['id']}] retriever={step['retriever']!r} top_k={top_k} → {len(docs)} docs")
    return {"op": "retrieve", "query": query, "docs": docs, "top_k": top_k,
            "retriever": step["retriever"]}


def _do_hybrid_search(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context) -> Dict[str, Any]:
    """BM25 + vector search combined via Reciprocal Rank Fusion (RRF).
    Requires `retriever:` (vector) + `bm25_corpus_source:` (a prior step
    that produced docs) OR `bm25_corpus_docs:` inline."""
    retriever = _resolve_retriever(step["retriever"], comp.retrievers)
    top_k = int(step.get("top_k", 10))
    query = _current_query(state, step)

    # Vector arm — pull top_k*2 to give RRF headroom.
    emb = _embed_query(retriever.get("embedding") or {}, query)
    vec_hits = _vector_search(retriever, emb, top_k * 2)

    # BM25 arm — needs an in-memory corpus. Users pass a prior step id
    # whose docs form the BM25 corpus (typical pattern: retrieve → bm25
    # on same corpus with a wider recall to get more diversity), OR
    # supply an inline list.
    corpus_source = step.get("bm25_corpus_source")
    if corpus_source:
        if corpus_source not in state:
            raise ValueError(f"bm25_corpus_source={corpus_source!r} not in state")
        corpus = state[corpus_source].get("docs") or []
    else:
        corpus = step.get("bm25_corpus_docs") or vec_hits  # default: rerank same vec_hits
    bm25_hits = _bm25_search(corpus, query, top_k * 2)

    fused = _rrf_combine([vec_hits, bm25_hits], k=int(step.get("rrf_k", 60)), top_k=top_k)
    context.log.info(f"[hybrid_search:{step['id']}] vec={len(vec_hits)} bm25={len(bm25_hits)} → fused={len(fused)}")
    return {"op": "hybrid_search", "query": query, "docs": fused, "top_k": top_k,
            "retriever": step["retriever"]}


def _do_rerank(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context) -> Dict[str, Any]:
    """Rerank docs from a prior step using Voyage rerank-2.5 or Cohere rerank-3.5."""
    reranker = _resolve_reranker(step["reranker"], comp.rerankers)
    docs = _current_docs(state, step)
    query = _current_query(state, step)
    top_k = int(step.get("top_k", 5))
    if not docs:
        return {"op": "rerank", "query": query, "docs": [], "top_k": top_k, "reranker": step["reranker"]}
    reranked = _rerank_docs(reranker, query, docs, top_k)
    context.log.info(f"[rerank:{step['id']}] {step['reranker']!r} {len(docs)}→{len(reranked)}")
    return {"op": "rerank", "query": query, "docs": reranked, "top_k": top_k, "reranker": step["reranker"]}


def _do_expand_query(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context) -> Dict[str, Any]:
    """HyDE — LLM generates a hypothetical answer to embed instead of the
    raw query. Improves retrieval when queries are short/underspecified."""
    step = _resolve_persona(step, comp.personas)
    query = _current_query(state, step)
    system_prompt = step.get("system_prompt") or (
        "Given a user question, write a concise, factual paragraph that would "
        "answer it. This will be used for semantic search, so include the "
        "kind of terminology and details you'd expect in a source document."
    )
    prompt_template = step.get("prompt_template", "Question: {query}\n\nHypothetical answer:")
    prompt = prompt_template.replace("{query}", query)

    result = _rag_completion(
        model=step.get("model") or "gpt-4o-mini",
        system_prompt=system_prompt,
        user_prompt=prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=float(step.get("temperature", 0.3)),
        max_tokens=int(step.get("max_tokens", 300)),
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
        prompt_caching=bool(step.get("prompt_caching", False)),
    )
    hypothetical = result["content"] or ""
    # New "query" downstream steps will see is the hypothetical answer;
    # keep the original query available under original_query.
    context.log.info(f"[expand_query:{step['id']}] {len(hypothetical)}c hypothetical")
    return {
        "op": "expand_query",
        "query": hypothetical,
        "original_query": query,
        "cost_usd": result["cost_usd"],
        "latency_ms": result["latency_ms"],
        "tokens_total": result["tokens_total"],
    }


def _do_generate(step: dict, state: Dict[str, Any], comp: "RAGPipelineComponent", context) -> Dict[str, Any]:
    """Generate the final answer using retrieved/reranked docs as context."""
    step = _resolve_persona(step, comp.personas)
    query = _current_query(state, step)
    docs = _current_docs(state, step)
    docs_context = "\n\n---\n\n".join(
        d.get("text", "") for d in docs
    )
    prompt_template = step.get("prompt_template") or (
        "Answer the question using ONLY the provided context. If the context "
        "doesn't contain the answer, say so explicitly.\n\n"
        "Context:\n{context}\n\nQuestion: {query}\n\nAnswer:"
    )
    prompt = (
        prompt_template
        .replace("{context}", docs_context)
        .replace("{query}", query)
    )
    result = _rag_completion(
        model=step.get("model") or "gpt-4o-mini",
        system_prompt=step.get("system_prompt"),
        user_prompt=prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=float(step.get("temperature", 0.0)),
        max_tokens=int(step.get("max_tokens", 2048)),
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
        prompt_caching=bool(step.get("prompt_caching", False)),
    )
    context.log.info(f"[generate:{step['id']}] {result['tokens_total']} tokens, ${result['cost_usd']:.5f}")
    return {
        "op": "generate",
        "query": query,
        "answer": result["content"],
        "docs": docs,  # forward downstream
        "cost_usd": result["cost_usd"],
        "latency_ms": result["latency_ms"],
        "tokens_total": result["tokens_total"],
        "cache_read_tokens": result["cache_read_tokens"],
        "cache_creation_tokens": result["cache_creation_tokens"],
    }


_OPS = {
    "retrieve": _do_retrieve,
    "hybrid_search": _do_hybrid_search,
    "rerank": _do_rerank,
    "expand_query": _do_expand_query,
    "generate": _do_generate,
}


# ── Ops-mode asset builder ────────────────────────────────────────────


def _build_ops_mode(comp: "RAGPipelineComponent") -> Definitions:
    """Build the ops-mode asset: for each row in `upstream_asset_key`,
    run the `steps:` chain and materialize the final step's answer +
    aggregate cost/latency."""
    asset_name = comp.asset_name
    steps_cfg = list(comp.steps or [])
    upstream_asset_key = comp.upstream_asset_key
    query_column = comp.query_column
    answer_column = comp.answer_column
    sources_column = comp.sources_column
    include_sources = comp.include_sources
    query_prompt_template = comp.query_prompt_template

    if not steps_cfg:
        raise ValueError("ops mode requires at least one step in `steps:`")
    for s in steps_cfg:
        if "id" not in s or "op" not in s:
            raise ValueError(f"every step needs {{id, op}}; got: {s}")
        if s["op"] not in _OPS:
            raise ValueError(
                f"step {s['id']!r} op={s['op']!r} unsupported. Valid: {sorted(_OPS)}"
            )

    partitions_def = _build_partitions_def(
        comp.partition_type, comp.partition_start, comp.partition_values,
        comp.dynamic_partition_name, comp.partition_dimensions,
    )
    group_name = comp.group_name
    owners = comp.owners or []
    description = comp.description or "RAG pipeline (ops mode)"

    _inferred_kinds = list(comp.kinds or ["ai", "rag", "pipeline"])
    _all_tags = dict(comp.asset_tags or {})
    for _kind in _inferred_kinds:
        _all_tags[f"dagster/kind/{_kind}"] = ""

    _freshness_policy = None
    if comp.freshness_max_lag_minutes is not None:
        from dagster import FreshnessPolicy
        _freshness_policy = FreshnessPolicy(
            maximum_lag_minutes=comp.freshness_max_lag_minutes,
            cron_schedule=comp.freshness_cron,
        )

    _retry_policy = None
    if comp.retry_policy_max_retries is not None:
        from dagster import Backoff, RetryPolicy
        _retry_policy = RetryPolicy(
            max_retries=comp.retry_policy_max_retries,
            delay=comp.retry_policy_delay_seconds or 1,
            backoff=Backoff[comp.retry_policy_backoff.upper()],
        )

    @asset(
        key=AssetKey.from_user_string(asset_name),
        partitions_def=partitions_def,
        group_name=group_name,
        description=description,
        owners=owners,
        tags=_all_tags,
        freshness_policy=_freshness_policy,
        retry_policy=_retry_policy,
        ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        deps=[AssetKey.from_user_string(k) for k in (comp.deps or [])],
    )
    def _rag_ops_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
        # Defensive Output/MaterializeResult unwrap.
        if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
            upstream = upstream.value
        if isinstance(upstream, dict):
            _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
            upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

        df = upstream.copy()
        if query_column not in df.columns:
            raise ValueError(f"query_column {query_column!r} not in upstream columns: {list(df.columns)}")

        # Partition-key substitution values.
        subs = {"run_id": context.run_id}
        if context.has_partition_key:
            pk = context.partition_key
            if hasattr(pk, "keys_by_dimension"):
                subs["partition_key"] = str(pk)
                subs["partition"] = dict(pk.keys_by_dimension)
            else:
                subs["partition_key"] = str(pk)
                subs["partition"] = {}

        answers: List[str] = []
        sources_per_row: List[List[Dict[str, Any]]] = []
        totals = {"cost_usd": 0.0, "latency_ms": 0, "tokens_total": 0,
                  "cache_read_tokens": 0, "cache_creation_tokens": 0}

        for idx, row in df.iterrows():
            raw_query = str(row[query_column])
            query = raw_query
            if query_prompt_template:
                query = query_prompt_template.replace("{query}", raw_query).replace(
                    "{partition_key}", str(subs.get("partition_key", ""))
                )
                for k, v in (subs.get("partition") or {}).items():
                    query = query.replace(f"{{partition.{k}}}", str(v))
            state: Dict[str, Any] = {"__query__": query}

            for step in steps_cfg:
                op_fn = _OPS[step["op"]]
                context.log.info(f"[row {idx+1}/{len(df)}] step={step['id']} op={step['op']}")
                state[step["id"]] = op_fn(step, state, comp, context)

            # The last step's outputs become this row's answer.
            last = state[steps_cfg[-1]["id"]]
            answers.append(last.get("answer") or last.get("content") or "")
            sources_per_row.append(last.get("docs") or [])

            for k in totals:
                # Sum across all steps this row.
                for step_out in state.values():
                    if isinstance(step_out, dict) and k in step_out:
                        totals[k] += step_out[k] or 0

        df[answer_column] = answers
        if include_sources:
            df[sources_column] = sources_per_row

        from dagster import TableSchema, TableColumn
        _col_schema = TableSchema(columns=[
            TableColumn(name=str(c), type=str(df.dtypes[c])) for c in df.columns
        ])
        context.add_output_metadata({
            "dagster/row_count": MetadataValue.int(len(df)),
            "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            "n_steps": MetadataValue.int(len(steps_cfg)),
            "step_ids": MetadataValue.text(" → ".join(s["id"] for s in steps_cfg)),
            "total_cost_usd": MetadataValue.float(totals["cost_usd"]),
            "total_latency_ms": MetadataValue.int(totals["latency_ms"]),
            "total_tokens": MetadataValue.int(totals["tokens_total"]),
            "cache_read_tokens": MetadataValue.int(totals["cache_read_tokens"]),
            "cache_creation_tokens": MetadataValue.int(totals["cache_creation_tokens"]),
        })
        return df

    from dagster import build_column_schema_change_checks
    _schema_checks = build_column_schema_change_checks(assets=[_rag_ops_asset])
    return Definitions(assets=[_rag_ops_asset], asset_checks=list(_schema_checks))
