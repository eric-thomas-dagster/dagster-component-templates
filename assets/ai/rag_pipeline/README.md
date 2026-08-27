# RAG Pipeline

> **🔑 API keys required.** LLM (`OPENAI_API_KEY` / `ANTHROPIC_API_KEY` / etc. — LiteLLM-backed) and any vector store / reranker (`VOYAGE_API_KEY`, `COHERE_API_KEY`, `PINECONE_API_KEY`, `PGVECTOR_URL`, ...).

**One YAML. Whole RAG pipeline.** Family sibling of `agentic_pipeline` / `ml_pipeline` / `polars_pipeline`. Decomposes RAG into named ops: `retrieve` / `hybrid_search` / `rerank` / `expand_query` / `generate`.

## Why Dagster (not just a job runner)

Every step's cost / latency / token usage rolls up into first-class asset metadata. Click the asset, see every past materialization: which docs the retriever picked, reranker scores, generator tokens + cost + cache-hit rate — no log-grepping.

**Metadata emitted per materialization** (all present by default):

| Field | Type | What Dagster does with it |
|---|---|---|
| `total_cost_usd` | Float | Promote to a Dagster+ Insights custom metric via the UI; dashboards + alerts follow. |
| `total_latency_ms` | Int | Same — plot over time / alert on regressions. |
| `total_tokens` | Int | Same — set token budget alerts. |
| `cache_read_tokens` | Int | Anthropic cache-hit tokens (when `prompt_caching: true`). Ratio to `total_tokens` = cache-hit rate. |
| `cache_creation_tokens` | Int | Anthropic cache-warm-up tokens on first call. |
| `n_steps`, `step_ids` | Int / Text | Which ops ran, in order. |
| `dagster/row_count`, `dagster/column_schema` | Row + column shape. |

## Quick example

```yaml
type: dagster_community_components.RAGPipelineComponent
attributes:
  asset_name: docs_qa
  upstream_asset_key: user_questions      # DataFrame with a `query` column
  query_column: query
  answer_column: answer
  sources_column: sources

  personas:
    fast_gen:
      provider: openai
      model: gpt-4o-mini
      api_key_env_var: OPENAI_API_KEY
      temperature: 0.0
      max_tokens: 1024

  retrievers:
    docs_vs:
      kind: chromadb
      collection_name: product_docs
      path: ./chroma_db
      embedding:
        provider: voyage                    # SOTA MTEB retrieval
        model: voyage-3-large
        input_type: query                   # asymmetric — pair with input_type=document at indexing
        api_key_env_var: VOYAGE_API_KEY

  rerankers:
    voyage_rr:
      provider: voyage
      model: rerank-2.5
      api_key_env_var: VOYAGE_API_KEY

  steps:
    - id: retrieved
      op: retrieve
      retriever: docs_vs
      top_k: 20                              # wide recall

    - id: reranked
      op: rerank
      source: retrieved
      reranker: voyage_rr
      top_k: 5                               # narrow to top-5 for the LLM

    - id: answered
      op: generate
      source: reranked
      persona: fast_gen
      system_prompt: >
        You are a helpful product docs assistant. Answer using ONLY the
        provided context. If the answer isn't in the context, say so.
```

## Op menu

| op | What it does | Config |
|---|---|---|
| `retrieve` | Vector search against a named retriever. Embeds the query using the retriever's declared embedding provider (openai / voyage / cohere / sentence_transformers), then hits the vector store. | `retriever: <name>`, `top_k` |
| `hybrid_search` | BM25 + vector combined via Reciprocal Rank Fusion (RRF). BM25 corpus comes from a prior step (`bm25_corpus_source`) OR falls back to the vector hits. Big precision lift on keyword-heavy queries. | `retriever: <name>`, `top_k`, `bm25_corpus_source`, `rrf_k` |
| `rerank` | Reranks docs from a prior step with Voyage `rerank-2.5` or Cohere `rerank-3.5`. Overwrites `score` on each doc. Ideal shape: retrieve wide (top_20) → rerank narrow (top_5). | `reranker: <name>`, `source`, `top_k` |
| `expand_query` | HyDE — LLM writes a hypothetical answer to the query; downstream `retrieve` embeds that instead of the raw query. Improves retrieval on short/underspecified queries. | `persona`, `system_prompt`, `prompt_template` |
| `generate` | Final answer using retrieved/reranked docs as `{context}` in the prompt. LiteLLM-backed; supports `reasoning_effort` (o1/o3, Gemini 2.5+) + `thinking_budget` (Gemini native / Anthropic thinking) + `prompt_caching` (Anthropic ephemeral cache). | `persona`, `system_prompt`, `prompt_template`, `source` |

**Wiring:** any step reads docs from a prior step via `source: <step_id>` and query text via `query_source: <step_id>` (or falls back to the row's query). Omit both to consume the most recent step.

## Reusable blocks

### `personas:` — reusable LLM sub-configs

Declare once, reference by name from any `generate` / `expand_query` step. Fields (all optional; merged into the referencing step, explicit inline fields win):

```
provider, model, api_key_env_var, api_base_env_var,
system_prompt, temperature, max_tokens,
reasoning_effort, thinking_budget, prompt_caching
```

Same shape as `AgenticPipelineComponent.personas`, so `reasoning_effort: low|medium|high` (OpenAI o1/o3 + Gemini 2.5+), `thinking_budget: <int>` (Gemini native / Anthropic thinking mode), and `prompt_caching: true` (Anthropic — wraps system prompt with `cache_control: {type: ephemeral}`) all just work. Provider-family-filtered so a persona can be reused across mixed-provider steps.

### `retrievers:` — reusable vector-store configs

| kind | Required fields | Optional |
|---|---|---|
| `chromadb` | `collection_name` | `path` (default `./chroma_db`) |
| `pinecone` | `index_name` | `namespace`, `api_key_env_var` (default `PINECONE_API_KEY`) |
| `qdrant` | `collection_name` + (`url` or `url_env_var` or `path`) | `api_key_env_var` (default `QDRANT_API_KEY`) |
| `pgvector` | `table`, `connection_env_var` (default `PGVECTOR_URL`) | `embedding_column` (default `embedding`), `content_column` (default `content`), `metadata_column` |
| `weaviate` | `collection_name`, `url_env_var` (default `WEAVIATE_URL`) | `content_property` (default `text`) |

Every retriever declares its **embedding provider** — that's what runs on query text at retrieval time:

```yaml
retrievers:
  docs_vs:
    kind: chromadb
    collection_name: docs
    path: ./chroma_db
    embedding:
      provider: voyage                # or 'openai' | 'cohere' | 'sentence_transformers'
      model: voyage-3-large
      input_type: query               # Voyage asymmetric — 'query' at retrieval, 'document' when indexing
      api_key_env_var: VOYAGE_API_KEY
```

### `rerankers:` — reusable reranker configs

```yaml
rerankers:
  voyage_rr:
    provider: voyage
    model: rerank-2.5                # Voyage SOTA reranker
    api_key_env_var: VOYAGE_API_KEY

  cohere_rr:
    provider: cohere
    model: rerank-3.5
    api_key_env_var: COHERE_API_KEY
```

## Composition patterns

### Hybrid search + rerank

```yaml
steps:
  - id: hybrid_hits
    op: hybrid_search
    retriever: docs_vs
    top_k: 30

  - id: reranked
    op: rerank
    source: hybrid_hits
    reranker: voyage_rr
    top_k: 5

  - id: answered
    op: generate
    source: reranked
    persona: fast_gen
```

### HyDE query expansion

```yaml
steps:
  - id: hyde
    op: expand_query
    persona: fast_gen                  # cheap LLM writes a hypothetical answer

  - id: retrieved
    op: retrieve
    query_source: hyde                 # embed the HyDE output, not the raw query
    retriever: docs_vs
    top_k: 10

  - id: answered
    op: generate
    source: retrieved
    persona: fast_gen
```

### Reasoning-model generation with prompt caching

```yaml
personas:
  thinking_gen:
    provider: anthropic
    model: claude-sonnet-4-6
    api_key_env_var: ANTHROPIC_API_KEY
    thinking_budget: 4096              # Claude thinking mode — reasoning trace
    prompt_caching: true               # ~90% cheaper on cached system prompt

steps:
  - id: retrieved
    op: retrieve
    retriever: docs_vs
    top_k: 5

  - id: answered
    op: generate
    source: retrieved
    persona: thinking_gen
    system_prompt: |
      <long system prompt — hits the cache on subsequent runs>
```

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output asset name |
| `upstream_asset_key` | `str` | Upstream asset key providing a DataFrame with query text |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Asset description |
| `group_name` | `str` | — | Asset group |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']} |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `include_sources` | `bool` | `true` | Emit the retrieved docs list as `sources_column` on each output row. |
| `query_column` | `Union[str, int]` | `"query"` | Column name containing query text |
| `answer_column` | `Union[str, int]` | `"answer"` | Column name for generated answers |
| `sources_column` | `Union[str, int]` | `"sources"` | Column name for retrieved source documents |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output data in metadata (first 25 rows or a sample) for builder UIs. |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata. For long DataFrames (>10x preview_rows), a random sample is used; otherwise head(). |
| `personas` | `Dict[str, Dict[str, Any]]` | — | Named reusable LLM sub-configs (ops mode only). Each persona bundles `{provider, model, api_key_env_var, api_base_env_var, system_prompt, temperature, max_tokens, reasoning_effort, thinking_budget, prompt_caching}`. Refe… _(full docs in schema.json + component README)_ |
| `retrievers` | `Dict[str, Dict[str, Any]]` | — | Named reusable vector-store configs (ops mode only). Each entry declares `{kind: chromadb\|pinecone\|qdrant\|pgvector\|weaviate, collection_name / index_name, connection / path / url, embedding: {provider, model, api_key… _(full docs in schema.json + component README)_ |
| `rerankers` | `Dict[str, Dict[str, Any]]` | — | Named reusable reranker configs (ops mode only). Each entry: `{provider: voyage\|cohere, model, api_key_env_var}`. Reference from a `rerank` step via `reranker: <name>`. |
| `steps` | `List[Dict[str, Any]]` | — | Ordered ops chain (opt-in — unlocks the ops-based mode). Each entry: `{id, op, ...op-specific}`. Supported ops: `retrieve` (vector search across a named retriever), `hybrid_search` (BM25 + vector combined via RRF), `rera… _(full docs in schema.json + component README)_ |
| `query_prompt_template` | `str` | — | Ops mode only. Template applied to `query_column` values before they're fed into the first step. Placeholders: `{query}` (raw query text), `{partition_key}`, `{partition.<name>}`. Leave unset (default) to pass queries through unchanged. |

[//]: # (FIELDS:END)

## Model support

**Generation** — any LiteLLM-backed provider:

```yaml
provider: openai    → model: gpt-4o, gpt-4o-mini, o1-mini, o3
provider: anthropic → model: claude-opus-4-7, claude-sonnet-4-6, claude-haiku-4-5-20251001
provider: gemini    → model: gemini/gemini-2.5-flash, gemini/gemini-2.5-pro
provider: groq      → model: groq/llama-3.3-70b-versatile
provider: bedrock   → model: bedrock/anthropic.claude-3-5-sonnet-…
provider: ollama    → model: ollama/llama3.2
```

**Embeddings** (declared per retriever) — `openai` / `voyage` / `cohere` / `sentence_transformers`.

**Rerankers** — `voyage` (rerank-2.5) / `cohere` (rerank-3.5).

## When to reach for this vs `AgenticPipelineComponent`

- **`rag_pipeline`** — the pipeline shape IS retrieval → generation. Every row of upstream is a query; output is an answer. Batteries-included for RAG (retrievers, rerankers, HyDE, hybrid search).
- **`agentic_pipeline`** — general multi-step LLM workflow: debate, critique loops, tool use, sub-pipelines. Use if you want to compose RAG as one step of a larger agent workflow.
