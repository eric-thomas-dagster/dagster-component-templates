# HybridKnowledgeSearchServerComponent

Combined vector + graph retrieval over one corpus, joined by `chunk_id`.
Pair `context_engineering_pipeline` (a vector knowledge base) with
`knowledge_graph_builder` (a knowledge graph) over the SAME chunks (see
each component's own "shared chunk-id contract") and search both through
ONE MCP server tool. Agents can still use `knowledge_search_server` /
`knowledge_graph_search_server` separately -- this adds the combined one.

Same dual-mode shape as those two:

1. An importable `HybridKnowledgeSearchResource`, registered under `resource_key`.
2. A self-contained stdio MCP server: `python -m
   dagster_community_components.assets.ai.hybrid_knowledge_search_server.component`.

## Config

Two independent, nested blocks -- not this component's own flat fields,
since they're genuinely separate store configurations being composed:

- **`vector_store`**: identical fields to `knowledge_search_server`'s own
  `KnowledgeSearchResource` (`provider`, `connection_string`,
  `collection_name` [required], `api_key_env_var`, `embed_provider`,
  `embed_model`, `embed_api_key_env_var`, `text_metadata_key`,
  `vector_bucket_name`, `aws_region`, `demo_mode`).
- **`graph`**: identical fields to `knowledge_graph_search_server`'s own
  `KnowledgeGraphSearchResource` (`provider`, `path`, `uri`,
  `username_env_var`, `password_env_var`, `database`, `host`, `port`,
  `graph_name`, and, for `provider: sql_tables`: `sql_dialect`,
  `output_table`, `snowflake_*`, `databricks_*`, `bigquery_project_id` --
  see that component's README for the full field list and caveats).

## The `search` tool

`search(query, top_k=5, hops=1, mode="hybrid")`:

- **`mode: vector`**: vector search only -- `{"passages": [...]}`, no graph expansion.
- **`mode: graph`**: starts from entities matching the query text (fuzzy
  name/alias search), expands `hops` steps, and returns every chunk those
  entities (and their neighbors) appear in, plus the supporting triples --
  no vector step.
- **`mode: hybrid`** (default): vector search AND graph expansion (seeded
  by both the entities mentioned in the top vector hits AND entities
  matching the query text directly), merged and de-duplicated by
  `chunk_id`. Each passage's `paths` field names how it was found
  (`"vector"`, `"graph:hop_<N>:<entity>"`, or both).

Graph-only passages (reached by traversal, not a direct vector hit) have
`text: null` **unless** `vector_store.provider` is `chromadb`, in which
case a best-effort `collection.get(ids=[chunk_id])` backfills it --
chromadb is the only provider here with a cheap get-by-id lookup; every
other provider is left `null` rather than guessed at.

Every result is capped by `max_results` (default 50).

## Closed-loop receipts (`receipts_db_path`)

Without this, a knowledge base built with `context_engineering_pipeline`'s `receipts:`
policy learns nothing when agents reach it through this search server -- only the generated
FastMCP gateway and `rag_pipeline` ever called `record_retrieval`.

Set `receipts_db_path` to a SQLite file path and:
- `search` records a retrieval receipt per call that has a vector component (`mode: vector` or
  `hybrid`) for its vector-sourced passages (chunk ids and scores only -- never the query or
  chunk text) and returns `receipt_id`. `mode: graph` alone records nothing (no ranked vector
  retrieval to attribute a receipt to).
- Quarantined chunks (`feedback_quarantined` metadata, the same field `write_vector_store`'s
  receipts policy stamps) are always dropped from vector results, whether or not
  `receipts_db_path` is set.
- A new `record_feedback(receipt_id, signal, user_id?)` tool is exposed. `signal` is one of
  `thumbs_up`, `thumbs_down`, `accepted`, `edited`, `escalated`, `reopened`, `closed_won`,
  `closed_lost`, `reformulated`. `user_id`, if given, becomes the outcome's `outcome_ref`: a
  later call with the same `user_id` for the same `receipt_id` supersedes the earlier one.
- The inspection asset reports `retrievals_recorded`/`outcomes_recorded` counts.

**Why a plain file path, not a `receipts_resource_key`:** `search`/`record_feedback` run
inside this server's own stdio MCP subprocess (`python -m <this module>`, launched from plain
environment variables), which has no Dagster `context.resources` to resolve a resource key
from. `receipts_db_path` is read directly by that subprocess (via
`HYBRID_KG_SEARCH_RECEIPTS_DB_PATH` when launched through `mcp_server_resource_key`) using a
small, self-contained SQLite writer -- not the full multi-backend `ContextReceiptStore` (no
bigquery/sql backends, no usage sensor, no rerank priors here). Point a real
`ContextReceiptStoreComponent(backend="sqlite", database_path=<same path>)` resource at the
SAME file elsewhere in your project to read these receipts back. See
`knowledge_search_server`'s own README for the same mechanism in more detail.

Outcomes recorded via `record_feedback` are real-time, in-session feedback. For outcomes that
arrive later from an external system, use `receipt_outcome_ingestion` instead.

## Other tools (drive it step by step)

| Tool | Returns |
|---|---|
| `search_entities(query, type?)` | Fuzzy name/alias search over graph entities |
| `get_entity(name)` | Exact-name lookup: attributes, relations, evidence |
| `get_neighbors(name, hops=1, relation?)` | Entities within `hops` (capped at 3) |
| `find_path(source, target, max_hops=4)` | Shortest path (capped at 6 hops) between two entities |
| `get_chunks_for_entity(name)` | Every `chunk_id` an entity appears in |
| `get_graph_context(chunk_id)` | Entities + triples associated with a given `chunk_id` |

## Corpus-overlap check

The inspection asset (`inspection_asset_name`) reports
`overlap_with_knowledge_base`: the Jaccard overlap (`|intersection| /
|union|`) between the vector collection's chunk ids and the graph's chunk
ids -- **only** computed when `vector_store.provider == "chromadb"` and
`graph.provider` is `"networkx"` or `"sql_tables"` (the combinations where
listing every id in both stores is cheap). Every other provider combination reports
`overlap_with_knowledge_base: null` with an `overlap_note` explaining why,
rather than silently guessing. A low overlap after both pipelines have
run usually means they weren't pointed at the same chunk ids -- see the
shared chunk-id contract in `knowledge_graph_builder`'s README. The same
check also reports `chunk_count_vector` and `chunk_count_graph` (also
`null` for an unsupported provider combination) -- the raw per-side chunk
id counts the overlap fraction was computed from.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"hybrid_knowledge_search"` | Dagster resource key the in-process HybridKnowledgeSearchResource is registered under. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group name. |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners. |
| `asset_tags` | `Dict[str, Any]` | — | Extra asset tags, applied to the inspection asset. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `mcp_server_resource_key` | `str` | — | If set, ALSO registers a stdio MCP-server-shaped resource under this key. |
| `vector_store` | `Dict[str, Any]` | `dict()` | Same fields as knowledge_search_server's KnowledgeSearchResource (provider, connection_string, collection_name [required], api_key_env_var, embed_provider, embed_model, embed_api_key_env_var, text_metadata_key, vector_bu… _(full docs in schema.json + component README)_ |
| `graph` | `Dict[str, Any]` | `dict()` | Same fields as knowledge_graph_search_server's KnowledgeGraphSearchResource (provider, path, uri, username_env_var, password_env_var, database, host, port, graph_name). |
| `knowledge_base` | `str` | — | Optional UI-linking label. |
| `max_results` | `int` | `50` | Cap on rows returned by any list-returning method. |
| `receipts_db_path` | `str` | — | SQLite file path. When set, search() records a retrieval receipt per call with a vector component (mode='vector' or 'hybrid') and a new record_feedback(receipt_id, signal, user_id?) tool is exposed. Quarantined chunks ar… _(full docs in schema.json + component README)_ |
| `inspection_asset_name` | `str` | `"hybrid_knowledge_search_tools"` | Asset key for the inspection asset reporting this server's configured settings and the corpus-overlap check. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
# 1. context_engineering_pipeline chunks once and builds the vector index (per_step_ops
#    so the chunk step is its own asset; see knowledge_graph_builder's README).
type: dagster_community_components.ContextEngineeringPipelineComponent
attributes:
  asset_name: support_kb
  upstream_asset_key: raw_support_tickets
  id_column: ticket_id
  text_column: body
  per_step_ops: true
  steps:
    - {id: chunks, op: chunk, chunk_size: 500, chunk_overlap: 50}
    - {id: embedded, op: embed, provider: sentence_transformers}
    - {id: indexed, op: write_vector_store, provider: chromadb, collection_name: support_kb}
---
# 2. knowledge_graph_builder reuses the SAME chunks (support_kb_chunks) -- no re-chunking.
type: dagster_community_components.KnowledgeGraphBuilderComponent
attributes:
  asset_name: support_kg
  upstream_asset_key: support_kb_chunks
  id_column: ticket_id
  text_column: body
  steps:
    - {id: triples, op: extract, method: llm, model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY}
    - {id: resolved, op: resolve, min_mentions: 2}
    - {id: graph, op: write_graph, provider: networkx, path: /tmp/support_kg.json}
---
# 3. hybrid_knowledge_search_server searches both, joined by chunk_id.
type: dagster_community_components.HybridKnowledgeSearchServerComponent
attributes:
  resource_key: support_hybrid_search
  mcp_server_resource_key: support_hybrid_search_mcp
  knowledge_base: support_kb
  vector_store:
    provider: chromadb
    connection_string: ./chroma_db
    collection_name: support_kb
  graph:
    provider: networkx
    path: /tmp/support_kg.json
    graph_name: support_kg
```

Then an agent component can reference it:

```yaml
type: dagster_community_components.AnthropicAgentComponent
attributes:
  asset_name: support_agent
  mcp_servers:
    - server_resource_key: support_hybrid_search_mcp
```

### `graph.provider: sql_tables`

When `knowledge_graph_builder` ran with `execution_mode='sql'` instead, point
`graph` at its `<output_table>_nodes`/`_edges` tables directly:

```yaml
type: dagster_community_components.HybridKnowledgeSearchServerComponent
attributes:
  resource_key: support_hybrid_search
  vector_store:
    provider: chromadb
    connection_string: ./chroma_db
    collection_name: support_kb
  graph:
    provider: sql_tables
    sql_dialect: snowflake_cortex
    output_table: support_kg
    snowflake_account_env_var: SNOWFLAKE_ACCOUNT
    snowflake_user_env_var: SNOWFLAKE_USER
    snowflake_password_env_var: SNOWFLAKE_PASSWORD
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | none | Reads the vector store and graph store directly, not a Dagster asset input |
| Output | none | Registers a resource (and optional MCP server resource) plus a small inspection asset |

## Out of scope

Community summaries (Microsoft GraphRAG-style "global" queries over
entity-cluster summaries) and a gateway MCP that proxies other MCP
servers are explicitly not built here.
