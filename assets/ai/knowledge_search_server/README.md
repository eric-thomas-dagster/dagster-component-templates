# knowledge_search_server

Lets an agent search a knowledge base built by
[`context_engineering_pipeline`](../context_engineering_pipeline) (an `embed` +
`write_vector_store` step pair), via a `search_knowledge(query, top_k=5)` tool returning
each chunk's text, score, source, doc id, and chunk id as JSON.

| Tool | Does |
|---|---|
| `search_knowledge(query, top_k=5)` | Embeds `query` with the same embed provider/model the index was built with, queries the vector store for the nearest `top_k` chunks, and returns them. |

```json
{
  "chunks": [
    {"chunk_id": "doc-1_0", "text": "Our return policy is 30 days.", "score": 0.91, "source": "docs_kb", "doc_id": "doc-1"}
  ]
}
```

`source` is the chunk's `source_asset_key` metadata (which pipeline/asset built it);
`doc_id` is its `source_id` metadata (which upstream document/row the chunk came from --
distinct from `chunk_id` when one document produced multiple chunks). Both are stamped by
`write_vector_store`'s own `stamp_provenance` convention.

## This needs its own explicit settings -- it does not read another asset's config

`provider`/`connection_string`/`collection_name`/`embed_provider`/`embed_model` must match
whatever the `context_engineering_pipeline` instance that built the index actually used.
There is no way to read another component's step config from just an asset name across
component boundaries at build time (the same constraint already applies to
`dagster_workflow_tools`/`skill_resource`'s own `definitions_module`/source fields;
`context_engineering_pipeline`'s own `reuse_from` knob takes the same explicit shape
rather than resolving another asset's config by name). `knowledge_base` is an *optional
UI-linking label only* -- the literal value of the `dagster-community/knowledge-base-name`
tag `context_engineering_pipeline` already stamps on its own final step -- not something
this component reads or validates.

## Known limitation (inherited from `context_engineering_pipeline`, not introduced here)

For non-chromadb vector stores (pinecone/qdrant), a chunk's raw text is only retrievable
if the upstream `write_vector_store` step's `metadata_columns` explicitly included the
text column -- that step excludes it from metadata by default. chromadb always has the
text via its own `documents` field regardless of `metadata_columns`. If you're using
pinecone/qdrant and see `"text": null` in results, add the text column
(`chunk_text` by default) to `write_vector_store`'s `metadata_columns`.

## Wiring it into agentic_pipeline

Same shape as `dagster_instance_tools`/`dagster_workflow_tools`: `tool_use_loop` /
`mcp_call` / `agent_call` get tools **only from MCP servers**, so `component.py` is
itself a stdio MCP server. **Must be invoked with `python -m`, not a bare
`python component.py`**.

```yaml
- id: answer
  op: tool_use_loop
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  prompt_template: "Answer the question using search_knowledge: {{ question }}"
  mcp_servers:
    - name: knowledge_search
      type: stdio
      command: [python, -m, dagster_community_components.assets.ai.knowledge_search_server.component]
      env:
        KNOWLEDGE_SEARCH_COLLECTION_NAME: context_engineering_kb
        KNOWLEDGE_SEARCH_PROVIDER: chromadb
        KNOWLEDGE_SEARCH_CONNECTION_STRING: ./chroma_db
```

Server env vars: `KNOWLEDGE_SEARCH_COLLECTION_NAME` (required), `KNOWLEDGE_SEARCH_PROVIDER`
(default `chromadb`), `KNOWLEDGE_SEARCH_CONNECTION_STRING`,
`KNOWLEDGE_SEARCH_API_KEY_ENV_VAR`, `KNOWLEDGE_SEARCH_EMBED_PROVIDER` (default
`sentence_transformers`), `KNOWLEDGE_SEARCH_EMBED_MODEL`,
`KNOWLEDGE_SEARCH_EMBED_API_KEY_ENV_VAR`, `KNOWLEDGE_SEARCH_TEXT_METADATA_KEY` (default
`chunk_text`).

## Referencing it by key (`mcp_server_resource_key`)

The YAML above works, but it repeats the `KNOWLEDGE_SEARCH_*` env vars by hand in every
`mcp_servers:` entry. Set `mcp_server_resource_key` on the component instead, and it
registers a second, [`MCPServerResource`](../../../resources/mcp_server_resource)-shaped
resource under that key -- built from this component's own settings:

```yaml
type: dagster_community_components.KnowledgeSearchServerComponent
attributes:
  collection_name: context_engineering_kb
  mcp_server_resource_key: knowledge_search_mcp
```

Then, in any `mcp_servers`-aware component:

```yaml
mcp_servers:
  - server_resource_key: knowledge_search_mcp
```

## As a Dagster resource (in-process)

```yaml
type: dagster_community_components.KnowledgeSearchServerComponent
attributes:
  collection_name: context_engineering_kb
```
`context.resources.knowledge_search.search("what is your return policy?", top_k=5)`.

## Catalog visibility: the inspection asset

Resources have no tag mechanism of their own (confirmed via direct
`ResourceDefinition`/`ConfigurableResource` introspection), so this component always
emits one small inspection asset (`inspection_asset_name`, default
`knowledge_search_tools`) tagged `dagster-community/knowledge-search` (value:
`knowledge_base` if set, else `resource_key`), reporting the configured
provider/collection/embed settings as metadata when materialized -- the same
catalog-visibility role `openapi_tool_server`'s own `mode="mcp_server"` inspection asset
plays.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `collection_name` | `str` | Collection/index name the knowledge base was written to (write_vector_store's own collection_name). |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"knowledge_search"` | Dagster resource key the in-process KnowledgeSearchResource is registered under. |
| `connection_string` | `str` | — | chromadb: local path (default './chroma_db'). qdrant: server URL (default 'localhost'). Unused for pinecone. |
| `api_key_env_var` | `str` | — | Env var holding the vector store's API key (pinecone/qdrant). |

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
| `mcp_server_resource_key` | `str` | — | If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/env shape as MCPServerResource) under this key, built from this component's own settings -- python -m this module, with the KNOWLEDGE_SEAR… _(full docs in schema.json + component README)_ |
| `knowledge_base` | `str` | — | Optional UI-linking label only -- the asset name of the context_engineering_pipeline step that built this index (its dagster-community/knowledge-base-name tag value). This component does not read that asset's config from… _(full docs in schema.json + component README)_ |
| `provider` | `Literal['chromadb', 'pinecone', 'qdrant']` | `"chromadb"` | Vector store the knowledge base lives in. |
| `embed_provider` | `Literal['sentence_transformers', 'openai', 'cohere', 'litel…` | `"sentence_transformers"` | Must match the embed provider the knowledge base was built with (the embed step's own provider). |
| `embed_model` | `str` | — | Must match the embed model the knowledge base was built with (the embed step's own model). |
| `embed_api_key_env_var` | `str` | — | Env var holding the embed provider's API key (openai/cohere/litellm). |
| `text_metadata_key` | `str` | `"chunk_text"` | Metadata key holding chunk text for non-chromadb stores. Must match write_vector_store's own text_column if it was customized -- and that column must have been included in metadata_columns, since write_vector_store exclu… _(full docs in schema.json + component README)_ |
| `inspection_asset_name` | `str` | `"knowledge_search_tools"` | Asset key for the inspection asset reporting this server's configured settings. |

[//]: # (FIELDS:END)
