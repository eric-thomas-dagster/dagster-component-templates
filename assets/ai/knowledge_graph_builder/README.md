# KnowledgeGraphBuilderComponent

Chunk raw text, extract `(subject, relation, object)` triples, resolve entity
aliases, and write the result into a real graph store (a local networkx
file, Neo4j, or FalkorDB). Same "pipeline component" shape as
`context_engineering_pipeline`: one YAML, a `steps:` list, each step's
output is a DataFrame the next step reads.

## Use Cases

- Build a queryable entity/relationship graph out of support tickets,
  contracts, meeting notes, or any other unstructured text corpus.
- Pair with `knowledge_graph_search_server` to let an agent search the
  resulting graph (`search_entities`, `get_entity`, `get_neighbors`,
  `find_path`, `top_entities`).
- Incrementally rebuild as new documents arrive without re-paying an LLM
  call for chunks that haven't changed (`extract`'s `reuse_from`).
- Pair with `context_engineering_pipeline` and `hybrid_knowledge_search_server`
  for combined vector + graph retrieval over the same corpus, joined by
  `chunk_id` (see "Shared chunk-id contract" below).

## Shared chunk-id contract

`chunk`'s algorithm, default id scheme (`{chunk_id_prefix}{source_id}_{index}`),
and `content_hash` formula (sha256 of the chunk text) are copied verbatim
from `context_engineering_pipeline`'s own `chunk` op. Given the **same**
source text and the **same** `chunk_size`/`chunk_overlap`/
`preserve_sentences`/`chunk_id_prefix`, both pipelines produce
byte-identical `chunk_id` and `content_hash` values for the same chunk --
a vector knowledge base (`context_engineering_pipeline`) and this
knowledge graph can be joined by `chunk_id`, which is exactly what
`hybrid_knowledge_search_server` does.

To chunk the corpus only **once** and feed both pipelines from it:

1. Run `context_engineering_pipeline` with `per_step_ops: true` so its
   chunk step is its own queryable asset (`<asset_name>_<chunk step id>`).
2. Point this component's `upstream_asset_key` at that per-step chunk
   asset, and start `steps:` with `op: extract` instead of `op: chunk` --
   the first step may be either. When it's `extract`, upstream must
   already have a `chunk_id` column (a clear error names what's missing
   otherwise); `id_column` must still be present for source provenance.

```yaml
# context_engineering_pipeline: chunk once, also embed + index
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
# knowledge_graph_builder: reuses the SAME chunks (support_kb_chunks), no re-chunking
type: dagster_community_components.KnowledgeGraphBuilderComponent
attributes:
  asset_name: support_kg
  upstream_asset_key: support_kb_chunks
  id_column: ticket_id
  text_column: body   # unused when the first step is extract; id_column is still required
  steps:
    - {id: triples, op: extract, method: llm, model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY}
    - {id: resolved, op: resolve, min_mentions: 2}
    - {id: graph, op: write_graph, provider: networkx, path: /tmp/support_kg.json}
```

## Op coverage

- **`chunk`**: fixed-size chunking with sentence-boundary snapping and
  configurable overlap -- same output contract as
  `context_engineering_pipeline`'s own `chunk` op (`chunk_id`,
  `chunk_index`, `total_chunks`, `chunk_text`, `content_hash`, plus
  `id_column`/`metadata_columns` carried through).
- **`extract`**: `method: llm` (any litellm model; `entity_types`/
  `relation_types` act as a closed-vocabulary hint; `min_confidence` filters
  on the model's own reported confidence) or `method: spacy` (free, local,
  dependency-parse-based subject-verb-object extraction over named
  entities -- no API key, modest precision, reports no calibrated
  confidence so `min_confidence` is ignored). Each output row is ONE triple.
- **`resolve`**: always merges exact case/punctuation-normalized aliases
  within the same entity type; optionally also merges near-duplicate names
  above `embedding_similarity_threshold` (cosine similarity of name
  embeddings), optionally confirmed per candidate pair by an LLM
  (`llm_adjudication: true`). `min_mentions` drops entities (and every
  triple referencing them) seen fewer times than that across the run.
- **`write_graph`**: `provider: networkx` (a local JSON file), `neo4j`, or
  `falkordb`. `sync_mode`: `upsert` (default), `replace_sources` (also
  removes triples whose source rows are in this run but that this run no
  longer produces), or `replace_all` (the graph becomes exactly this run's
  triples; refused when partitioned).

Every edge carries real provenance: `source_ids`, `chunk_ids` +
`content_hashes` (parallel arrays, one entry per supporting chunk), and a
verbatim `evidences` snippet per chunk.

## Incremental extraction

`extract`'s `reuse_from: {provider: networkx, path: ...}` (same connection
shape as `write_graph`) looks up chunks whose `chunk_id` + `content_hash`
already have provenance recorded in that graph store and reuses their
previously-extracted triples instead of calling the LLM/spaCy again -- only
new or changed chunks are (re-)extracted.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output Dagster asset name |
| `id_column` | `Union[str, int]` | Column uniquely identifying each source row -- threaded through to every triple as provenance. |
| `text_column` | `Union[str, int]` | Column containing the raw text to chunk. |
| `steps` | `List[Dict[str, Any]]` | Ordered list of {id, op, ...op-specific fields} dicts. Each step's `source:` names a prior step's id to read from; defaults to the immediately preceding step. The first step must be op='chunk'. Valid ops: 'chunk', 'extra… _(full docs in schema.json + component README)_ |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group name |
| `description` | `str` | — | Asset description shown in the Dagster catalog. |
| `owners` | `List[str]` | — | Asset owners. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags to apply to the asset. |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. Defaults to ['ai', 'knowledge-graph'] if unset. |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | Backfill execution strategy for a partitioned asset. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily'/'weekly'/'monthly'/'hourly'/'cron'/'static'/'multi'/'dynamic'/None. |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition_type='cron' keys. |
| `partition_end_offset` | `int` | `0` | 0 (default) excludes the current, in-progress time window; 1 includes it. |
| `partition_timezone` | `str` | — | IANA timezone for time-based partitions (defaults to UTC). |
| `partition_start` | `str` | — | Partition start date (ISO). Required for time-based types. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static/multi partitioning. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec. Overrides the flat partition fields. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `Union[str, List[str]]` | — | Upstream asset key providing a DataFrame of raw text rows. Mutually exclusive with `source` -- set exactly one. |
| `source` | `Dict[str, Any]` | — | Pull raw text rows directly via SQL instead of from an upstream asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `upstream_asse… _(full docs in schema.json + component README)_ |
| `metadata_columns` | `List[str]` | — | Additional source columns carried forward onto every chunk row. |
| `max_source_rows` | `int` | — | Safety cap on source rows processed per materialize. Pair with partitioning to bound each run to a slice of a large corpus. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition. |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.KnowledgeGraphBuilderComponent
attributes:
  asset_name: support_kg
  upstream_asset_key: raw_support_documents
  id_column: doc_id
  text_column: body
  steps:
    - id: chunks
      op: chunk
      chunk_size: 1500
      chunk_overlap: 150
    - id: triples
      op: extract
      method: llm
      model: gpt-4o-mini
      api_key_env_var: OPENAI_API_KEY
      entity_types: [person, organization, product, issue]
    - id: resolved
      op: resolve
      min_mentions: 2
    - id: graph
      op: write_graph
      provider: networkx
      path: /tmp/support_kg.json
      sync_mode: replace_sources
  group_name: knowledge_graph
```

### Free/local extraction (spaCy, no API key)

```yaml
steps:
  - {id: chunks, op: chunk}
  - {id: triples, op: extract, method: spacy, spacy_model: en_core_web_sm}
  - {id: resolved, op: resolve}
  - {id: graph, op: write_graph, provider: networkx, path: /tmp/kg.json}
```

### Incremental rebuild

```yaml
steps:
  - {id: chunks, op: chunk}
  - id: triples
    op: extract
    method: llm
    model: gpt-4o-mini
    api_key_env_var: OPENAI_API_KEY
    reuse_from: {provider: networkx, path: /tmp/support_kg.json}
  - {id: resolved, op: resolve, min_mentions: 2}
  - {id: graph, op: write_graph, provider: networkx, path: /tmp/support_kg.json, sync_mode: replace_sources}
```

## Entity resolution details

`resolve`'s alias merge always runs: names are lowercased, stripped of
punctuation, and whitespace-collapsed before grouping, within the same
entity type (untyped entities form their own bucket). The embedding-based
merge is bounded by `max_entities_for_embedding_resolve` (default 500,
since it compares every pair of unique names within a type bucket) and
skipped with a warning above that, rather than silently running an
unbounded O(n²) comparison.

## Graph store notes

- **networkx**: persisted as node-link JSON (`nx.node_link_data`/
  `node_link_graph`), not GraphML -- GraphML's string-only attributes can't
  round-trip the list-valued provenance fields without lossy flattening.
- **neo4j** / **falkordb**: every node/edge is tagged with a `graph`
  property (the step's `graph_name`) so multiple knowledge graphs can share
  one database/instance without colliding. `sync_mode` is implemented as
  Cypher `MERGE`/`DELETE` statements scoped to that tag -- not exercised
  against a live server in this repo's own test suite (no account/service
  available here); reviewed directly against the `neo4j` Python driver and
  `falkordb-py`'s documented `select_graph`/`query` API.

## Run metadata

The final asset's materialization metadata reports: `node_count`,
`edge_count`, `chunk_count` (distinct chunk_ids written this run),
`entities_linked_to_chunks`, `top_entities` (top 20 by degree/mention_count),
`sample_triples` (first 10), `extraction_settings` (method/model/
entity_types/relation_types), and `graph_location` (the file path or
`provider:graph_name`).

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` (or SQL `source`) | Raw text rows: `id_column`, `text_column`, optional `metadata_columns` -- or a pre-chunked DataFrame (`chunk_id` + `id_column`) when the first step is `extract` |
| Output | `pd.DataFrame` | The final step's triples, for lineage/debugging -- the real graph lives in the configured store |
