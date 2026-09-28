# Context Engineering Pipeline

One YAML builds a governed, searchable, citeable knowledge base out of raw unstructured text: **chunk → classify → embed → write_vector_store**. Pairs with `rag_pipeline` for the query side — this component's `write_vector_store` output is exactly what `rag_pipeline`'s `retrieve`/`hybrid_search` ops expect to query against.

This is the "build the knowledge base" answer to what dbt Labs calls [context engineering](https://docs.getdbt.com/blog/dbt-context-engineering): chunk/embed/classify raw text, govern it, and make it citeable — the same discipline dbt applies to metrics, applied to unstructured text instead. Same "pipeline component" shape as `ml_pipeline`, `rag_pipeline`, `warehouse_pipeline`, `polars_pipeline`: one YAML, a `steps:` list in reading order, each step's output readable by the next via `source:` (or, if omitted, the immediately-prior step).

```yaml
type: dagster_component_templates.ContextEngineeringPipelineComponent
attributes:
  asset_name: support_kb
  upstream_asset_key: raw_support_tickets
  id_column: ticket_id
  text_column: body
  metadata_columns: [customer_id, created_at]
  steps:
    - id: chunks
      op: chunk
      chunk_size: 500
      chunk_overlap: 50
    - id: classified
      op: classify
      candidate_labels: [billing, technical, shipping, other]
    - id: embedded
      op: embed
      provider: sentence_transformers
      model: all-MiniLM-L6-v2
    - id: indexed
      op: write_vector_store
      provider: chromadb
      connection_string: /tmp/context_kb
      collection_name: support_tickets
```

Every op is free/local by default (sentence-transformers + HuggingFace zero-shot + ChromaDB) — no API key required to run this end to end, matching dbt's own stated philosophy that governance tooling should be cheap to validate, not another paid call per test.

## Ops

- **chunk** (must be first): fixed-size chunking with sentence-boundary snapping and configurable overlap. Threads `id_column` (and any `metadata_columns`) through to every chunk row as a citation back to the source row.
- **classify**: tags each chunk with a category **before** embedding — dbt's "relevance vs. similarity" fix: raw cosine similarity ranks short boilerplate over long relevant analysis unless chunks are pre-filtered by category first. Two modes: `mode: zero_shot` (default — HuggingFace zero-shot, local, free, no API key) or `mode: llm` (any litellm model judges the category — costs money per chunk, but applies real judgment a fixed label set can't for nuanced/ambiguous cases).
- **embed**: `provider: sentence_transformers` (default, local, free), `openai`, `cohere`, or `litellm` (universal gateway — one interface across every provider litellm supports: Azure, Bedrock, Vertex, Ollama, VoyageAI, Mistral, etc., via litellm's `"<provider>/<model>"` naming).
- **write_vector_store**: `provider: chromadb` (default, local, free), `pinecone`, or `qdrant`.

## Getting the raw text in: two ways

- **`upstream_asset_key`**: the usual Dagster way — point at any asset producing a DataFrame with `id_column`/`text_column`.
- **`source: {kind: warehouse_query, resource_key: ..., sql: ...}`**: pull rows directly via SQL, no upstream asset required. Works out of the box with `duckdb_resource` and any resource exposing `.get_engine()` (SQLAlchemy) or `.get_connection()` (DB-API) — `postgres_resource`, `snowflake_resource`, `bigquery_resource`, etc. This is what makes the pipeline "broadly usable against DuckDB and other databases" rather than requiring the raw text to already be a materialized Dagster asset.

Set exactly one — `upstream_asset_key` and `source` are mutually exclusive.

## Scale: the corpus won't fit in memory, and that's expected

A real corpus is almost never small enough to hold in one pandas DataFrame and process with one Python loop per chunk. Two levers, matching how every other pipeline component in this repo already handles this — not a bespoke rewrite:

1. **Partition + `max_source_rows`** (python mode): bound each run to a slice (e.g. `partition_type: daily` → one day's new tickets) instead of the whole historical corpus, and `max_source_rows` as a hard safety cap so a partition that's unexpectedly huge truncates loudly (a logged warning, `source_rows` vs. `source_rows_available` in output metadata) instead of OOM-ing silently.
2. **`execution_mode: sql`** (see below): the real at-scale, no-egress answer — the whole chunk/classify/embed chain runs as ONE query in the warehouse's own distributed engine. No in-memory limit, because the data never comes to Python at all.

## `execution_mode: sql` — data never leaves the database

dbt's own context-engineering package never sends data to Python either — chunk/embed/classify all run as warehouse-native SQL functions (Snowflake Cortex, BigQuery `ML.GENERATE_EMBEDDING`/`AI.CLASSIFY`, Databricks `ai_query`/`ai_classify`). Set `execution_mode: sql` to do the same thing here:

```yaml
type: dagster_component_templates.ContextEngineeringPipelineComponent
attributes:
  asset_name: support_kb_sql
  source:
    kind: warehouse_query
    resource_key: snowflake_resource
    sql: "SELECT ticket_id, body, customer_id FROM raw.support_tickets"
  id_column: ticket_id
  text_column: body
  execution_mode: sql
  sql_dialect: snowflake_cortex
  output_table: analytics.support_kb
  steps:
    - id: chunks
      op: chunk
      chunk_size: 500
      chunk_overlap: 50
    - id: classified
      op: classify
      candidate_labels: [billing, technical, shipping]
    - id: embedded
      op: embed
      model: snowflake-arctic-embed-m
```

This generates and runs a single `CREATE OR REPLACE TABLE ... AS ...` statement via the resource's own `.get_engine()`/`.get_connection()`, executed entirely by the warehouse. `sql_dialect` is one of `snowflake_cortex`, `bigquery`, `databricks` — whichever has a native embedding/completion function. **`write_vector_store` has no SQL-mode equivalent** — there's no in-warehouse vector index behaving like chromadb/pinecone/qdrant across these dialects, so point `rag_pipeline`'s `retrieve` op straight at `output_table` instead (Snowflake Cortex Search / BigQuery vector search / Databricks Vector Search all query a table directly).

**DuckDB has no native embedding/completion function** — same limitation dbt's own package has (their `jaffle-logistics` example leaves DuckDB as a placeholder for exactly this reason). Use `execution_mode: python` with a `source: {kind: warehouse_query, resource_key: duckdb_resource, ...}` for DuckDB instead — the corpus still never has to already be a materialized Dagster asset, it just runs the chunk/classify/embed chain in Python rather than natively in the database.

## Hand-writing this YAML

Every field name and step shape here is meant to be as readable for a human analyst hand-writing YAML as it is for an agent generating it — no positional args, no implicit ordering beyond `steps:` itself, `source:` on a step is optional and defaults to the step right above it.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output Dagster asset name |
| `id_column` | `str` | Column uniquely identifying each source row — threaded through to every chunk as a citation back to the source. |
| `text_column` | `str` | Column containing the raw text to chunk. |
| `steps` | `List[Dict]` | Ordered list of `{id, op, ...op-specific fields}` dicts. Each step's `source:` names a prior step's id to read from; if omitted, defaults to the immediately preceding step. Valid ops: `chunk` (must be first), `classify`, `embed`, `write_vector_store`. |

### Source (set exactly one of the two)

| Field | Type | Description |
|---|---|---|
| `upstream_asset_key` | `str` | Upstream asset key providing a DataFrame of raw text rows. |
| `source` | `Dict` | `{kind: warehouse_query, resource_key: ..., sql: ...}` — pull rows directly via SQL instead. |

### Scale / execution mode

| Field | Type | Default | Description |
|---|---|---|---|
| `max_source_rows` | `int` | — | Safety cap on source rows processed per materialize (python mode only). Pair with partitioning to process the full corpus over multiple runs. |
| `execution_mode` | `str` | `"python"` | `"python"` or `"sql"`. `"sql"` runs the whole chain server-side — no egress, no in-memory limit — but only for warehouses with a native AI function. |
| `sql_dialect` | `str` | — | Required when `execution_mode: sql`. One of `snowflake_cortex`, `bigquery`, `databricks`. |
| `output_table` | `str` | — | Required when `execution_mode: sql`. Destination table for the resulting knowledge base, in the same database. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `metadata_columns` | `List[str]` | — | Additional source columns carried onto every chunk row and written as vector-store metadata for filtered search. |
| `group_name` | `str` | — | Dagster asset group name |
| `description` | `str` | — | Asset description shown in the Dagster catalog. |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |
| `owners` | `List[str]` | — | Asset owners — team names or email addresses. |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset. |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. Auto-inferred if unset. |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output data in metadata. |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata. |
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries. |
| `retry_policy_backoff` | `str` | `"exponential"` | `linear` or `exponential`. |
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag before the asset is considered stale. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy. |
| `partition_type` | `str` | — | `daily`/`weekly`/`monthly`/`hourly`/`static`/`dynamic`/None. |
| `partition_start` | `str` | — | Partition start date (ISO), required for time-based types. |
| `partition_values` | `str` | — | Comma-separated values for static partitioning. |
| `dynamic_partition_name` | `str` | — | Name for `DynamicPartitionsDefinition`. |

[//]: # (FIELDS:END)

## Validation

`validation.level: code`.

**Live-verified (nothing mocked)**:
- Full python-mode pipeline (chunk → classify → embed → write_vector_store) against real sentence-transformers embeddings, real HuggingFace zero-shot classification, and a real ChromaDB write — a semantic search query against the resulting collection correctly retrieves the matching ticket with full citation metadata.
- `source: {kind: warehouse_query}` against a real DuckDB database (via `dagster-duckdb`) — including catching and fixing a real bug along the way: `DuckDBResource.get_connection()` is a `@contextmanager`; calling it without `with` raises `AttributeError` (confirmed live), so the ingestion helper now correctly enters it via `with`.
- `max_source_rows` truncation — both the truncated row count and the original available count are reported in output metadata.
- All validation guards (mutual exclusivity of `upstream_asset_key`/`source`, `execution_mode` values, `sql_dialect`/`output_table`/`source` requirements when `execution_mode: sql`, first-step-must-be-chunk).

**Mocked (no free/local backend exists for these)**:
- `embed` `provider: litellm` and `classify` `mode: llm` — litellm's own call is mocked; the request/response handling and category-validation logic run for real.

**Structural only, not executed (no live warehouse credentials in this environment)**:
- `execution_mode: sql` — the generated SQL string is asserted for all three dialects (`snowflake_cortex`, `bigquery`, `databricks`) and for the "classify without candidate_labels" error case, but never run against a real warehouse. Flip to `validation.level: live` only once that's confirmed against a real Snowflake/BigQuery/Databricks account.
