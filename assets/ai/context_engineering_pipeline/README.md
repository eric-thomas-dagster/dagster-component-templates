# Context Engineering Pipeline

One YAML builds a governed, searchable, citeable knowledge base out of raw unstructured text: **chunk → classify → embed → write_vector_store**. Pairs with `rag_pipeline` for the query side — this component's `write_vector_store` output is exactly what `rag_pipeline`'s `retrieve`/`hybrid_search` ops expect to query against.

**Receipts.** This pipeline is the *build side* of the retrieval-receipt loop ([`resources/context_receipt_store`](../../../resources/context_receipt_store/README.md)):
it stamps every chunk with the provenance a gateway needs (`source_asset_key`, `source_version`, `chunk_id`, `content_hash`, ...), emits a content-derived data
version so a no-op rebuild doesn't make receipts look stale, and records the recipe as a Dagster `code_version`. See [Receipt-ready context](#receipt-ready-context).
It does not *write* receipts itself: a receipt records what was actually retrieved for a question, so it is written by whatever serves the queries
(`rag_pipeline` with its `receipts:` block, an MCP gateway, or your own retrieval code) via `ContextReceiptStore.record_retrieval(...)`. It does **read them back**:
with `receipts:` set, every rebuild learns from what was retrieved and how it turned out. See [Learning from receipts](#learning-from-receipts).

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

## Knobs (all configured in YAML)

Everything below is YAML `steps` and component settings; none of it needs Python code. "Python mode" is
`execution_mode: python` (the default), where the steps run in Python. `execution_mode: sql` instead runs chunk, classify
and embed inside the warehouse and supports only those three ops, so `filter`, `dedupe`, `sensitivity`, `stamp`, `enrich`,
parent-child chunking, `retrieval_eval` and the rest here need python mode.

**Every knob in one runnable pipeline:** [`example_all_knobs.yaml`](example_all_knobs.yaml). It is exercised end to end by
`tests/test_example_all_knobs.py`, so it stays correct.

**`chunk`**: `strategy` (`fixed` | `recursive` | `markdown`: markdown splits at headings and records a `section_path`
breadcrumb for citations | `code`: one chunk per top-level function or class, Python via `ast` so decorators and extents are exact,
other languages by a boundary-keyword heuristic rather than a parser; `language`, `language_column` or `path_column` pick
the language, small units are packed together, oversized ones split, and `section_path` names the symbol), `chunk_id_prefix`
(required when assets share a collection and their source ids can overlap), `chunk_size` / `chunk_overlap` with `length_unit: chars | tokens` (tokens are estimated as
`chars_per_token`, default 4), `min_chunk_chars`, `max_chunks_per_source`, `chunk_id_mode: positional | content`
(content ids are `source_<hash>`: an unchanged chunk keeps its id and a changed one gets a new id, so learned
retrieval priors do not carry over to different text), `header_template` (e.g. `"[{title}] "`, prepended to each chunk
and included in its hash). Every chunk gets a `content_hash`.

**`chunk` parent-child:** `parent_chunk_size` (and `parent_chunk_overlap`) embeds small child chunks but keeps each
child's larger parent passage as `parent_text` / `parent_id` (written to the index metadata), so retrieval matches
precisely and can return the surrounding passage. A child's identity includes its parent, so a change to the surrounding
passage changes the version.

**`enrich`**: an LLM-written field on every chunk: `mode: summary | keywords | questions | context` (a situating
sentence, the "contextual retrieval" technique; it is shown the chunk's `parent_text` when there is one), `model` (any
litellm model, required), `prepend: true` to put the text in front of the chunk so it is embedded and hashed,
`on_error: raise | skip`. One model call per chunk: it costs money and time.

**`filter`**: drop by `min_chars`, `max_chars`, `min_alnum_ratio`, `exclude_patterns` (regexes for boilerplate, footers,
signatures); reasons are counted in the step metadata.

**`dedupe`**: `mode: exact | normalized`, `within: [columns]` so one tenant's chunk never suppresses another's.

**`sensitivity`**: regex PII detection (email, phone, ssn, Luhn-checked credit_card, ipv4); `action: tag | redact | drop`.
`tag` adds `sensitivity` (`pii`/`none`) and `pii_types` columns that a gateway can withhold on (receipt exclusion reason
`pii`). Regex only, not NER: it misses names and free-form identifiers, so treat it as a floor.

**`stamp`**: governance columns that become filterable vector-store metadata: `columns: {scope: "{customer_id}"}`,
`ttl_days` (adds `valid_until` so a gateway can withhold expired chunks, receipt reason `stale`).

**`embed`**: `reuse_from: {connection_string, collection_name}` (chromadb) skips chunks whose `chunk_id`, `content_hash`
and embedding model already exist in the index, so only new or changed chunks pay for an embedding call. Every embedded
row records its `embedding_model`.

**`write_vector_store`**: `purge_expired: true` (chromadb) also deletes this asset's chunks whose `valid_until` has passed; `sync_mode: replace_sources` (default; deletes chunks of the source rows in this run that no
longer exist, so a shrunk document leaves no stale chunks) | `upsert` (never deletes) | `replace_all` (collection becomes
exactly this run; refused for partitioned assets). chromadb is exercised by tests. Qdrant and Pinecone are not tested
against live services; Pinecone cannot delete by filter on serverless indexes, so it behaves as `upsert` with a warning.
Qdrant point ids are derived from the chunk id; re-index collections written by earlier versions.

## Several kinds of context: tickets, docs, code

Each kind of source is its own asset with its own chunking, collection and policy; that is the supported shape. The
gateway discovers all of them with `context_policies(defs)` and every chunk names its asset, so receipts, health
checks and rebuilds stay per source.

```yaml
# three components, three assets, three independently versioned collections
- {asset_name: support_tickets, upstream_asset_key: raw_tickets, id_column: ticket_id, text_column: body,
   context_policy: {scope: customer_id, sensitivity: confidential},
   steps: [{id: c, op: chunk, strategy: recursive, chunk_id_prefix: "tickets:"}, ...]}
- {asset_name: product_docs, upstream_asset_key: raw_docs, id_column: page_id, text_column: markdown,
   context_policy: {sensitivity: public},
   steps: [{id: c, op: chunk, strategy: markdown, chunk_id_prefix: "docs:"}, ...]}
- {asset_name: product_code, upstream_asset_key: raw_code, id_column: path, text_column: source,
   context_policy: {sensitivity: internal, access_tags: [engineering]},
   steps: [{id: c, op: chunk, strategy: code, path_column: path, chunk_id_prefix: "code:"}, ...]}
```

Separate collections are the simplest. Assets may share one collection if each sets a distinct `chunk_id_prefix`; a chunk id that already belongs to a
different asset raises, and stale-chunk cleanup only touches the writing asset's own chunks. Getting the raw rows in (from a ticketing export, a docs site, a git checkout) is
the job of the upstream ingestion assets.

## Gating on retrieval quality

`retrieval_eval: {queries: [{query: "how do I export?", expected: [export_doc_0]}], k: 5, min_recall: 0.8, min_mrr: 0.5,
severity: ERROR, blocking: true}` embeds each golden query with the pipeline's own embed step, runs it against the index
this run just wrote, and reports **`retrieval_quality`** as an asset check: recall@k (a hit when any expected chunk id or
source id is in the top k) and MRR. `blocking: true` holds downstream assets until it passes. Together with the code
version this is the safety net for recipe changes: a new chunking or embedding model must still answer the golden
questions. Python mode, chromadb only.

## Declaring governance policy

`context_policy: {scope: customer_id, sensitivity: confidential, max_age_hours: 24, token_budget: 4000, access_tags: [support]}`
records the policy a retrieval gateway should enforce as `context/policy` asset metadata and `context/*` tags (same
declaration as the `@context_asset` decorator in `resources/context_receipt_store`; read back with
`context_policies(defs)`). Pair it with the `stamp` op (`columns: {customer_id: "{customer_id}"}`, `ttl_days`) and the
`sensitivity` op so each chunk carries the fields the policy checks.

## Receipt-ready context

Three component fields make this pipeline the build side of the retrieval-receipt loop
(`resources/context_receipt_store`):

- **`emit_data_version`** (default on) emits a content-derived data version: a hash of every chunk id + content hash
  plus the build recipe. Dagster's default data version derives from the asset's code version and its inputs' data
  versions, so it changes every run when an upstream asset has no explicit version (a typical raw table); without this a
  no-op rebuild would make every recorded receipt look stale.
- **`code_version`** (defaults to a fingerprint of the recipe: chunking, filters, embedding model, everything except where it
  is written) is set as the asset's Dagster `code_version` and stamped on every chunk as `source_code_version`. Changing
  the recipe marks the asset stale in Dagster (usable with `AutomationCondition.code_version_changed()`), and receipts can
  then be compared across recipe versions with `outcomes_by_code_version`. Set it explicitly to use something else, such
  as a git SHA.
- **`stamp_provenance`** (default on) writes `source_asset_key`, `source_version` (that same data version),
  `source_partition`, `source_id`, `chunk_id`, `content_hash` and `embedding_model` onto every chunk. A gateway builds a
  receipt item straight from a hit's metadata: `{item_id: chunk_id, source_asset_key, source_version, source_partition}`.

## Learning from receipts

Close the loop and every rebuild gets better. Serve queries with `rag_pipeline` (its `receipts:` block records what each query retrieved), record how the
answers turned out (`store.record_outcome(receipt_id, signal="thumbs_down")`, evidence, escalations...), and add `receipts:` here:

```yaml
attributes:
  asset_name: support_kb
  receipts:
    resource_key: context_receipts      # a registered ContextReceiptStoreComponent
    window_days: 30                     # only recent receipts count
    min_retrievals: 5                   # evidence needed before acting on a chunk
    min_outcomes: 3
    quarantine: {mean_outcome_below: -0.3, mode: exclude}     # flag | exclude
    regression_check: {min_outcomes: 10, max_drop: 0.15, severity: WARN}
```

Right before `write_vector_store` the build reads each chunk's history from the store and:

- **Stamps** `feedback_retrievals`, `feedback_rated`, `feedback_mean_outcome`, `feedback_cited` and `feedback_prior` onto the chunk (typed numbers, so a gateway can
  filter and rerank on them; every other metadata field is stored as a string). `refresh_priors` (default on) recomputes the store's item priors first.
- **Quarantines** chunks with at least `min_retrievals` retrievals and `min_outcomes` ratings whose mean outcome is at or below the threshold. `mode: flag` keeps them
  indexed with `feedback_quarantined: true`; `mode: exclude` deletes them from the index (by id, so it works even when a document's only chunk is quarantined, with any
  `sync_mode`; chromadb only). A chunk is not banned forever: an excluded chunk gets no new receipts, so once its old receipts age out of `window_days` it is
  judged afresh. Fix the source text and it comes back sooner.
- **Reports** `receipts_feedback` metadata: chunks with feedback, the quarantined ids, **dead weight** (`never_retrieved`, once the asset has at least
  `dead_weight_min_receipts` receipts), best and worst chunks, why chunks were withheld at query time (`exclusion_reasons`: stale / pii / policy ...), and
  `outcomes_by_code_version`.
- **Judges the recipe** (`regression_check`): the `recipe_outcomes` asset check compares the current recipe's real-world mean outcome with the best earlier recipe and
  warns (or fails) when it is worse by more than `max_drop` -- "chunk size 300 was rated better than 500; consider rolling back". With too few outcomes it passes and
  says `insufficient_evidence` or `no_baseline` rather than guessing.

Feedback is scoped to the current recipe (`code_version`) by default, so a recipe change starts fresh instead of applying old ratings to different chunks
(`scope_to_recipe: false` to opt out). Chroma merges metadata on upsert, so when a `feedback_*` key stops applying (stamping off, quarantine mode changed) the
build writes `None` to delete it instead of leaving a stale flag behind. Python mode reads a `ContextReceiptStore` resource; `execution_mode: sql` on BigQuery, Snowflake or Databricks computes the same feedback in the warehouse (see [Receipts in SQL mode](#receipts-in-sql-mode)). The end-to-end proof is `tests/test_closed_loop.py`: a real build,
real queries through `rag_pipeline` that record their own receipts, ratings, and a rebuild that stops serving the chunks users kept rating badly.

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

This generates and runs a single `CREATE OR REPLACE TABLE ... AS ...` statement, executed entirely by the warehouse, through whichever interface the resource exposes: `.get_engine()` (SQLAlchemy), `.get_connection()` (DB-API) or, for BigQuery, `.get_client()` (dagster-gcp `BigQueryResource`). `sql_dialect` is one of `snowflake_cortex`, `bigquery`, `databricks` and the statement is written in that warehouse's own SQL (BigQuery: `UNNEST(ARRAY(SELECT SUBSTR(..) FROM UNNEST(GENERATE_ARRAY(..)))) WITH OFFSET`, the `ML.GENERATE_EMBEDDING` table function, `AI.CLASSIFY`; Databricks: `LATERAL VIEW posexplode(transform(sequence(..)))`, `ai_query`, `ai_classify`; Snowflake: `LATERAL FLATTEN` over Cortex `SPLIT_TEXT_RECURSIVE_CHARACTER`, `EMBED_TEXT_768`, `CLASSIFY_TEXT`). BigQuery and Databricks have no recursive splitter, so they cut fixed-stride substrings (`chunk_size`, `chunk_overlap`) and drop empty chunks. Labels and model names are escaped and identifiers (`output_table`, `id_column`, `metadata_columns`) are validated. **`write_vector_store` has no SQL-mode equivalent** — there's no in-warehouse vector index behaving like chromadb/pinecone/qdrant across these dialects, so point `rag_pipeline`'s `retrieve` op straight at `output_table` instead (Snowflake Cortex Search / BigQuery vector search / Databricks Vector Search all query a table directly).

**Output columns.** `<id_column>`, `chunk_text`, `chunk_index`, your `metadata_columns`, `chunk_id` (`{chunk_id_prefix}{id}_{chunk_index}`, the same positional id python mode uses), `content_hash` (SHA-256 of the chunk), `category` (classify) and `embedding` (embed). With `stamp_provenance` (default on) the same statement also writes `source_asset_key`, `source_version`, `source_partition` (the partition key of the run) and `source_code_version`. `source_version` is a content version computed inside the statement (an MD5 over every chunk's hash in chunk-id order); the asset reads it back after the run and emits it as the Dagster data version (`emit_data_version`) and as `context_version` metadata, so a no-op rebuild does not look like new content.

### Receipts in SQL mode

`receipts:` works in SQL mode for `sql_dialect: bigquery`, `snowflake_cortex` and `databricks` (other dialects raise a clear error; use python mode with a receipt-store resource there). The receipts live in tables in that warehouse (DDL: `receipt_table_ddl(dataset, prefix, dialect)` in the block inlined in `component.py`, dialect `bigquery` / `snowflake` / `databricks`), and the feedback is computed by joins in the same statement that builds the table. `dataset` names where they live: `project.dataset` on BigQuery, `DATABASE.SCHEMA` on Snowflake, `catalog.schema` on Databricks. Full examples: `example_bigquery_sql_receipts.yaml`, `example_snowflake_sql_receipts.yaml`, `example_databricks_sql_receipts.yaml`.

```yaml
  execution_mode: sql
  sql_dialect: bigquery
  receipts:
    dataset: my-project.context_receipts   # holds context_receipts, context_receipt_items, context_receipt_events, context_item_scores
    table_prefix: ""                        # optional
    window_days: 30
    quarantine: {mean_outcome_below: -0.3, mode: flag}   # flag | exclude (exclude drops the rows from the table)
    regression_check: {min_outcomes: 10, max_drop: 0.15} # adds the recipe_outcomes check, also computed in the warehouse
```

`resource_key` is optional here and defaults to the source's warehouse resource (it must be that same resource). The `recipe_outcomes` check reads its numbers with one extra query through that same resource (`.get_engine()` -> `exec_driver_sql`, `.get_connection()` with `.execute` or cursor-style `cursor.execute` / `fetchall` as Snowflake connections are, or BigQuery's `.get_client()`). Per dialect the JSON payloads are read with `JSON_VALUE` (BigQuery), `PARSE_JSON(payload_json):value::FLOAT` (Snowflake) or `get_json_object(payload_json, '$.value')` (Databricks), and evidence scoped to `details.item_ids` is matched with `UNNEST` / `ARRAY_CONTAINS` / `array_contains(from_json(..., 'array<string>'), ...)`; on Snowflake the window cut-off is a `TIMESTAMP_TZ` literal so it does not depend on the session time zone. The semantics are those of `ContextReceiptStore.item_feedback` (retrievals = distinct receipts returning the chunk, rated = receipts with an outcome, mean outcome = mean of each receipt's effective outcome with a later event on the same `outcome_ref` superseding the earlier, cited = receipts with cited/verified/counterfactual_tested evidence that applies to the chunk), stamped as `feedback_retrievals`, `feedback_rated`, `feedback_mean_outcome`, `feedback_cited`, `feedback_prior` (read from `context_item_scores`) and, with `quarantine`, `feedback_quarantined`. Differences from python mode: `refresh_priors` does nothing (priors are read as written), there is no `receipts_feedback` report (counts of chunks with feedback and quarantined chunks are in the materialization metadata), and no write_vector_store step is needed.

**DuckDB has no native embedding/completion function** — same limitation dbt's own package has (their `jaffle-logistics` example leaves DuckDB as a placeholder for exactly this reason). Use `execution_mode: python` with a `source: {kind: warehouse_query, resource_key: duckdb_resource, ...}` for DuckDB instead — the corpus still never has to already be a materialized Dagster asset, it just runs the chunk/classify/embed chain in Python rather than natively in the database.

## Hand-writing this YAML

Every field name and step shape here is meant to be as readable for a human analyst hand-writing YAML as it is for an agent generating it — no positional args, no implicit ordering beyond `steps:` itself, `source:` on a step is optional and defaults to the step right above it.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output Dagster asset name |
| `id_column` | `Union[str, int]` | Column uniquely identifying each source row -- threaded through to every chunk as a citation back to the source. |
| `text_column` | `Union[str, int]` | Column containing the raw text to chunk. |
| `steps` | `List[Dict[str, Any]]` | Ordered list of {id, op, ...op-specific fields} dicts. Each step's `source:` names a prior step's id to read from; if omitted, defaults to the immediately preceding step (so you don't have to repeat `source:` on every li… _(full docs in schema.json + component README)_ |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `code_version` | `str` | — | Dagster code_version for the asset: the identity of the recipe that builds this context. Defaults to a fingerprint of the steps (chunking, filters, embedding model...), so changing the recipe marks the asset stale in Dag… _(full docs in schema.json + component README)_ |
| `group_name` | `str` | — | Dagster asset group name |
| `description` | `str` | — | Asset description shown in the Dagster catalog. |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags to apply to the asset. |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. Auto-inferred if unset. |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily'/'weekly'/'monthly'/'hourly'/'static'/'dynamic'/None. |
| `partition_start` | `str` | — | Partition start date (ISO), required for time-based types. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static partitioning. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `output_table` | `str` | — | Required when execution_mode='sql'. Fully-qualified destination table name the final chunk/category/embedding rows are written to, in the same database (CREATE OR REPLACE TABLE ... AS ...) -- the knowledge base never leaves the warehouse. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `Union[str, List[str]]` | — | Upstream asset key providing a DataFrame of raw text rows. Mutually exclusive with `source` -- set exactly one. |
| `source` | `Dict[str, Any]` | — | Pull raw text rows directly via SQL instead of from an upstream asset: {kind: warehouse_query, resource_key: <registered resource>, sql: <query>}. resource_key must point at a resource exposing .get_engine() (SQLAlchemy)… _(full docs in schema.json + component README)_ |
| `metadata_columns` | `List[str]` | — | Additional source columns to carry forward onto every chunk row (e.g. customer_id, created_at) -- available to later steps and written as vector-store metadata for filtered search. |
| `max_source_rows` | `int` | — | Safety cap on source rows processed per materialize (applied AFTER any SQL filtering, before chunking). A real corpus is typically far too large to process in one in-memory run -- pair this with partitioning (partition_t… _(full docs in schema.json + component README)_ |
| `execution_mode` | `str` | `"python"` | 'python' (default): pulls rows into a DataFrame and calls chunk/classify/embed as Python/API calls -- works with every provider, but data leaves the database and the corpus must fit in memory (bound it with max_source_ro… _(full docs in schema.json + component README)_ |
| `sql_dialect` | `str` | — | Required when execution_mode='sql'. One of: 'snowflake_cortex', 'bigquery', 'databricks'. Selects which warehouse-native AI SQL functions to emit. |
| `emit_data_version` | `bool` | `true` | Emit a content-derived Dagster data version (hash of every chunk id + content hash + the build recipe). Dagster's default data version derives from the asset's code version and its inputs' data versions, so it changes on… _(full docs in schema.json + component README)_ |
| `stamp_provenance` | `bool` | `true` | Write source_asset_key, source_version (the emitted data version), source_partition, source_id, chunk_id, content_hash and embedding_model onto every chunk in the vector store, so a retrieval gateway can build receipts d… _(full docs in schema.json + component README)_ |
| `receipts` | `Dict[str, Any]` | — | Learn from real use: read the retrieval receipts recorded by resources/context_receipt_store back into every build. {resource_key: <registered ContextReceiptStoreComponent>, window_days: 30, min_retrievals: 5, min_outcom… _(full docs in schema.json + component README)_ |
| `retrieval_eval` | `Dict[str, Any]` | — | A golden set that gates this knowledge base: {queries: [{query, expected: [chunk or source ids]}], k: 5, min_recall: 0.8, min_mrr: null, severity: ERROR\|WARN, blocking: false}. After the index is written, each query is… _(full docs in schema.json + component README)_ |
| `context_policy` | `Dict[str, Any]` | — | Declare the governance policy a retrieval gateway should enforce for this knowledge base: {scope: <chunk metadata field naming the tenant>, sensitivity: public\|internal\|confidential\|restricted, max_age_hours, token_bu… _(full docs in schema.json + component README)_ |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output data in metadata. |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when include_preview_metadata is True. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition. |

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

**Parsed and partly executed, never against a real warehouse (no credentials in this environment)**:
- `execution_mode: sql` — every dialect's statement is parsed with sqlglot in that dialect. The chunk, provenance and receipts-feedback statements of all three dialects are additionally executed (sqlglot -> DuckDB, `tests/bq_fake.py` and `tests/sql_fake.py`, which patches the few constructs sqlglot mistranslates) against real tables, and the feedback columns and `recipe_outcomes` numbers are asserted equal to `ContextReceiptStore.item_feedback` / `outcomes_by_code_version` on the same events. The warehouse AI functions (`ML.GENERATE_EMBEDDING`, `AI.CLASSIFY`, Cortex, `ai_query`/`ai_classify`, Cortex's text splitter) are parse-only, and DuckDB is not Snowflake or Databricks (JSON, NULL and type-coercion corners can differ). Flip to `validation.level: live` only once that's confirmed against a real Snowflake/BigQuery/Databricks account.
