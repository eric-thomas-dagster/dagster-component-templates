# Context Engineering Pipeline

One YAML builds a governed, searchable, citeable knowledge base out of raw unstructured text: **chunk → classify → embed → write_vector_store**. Pairs with `rag_pipeline` for the query side — this component's `write_vector_store` output is exactly what `rag_pipeline`'s `retrieve`/`hybrid_search` ops expect to query against.

**Receipts.** This pipeline is the *build side* of the retrieval-receipt loop ([`resources/context_receipt_store`](../../../resources/context_receipt_store/README.md)):
it stamps every chunk with the provenance a gateway needs (`source_asset_key`, `source_version`, `chunk_id`, `content_hash`, ...), emits a content-derived data
version so a no-op rebuild doesn't make receipts look stale, and records the recipe as a Dagster `code_version`. See [Receipt-ready context](#receipt-ready-context).
It does not *write* receipts itself: a receipt records what was actually retrieved for a question, so it is written by whatever serves the queries
(`rag_pipeline` with its `receipts:` block, an MCP gateway, or your own retrieval code) via `ContextReceiptStore.record_retrieval(...)`. It does **read them back**:
with `receipts:` set, every rebuild learns from what was retrieved and how it turned out. See [Learning from receipts](#learning-from-receipts).

This is the "build the knowledge base" half of context engineering: chunk/embed/classify raw text, govern it, and make it citeable — the same discipline-first treatment Dagster already applies to tables and metrics, applied to unstructured text instead. Same "pipeline component" shape as `ml_pipeline`, `rag_pipeline`, `warehouse_pipeline`, `polars_pipeline`: one YAML, a `steps:` list in reading order, each step's output readable by the next via `source:` (or, if omitted, the immediately-prior step).

```yaml
type: dagster_community_components.ContextEngineeringPipelineComponent
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

Every op is free/local by default (sentence-transformers + HuggingFace zero-shot + ChromaDB) — no API key required to run this end to end; governance tooling should be cheap to validate, not another paid call per test.

### Scaffold these demos end-to-end

Three runnable walkthroughs, plus one full in-repo project:

- [`examples/context_engineering.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/context_engineering.md) — the closed context-receipt loop (build → serve → real feedback → rebuild with the identical recipe → the chunks that kept failing are gone); serving is `rag_pipeline`, so the query side shows up as a Dagster asset too.
  ```bash
  curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_context_engineering_demo.sh | bash
  cd context-engineering-demo
  uv run dg dev
  ```
- [`examples/context_engineering_mcp.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/context_engineering_mcp.md) — the same closed loop, but Dagster does only the build; serving is a standalone FastMCP gateway + agent over real stdio MCP, no Dagster import in the serving path at all.
  ```bash
  curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_context_engineering_mcp_demo.sh | bash
  cd context-engineering-mcp-demo
  uv run dg dev
  ```
- [`examples/context_engineering_partitioned.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/context_engineering_partitioned.md) — the same closed receipt loop as the first demo, but `partition_type: daily` + `per_step_ops: true`: one build per day, each reading only that day's new rows; a rebuild only re-processes the one partition whose chunk picked up real bad feedback, leaving never-served partitions untouched.
  ```bash
  curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_context_engineering_partitioned_demo.sh | bash
  cd context-engineering-partitioned-demo
  uv run dg dev
  ```

All three free/local by default (sentence-transformers + HuggingFace zero-shot + ChromaDB, plus local SQLite or DuckDB); all live-validated end to end, exit 0.

**Fourth, a full-fidelity production use, not a scaffolded illustration:** [`examples/prospect_recon.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/prospect_recon.md). An agentic Drive/Notion/web research loop feeds this exact pipeline (chunk → redact → dedupe → classify → stamp → embed → index) to build a governed, receipted per-prospect knowledge base, which a forced-function-call step then reads to write a sales-engineering brief — the receipt is written through a FastMCP governance server at generation time, not an in-process call. It isn't free/local like the three walkthroughs above (real LLM calls, real Google/GitHub/Slack APIs, real cost per run — see its own doc for the credentials and cost note), and unlike every other demo in that directory the script doesn't auto-launch it: there's no safe synthetic scenario to run against, since it depends on your own real Calendar/Drive data and writes to a real downstream repo/inbox/Slack. It scaffolds the full project and stops before launch so you can fill in real credentials first:
```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_prospect_recon_demo.sh | bash
cd prospect-recon-demo
```

## Ops

- **chunk** (must be first): fixed-size chunking with sentence-boundary snapping and configurable overlap. Threads `id_column` (and any `metadata_columns`) through to every chunk row as a citation back to the source row.
- **classify**: tags each chunk with a category **before** embedding — a relevance-vs-similarity fix: raw cosine similarity ranks short boilerplate over long relevant analysis unless chunks are pre-filtered by category first. Three modes: `mode: zero_shot` (default — HuggingFace zero-shot, local, free, no API key), `mode: llm` (any litellm model judges the category — costs money per chunk, but applies real judgment a fixed label set can't for nuanced/ambiguous cases), or `mode: custom` (bring your own classifier — `fn: "module.path:function_name"`, called as `fn(text, candidate_labels) -> str` once per chunk; same `module.path:function_name` convention as `langgraph_agent`'s `graph_fn`). `mode: custom` is python mode only — a Python function can't run inside `execution_mode: sql`'s warehouse-native statement.
- **embed**: `provider: sentence_transformers` (default, local, free), `openai`, `cohere`, or `litellm` (universal gateway — one interface across every provider litellm supports: Azure, Bedrock, Vertex, Ollama, VoyageAI, Mistral, etc., via litellm's `"<provider>/<model>"` naming).
- **custom**: bring your own Python callable — `fn: "module.path:function_name"`, called once per chunk as `fn(text, **params) -> str` (same `module.path:function_name` convention as `classify`'s `mode: custom`). The generic escape hatch for anything `filter`/`dedupe`/`sensitivity`/`stamp`/`enrich` can't express — e.g. wiring in `log_redaction.pii_log_redaction:redact_text` for NER-based PII redaction beyond the regex-only `sensitivity` op. Python mode only.
- **write_vector_store**: `provider: chromadb` (default, local, free), `pinecone`, or `qdrant`.

## Knobs (all configured in YAML)

Everything below is YAML `steps` and component settings; none of it needs Python code. "Python mode" is
`execution_mode: python` (the default), where the steps run in Python. `execution_mode: sql` instead runs steps inside
the warehouse as one atomic statement: chunk, classify, embed and enrich always (each mapped onto a warehouse-native AI
function), plus `filter`/`dedupe`/`sensitivity`/`stamp` (plain SQL, with specific unsupported parts called out and
rejected per op below). `write_vector_store`, parent-child chunking and everything else on this page needs python mode
(`retrieval_eval` works in both, see below).

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
`on_error: raise | skip`. One model call per chunk: it costs money and time. **Also works in `execution_mode: sql`**
(`model` becomes a warehouse-native model reference instead — BigQuery `ML.GENERATE_TEXT`, Snowflake Cortex
`COMPLETE`, Databricks `ai_query`; same incremental reuse as classify/embed, invalidated by a model **or** mode
change) — except `prepend: true`, rejected with a clear error (it would need `content_hash` re-hashed
mid-statement, the same reason `sensitivity`'s `action: redact` is rejected), and `parent_text` (SQL mode never
has parent-child chunking, so `mode: context` always uses the plain passage, same as python mode's own fallback
when a chunk has no parent). `on_error` has no effect in SQL mode: the whole statement succeeds or fails together,
there is no per-row skip.

**`filter`**: drop by `min_chars`, `max_chars`, `min_alnum_ratio`, `exclude_patterns` (regexes for boilerplate, footers,
signatures); reasons are counted in the step metadata. **Also works in `execution_mode: sql`** (plain `LENGTH`/
`REGEXP_REPLACE`-based SQL, portable across all 3 dialects) — except `exclude_patterns` entries that need lookahead,
lookbehind or backreferences (e.g. `(?=...)`, `(?<=...)`, `\1`): BigQuery's regex engine (RE2) can't express those, so
`build_defs()` raises a clear error instead of silently mistranslating the pattern. Keep `exclude_patterns` simple, or
use `execution_mode: python`.

**`dedupe`**: `mode: exact | normalized`, `within: [columns]` so one tenant's chunk never suppresses another's. **Also
works in `execution_mode: sql`** (one more CTE with `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY chunk_id) =
1`, the same pattern the incremental-reuse logic already uses) — no caveats, fully portable.

**`sensitivity`**: regex PII detection (email, phone, ssn, Luhn-checked credit_card, ipv4); `action: tag | redact | drop`.
`tag` adds `sensitivity` (`pii`/`none`) and `pii_types` columns that a gateway can withhold on (receipt exclusion reason
`pii`). Regex only, not NER: it misses names and free-form identifiers, so treat it as a floor. **Partially works in
`execution_mode: sql`**: `detectors: [email, ssn, ipv4]` and `action: tag | drop` are plain, portable SQL. `phone`'s
pattern needs a negative lookahead/lookbehind RE2 can't express, and `credit_card`'s precision in python mode comes
from a Luhn check with no portable SQL equivalent built here (a regex-only match would have a much higher
false-positive rate) — both detectors, and `action: redact` (which would need `content_hash` recomputed mid-statement),
raise a clear error in SQL mode rather than shipping a silently weaker version. Use `execution_mode: python` for those.

**`stamp`**: governance columns that become filterable vector-store metadata: `columns: {scope: "{customer_id}"}`,
`ttl_days` (adds `valid_until` so a gateway can withhold expired chunks, receipt reason `stale`). **Also works in
`execution_mode: sql`** — `columns` templates must be a plain literal or simple `{column}` placeholders only (no
Python format specs like `{x:.2f}`); `ttl_days` is computed once at build time and embedded as a date literal, so no
per-dialect date-arithmetic function is needed.

**`custom`**: bring your own Python callable — `fn: "module.path:function_name"` (required), resolved and called
once per row as `fn(text: str, **params) -> str`; same colon-separated convention as `classify`'s `mode: custom`.
`text_column` resolves the same way as `filter`/`dedupe`/`sensitivity`/`stamp`. `output_column` defaults to
overwriting `text_column` in place (and rehashes `content_hash`, the same redact-then-rehash pattern as
`sensitivity`'s `action: redact`); set it to a new column name instead to write the transformed text alongside the
untouched original (no rehash — the original text and hash are untouched). `params` (dict) is passed as `**params`
to `fn`. `on_error: raise` (default) stops the run on a failing row; `skip` keeps that row's original text
unchanged and counts it as failed in the step metadata. Python mode only — a Python function can't run inside
`execution_mode: sql`'s warehouse-native statement. The motivating use case: wiring in
`log_redaction.pii_log_redaction:redact_text` for NER-based PII redaction (`enable_presidio=True`) beyond what the
regex-only `sensitivity` op's fixed detector set catches.

All four of `filter`/`dedupe`/`sensitivity`/`stamp` in SQL mode are inserted right after chunk-id/content-hash
assignment and before receipts feedback and classify/embed/enrich, regardless of where they're listed in `steps:` —
so dropped or deduped chunks never pay for a receipts-feedback join or a warehouse AI function call. `classify` /
`enrich` / `embed` themselves also run in that fixed order regardless of `steps:` order (on BigQuery specifically,
each is its own warehouse-native table function, and only the last one in that order can wrap the final `SELECT`;
the earlier ones materialize as their own CTE first). None of `filter`/`dedupe`/`sensitivity`/`stamp`/`enrich` is
yet supported together with `per_step_ops: true` in SQL mode (that path stays chunk/classify/embed-only, one table
per step).

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
questions. Python mode needs a chromadb index.

**In `execution_mode: sql`** (`bigquery`, `snowflake_cortex`, `databricks`) the same `retrieval_eval` block runs warehouse-side, with the same golden-set shape,
thresholds, `severity`, `blocking`, check name and metadata (`recall_at_k`, `mrr`, `k`, `queries`, `misses`) and the same definitions (they are computed by
one shared function). It needs only an `embed` step. After the table is built, the check runs one statement through the build's own warehouse resource
(`get_engine` / `get_connection` / `get_client`, like `recipe_outcomes`): it embeds every golden query with the build's own embedding function and model
(`ML.GENERATE_EMBEDDING`, `SNOWFLAKE.CORTEX.EMBED_TEXT_768`, `ai_query`), scores each against every chunk of `output_table` by cosine similarity, and returns
each query's top `k` chunks (ties broken by `chunk_id`); matching against `expected` (a chunk id or the `id_column` value) happens in Python.
- **Similarity.** BigQuery `1 - COSINE_DISTANCE(embedding, query)` (exact scan, no vector index needed); Snowflake `VECTOR_COSINE_SIMILARITY` (the table's
  `embedding` is the `VECTOR` Cortex returned); Databricks cosine from plain array functions (`zip_with` / `aggregate`), so `embedding` must be `ARRAY<FLOAT>` /
  `ARRAY<DOUBLE>` as `ai_query` returns for an embedding endpoint. All three are a brute-force scan over every candidate row for every query (queries x chunks
  comparisons): fine for golden sets of tens of queries over millions of chunks; there is no ANN index and no top-k pushdown.
- **Which rows are searched: the whole table.** The table is the index (python mode likewise searches the whole collection, every partition), so a partitioned run
  evaluates the table as it stands after that partition's rows were swapped in, not only the partition's own rows. Expect the first partition runs to miss
  golden questions whose answers live in partitions that are not built yet. Candidates are limited to rows whose `embedding_model` equals the current model (vectors
  of two models are not comparable) and, with `stamp_provenance`, to this asset's `source_asset_key`.
- **Incremental reuse / receipts.** The check reads the final table, so reused embeddings count like fresh ones and only the golden queries are embedded by the
  check (N calls, once per query; the engine could in principle inline the query CTE and repeat the call per use, so keep golden sets small). Quarantine `exclude` rows
  are not in the table and therefore not retrievable; `flag` rows (and `prune` flagged rows) stay candidates, exactly as python mode (which does not filter on
  `feedback_quarantined` either): the check scores what the index serves, and a gateway filtering on the flag is responsible for its own filter.
- **Limits.** The golden queries are embedded with the same model but the build embeds `chunk_text` whose boundaries come from the SQL chunker, so scores are not
  numerically comparable with a python-mode run over the same documents.

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
    rewrite_below: -0.3                 # chunks at or below this mean outcome are listed for a human rewrite (default -0.3)
    prune: {never_retrieved_after_receipts: 200, mode: flag}  # dead weight: flag | exclude (see below)
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
  `outcomes_by_code_version`. It also carries **`rewrite_candidates`**: chunks retrieved at least `min_retrievals` times and rated at least `min_outcomes` times whose
  mean outcome is at or below `rewrite_below` (default -0.3, independent of `quarantine`), worst first, at most 20, each with its `source_id` so you know which
  document to rewrite. A quarantined (even excluded) chunk stays on the list: it is exactly what needs rewriting. The definition and thresholds are those of
  `ContextReceiptStore.maintenance_report` (a test runs both on the same events and asserts equal results).
- **Prunes dead weight** (`prune`): chunks with zero retrievals in the window. `mode: flag` (default) indexes them with `feedback_dead_weight: true`; `mode: exclude`
  leaves them out of the index (chromadb: deleted by id, as for quarantine). Never retrieved is only evidence when there was traffic and the chunk has had time to be
  retrieved, so a chunk is judged only when **all** of these hold:
  1. the window holds at least `never_retrieved_after_receipts` receipts (default 200) *of the current recipe* when `scope_to_recipe` is on (receipts of an
     earlier recipe prove nothing about this one: without this a recipe change would look like "nothing was ever retrieved");
  2. it has zero retrievals in the window (same rule as `never_retrieved`);
  3. it is at least `min_age_days` old (default: `window_days`, so it has existed for the whole receipts window). Every chunk is stamped with
     `feedback_first_seen_at` (carried over from the previous build; an index or table from before `prune` was switched on has none, so its chunks count as new and
     are protected until they have aged `min_age_days`). Python mode reads the stamp back from the previous chromadb index; a read failure fails safe (everything counts as new).

  `exclude` removes a chunk from search until a later build re-adds it, and it does so automatically: the build never remembers an exclusion, so once the window no
  longer qualifies (traffic below the threshold, or the chunk got retrieved) the next build indexes it again. A re-added chunk is new again (its stamp left with it),
  so it is protected for `min_age_days` and then judged afresh: probation, not a ban. Python mode needs the chromadb provider (it reads the first-seen stamp and
  deletes by id). The summary reports `pruned`, `pruned_ids`, `prune_judged` (false = not enough traffic, nothing was pruned) and `prune_protected_new`.
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

## Per-step assets (`per_step_ops`)

By default every configured step — `chunk`, `classify`, `embed`, `write_vector_store`, ... — runs inside
one `@asset`'s compute function: a single node in the graph, one materialization per run, with each step's
timing/row-count folded into that one asset's `step_metadata` field. Set `per_step_ops: true` to flip that:
each step becomes its own `@dg.op` inside one op graph, so each one is its own visible asset node, with its
own materialization history, its own metadata, and (via Dagster's native re-execution-from-failure)
independently retryable — same shape as `AgenticPipelineComponent`'s `per_step_ops`. Supported in both
python mode and `execution_mode: sql` (see below for the SQL-mode shape).

```yaml
attributes:
  asset_name: support_kb
  per_step_ops: true
  steps:
    - {id: chunks, op: chunk, chunk_size: 500}
    - {id: classified, op: classify, candidate_labels: [billing, technical, shipping, other]}
    - {id: embedded, op: embed, provider: sentence_transformers, model: all-MiniLM-L6-v2}
    - {id: indexed, op: write_vector_store, provider: chromadb, collection_name: support_kb}
```

Publishes four assets instead of one, with real step-to-step lineage between them:
`support_kb_chunks → support_kb_classified → support_kb_embedded → support_kb`. The **last** step always
keeps the plain `asset_name` ( `support_kb`, unsuffixed) — so any existing `upstream_asset_key: support_kb`
pointing at this component's output elsewhere keeps working unchanged — and every other step publishes as
`<asset_name>_<step id>`. The cross-cutting metadata (`n_steps`, `step_chain`, `context_version`,
`receipts_feedback`, the retrieval-quality/recipe-outcomes asset checks) all still land on that one final
asset, same as today; every step additionally gets its own `op` / `elapsed_seconds` / row-count metadata on
its own node.

### `per_step_ops` with `execution_mode: sql`

Each participating step (`chunk` always, `classify`/`embed` if configured) becomes its own real warehouse
table — `<output_table>__<step id>` for every step but the last, plain `output_table` for the last — instead
of folding chunk → stamp → classify → embed into one combined `CREATE OR REPLACE TABLE` statement. Same
asset-naming and lineage convention as python mode above. Incremental reuse is preserved per step: each
stage probes **its own** table's existing contents (not the single shared `output_table`'s), so changing
only the classify labels re-only-classifies without re-embedding, and vice versa — the same guarantee
`execution_mode: sql` already gives in its single-statement form, now per table.

```yaml
attributes:
  asset_name: support_kb
  execution_mode: sql
  per_step_ops: true
  sql_dialect: bigquery
  output_table: analytics.support_kb
  source: {kind: warehouse_query, resource_key: bq, sql: "SELECT ticket_id AS id, body FROM analytics.raw_tickets"}
  steps:
    - {id: chunks, op: chunk, chunk_size: 500}
    - {id: classified, op: classify, candidate_labels: [billing, technical, shipping, other]}
    - {id: embedded, op: embed, model: "myproject.ds.text_embedding_model"}
```

Publishes `support_kb_chunks` (table `analytics.support_kb__chunks`), `support_kb_classified` (table
`analytics.support_kb__classified`), and `support_kb` (table `analytics.support_kb`, the embed step). No
`write_vector_store` equivalent in SQL mode, per_step_ops or not — see "`execution_mode: sql` — data never
leaves the database" below for why 3 steps is the complete scope, not a gap. Not yet supported together
with partitioning: per-table partition staging is a tracked follow-up.

Not supported together with `freshness_max_lag_minutes` (either execution mode): Dagster's
`graph_multi_asset`/`AssetsDefinition.from_graph` machinery has no per-output freshness-policy plumbing
today, confirmed directly against the installed `dagster` package — both together raise at `build_defs()`
time rather than silently doing nothing.

## Scale: the corpus won't fit in memory, and that's expected

A real corpus is almost never small enough to hold in one pandas DataFrame and process with one Python loop per chunk. Two levers, matching how every other pipeline component in this repo already handles this — not a bespoke rewrite:

1. **Partition + `max_source_rows`** (python mode): bound each run to a slice (e.g. `partition_type: daily` → one day's new tickets) instead of the whole historical corpus, and `max_source_rows` as a hard safety cap so a partition that's unexpectedly huge truncates loudly (a logged warning, `source_rows` vs. `source_rows_available` in output metadata) instead of OOM-ing silently.
2. **`execution_mode: sql`** (see below): the real at-scale, no-egress answer — the whole chunk/classify/embed chain runs as ONE query in the warehouse's own distributed engine. No in-memory limit, because the data never comes to Python at all.

## `execution_mode: sql` — data never leaves the database

Never send the corpus to Python at all — chunk/embed/classify all run as warehouse-native SQL functions (Snowflake Cortex, BigQuery `ML.GENERATE_EMBEDDING`/`AI.CLASSIFY`, Databricks `ai_query`/`ai_classify`). Set `execution_mode: sql`:

```yaml
type: dagster_community_components.ContextEngineeringPipelineComponent
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

**Warehouse limits checked at build time.** `classify` needs 2-100 unique `candidate_labels` on Snowflake (`CLASSIFY_TEXT`) and 2-20 labels of 1-50 characters each on Databricks (`ai_classify` v1); BigQuery's `AI.CLASSIFY` documents no limit. On Snowflake the stored `category` is the `label` field of the object `CLASSIFY_TEXT` returns (`...:label::STRING`). BigQuery table names are `[project.]dataset.table` (a dash is allowed only in the project id) and are always emitted as one backticked path. Reading the existing table's columns uses `` `project`.dataset.INFORMATION_SCHEMA.COLUMNS ``: a missing table gives zero rows (built in full), but a missing *dataset* raises BigQuery's NotFound -- create the dataset first.

**Legacy Snowflake functions.** Snowflake marks `EMBED_TEXT_768` and `CLASSIFY_TEXT` legacy (deprecation announced for the end of 2026) with `AI_EMBED` / `AI_CLASSIFY` as the replacements. This component still emits the legacy functions; migrating to the new ones is future work.

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

`resource_key` is optional here and defaults to the source's warehouse resource (it must be that same resource). The `recipe_outcomes` check reads its numbers with one extra query through that same resource (`.get_engine()` -> `exec_driver_sql`, `.get_connection()` with `.execute` or cursor-style `cursor.execute` / `fetchall` as Snowflake connections are, or BigQuery's `.get_client()`). Per dialect the JSON payloads are read with `JSON_VALUE` (BigQuery), `PARSE_JSON(payload_json):value::FLOAT` (Snowflake) or `get_json_object(payload_json, '$.value')` (Databricks), and evidence scoped to `details.item_ids` is matched with `UNNEST` / `ARRAY_CONTAINS` / `array_contains(from_json(..., 'array<string>'), ...)`; on Snowflake the window cut-off is a `TIMESTAMP_TZ` literal so it does not depend on the session time zone. The semantics are those of `ContextReceiptStore.item_feedback` (retrievals = distinct receipts returning the chunk, rated = receipts with an outcome, mean outcome = mean of each receipt's effective outcome with a later event on the same `outcome_ref` superseding the earlier, cited = receipts with cited/verified/counterfactual_tested evidence that applies to the chunk), stamped as `feedback_retrievals`, `feedback_rated`, `feedback_mean_outcome`, `feedback_cited`, `feedback_prior` (read from `context_item_scores`) and, with `quarantine`, `feedback_quarantined`. Differences from python mode: `refresh_priors` does nothing (priors are read as written), `receipts_feedback` carries `rewrite_candidates` and (with `prune`) `pruned` / `prune_judged` / `prune_protected_new` but not the python-mode best/worst lists (counts of chunks with feedback and quarantined chunks are also separate metadata keys; `receipts_pruned` counts pruned chunks), and no write_vector_store step is needed. The report is read with extra queries over the same CTEs **before** the table is replaced, so chunks the statement is about to drop (quarantine / prune `exclude`) are still reported, and `prune` reads each chunk's earlier `feedback_first_seen_at` from the existing `output_table` (a first run, or a table without the column, treats every chunk as new). `prune` adds `feedback_first_seen_at` (and `feedback_dead_weight` in flag mode) columns, so on a partitioned table switching it on needs the same one-time drop/alter as any other new column.

### Incremental builds in SQL mode

`incremental: true` (the default) stops every run from re-embedding and re-classifying the whole corpus. The statement is still ONE atomic `CREATE OR REPLACE TABLE ... AS`, but it reads the existing `output_table` first: chunk, stamp, then per feature (embed / classify) *prior values* (one row per `content_hash`) and a *todo* set (distinct chunk texts with no reusable prior value). Only the todo set is passed to `ML.GENERATE_EMBEDDING` / Cortex `EMBED_TEXT_768` / `ai_query` / `AI.CLASSIFY` / `CLASSIFY_TEXT` / `ai_classify`; prior and new values are joined back onto every chunk. It is a split CTE plus join on every dialect (no `CASE WHEN`), so a reused row never reaches the function even on engines that evaluate both branches. Identical chunk text is embedded once. Receipts feedback, quarantine and provenance stamps apply after reuse, exactly as before.

- Reuse key: `content_hash` AND the stored `embedding_model` (new column, the model id) for embeddings; `content_hash` AND the stored `category_signature` (new column, a hash of `candidate_labels`, order-sensitive) for categories. Change the model or the labels and that feature is recomputed for every chunk; the other feature is still reused.
- First run (no table), or an existing table without the needed columns (built by an earlier version of this component, or before the step existed): that feature builds in full. Existence and columns are read from `INFORMATION_SCHEMA.COLUMNS`.
- Materialization metadata: `reused_embeddings`, `new_embeddings`, `reused_categories`, `new_categories` (counted from the same CTEs just before the table is replaced), and a warning when a non-empty prior table yielded zero reuse.
- `incremental: false` restores the always-recompute behaviour. The output table gains `embedding_model` and `category_signature` columns either way.

### Partitions and backfills in SQL mode

A partitioned SQL-mode asset no longer replaces the whole table on every partition run. `source_partition` is always written on a partitioned run (even with `stamp_provenance: false`). The first partition run does `CREATE TABLE ... AS`; later runs build the partition into a staging table (`<output_table>__stage_<hash>`, the AI calls happen here, so a failure leaves the table untouched) and then swap only that partition's rows: BigQuery in one `BEGIN TRANSACTION` script, Snowflake as `BEGIN` / `DELETE` / `INSERT` / `COMMIT`, Databricks as one atomic Delta `INSERT INTO ... REPLACE WHERE`. The staging table is dropped afterwards. Incremental reuse reads the whole table, so a chunk already embedded under any partition is reused. `context_version`, the data version and `dagster/row_count` describe that partition's rows. An existing table without `source_partition`, or with a column set the recipe no longer matches, is refused with an explanation instead of being overwritten.

`source.sql` (SQL mode and python mode) can use `{partition_key}`, `{partition_start}` / `{partition_end}` (ISO strings, time-window partitions, also on a multi-axis asset with a time axis) and `{partition_key.<dim>}` (multi-axis). Each is replaced by a complete escaped string literal, so write `WHERE d = {partition_key}`, not `'{partition_key}'`; `{{` and `}}` are literal braces. An unknown placeholder raises, a placeholder on an unpartitioned asset raises at build time, and a partitioned asset whose source uses none logs a warning that every partition reads the full source.

```yaml
  partition_dimensions:
    - {name: date, type: daily, start: "2026-01-01"}
    - {name: region, type: static, values: [us, eu]}
  backfill_policy: multi_run
  backfill_policy_max_partitions_per_run: 5
  source:
    kind: warehouse_query
    resource_key: bigquery
    sql: "SELECT id, body FROM raw.docs WHERE region = {partition_key.region} AND created >= {partition_start} AND created < {partition_end}"
```

`backfill_policy: single_run` in SQL mode is allowed only when `source.sql` uses both `{partition_start}` and `{partition_end}` and the asset is not multi-axis (otherwise it raises at build time). Such a run stamps rows `source_partition = 'first..last'` and replaces that label and any single-key partition inside the range; re-running a *different overlapping* range can leave duplicates, so keep backfill ranges consistent or use `multi_run`. The asset has no IO-manager value (`dagster_type=Nothing`), which is what lets a single_run backfill run at all.

**DuckDB has no native embedding/completion function.** Use `execution_mode: python` with a `source: {kind: warehouse_query, resource_key: duckdb_resource, ...}` for DuckDB instead — the corpus still never has to already be a materialized Dagster asset, it just runs the chunk/classify/embed chain in Python rather than natively in the database.

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
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | Backfill execution strategy for a partitioned asset. 'multi_run' launches one run per partition (optionally capped via backfill_policy_max_partitions_per_run); 'single_run' materializes a whole backfill range in one run… _(full docs in schema.json + component README)_ |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily'/'weekly'/'monthly'/'hourly'/'static'/'dynamic'/None. |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on Dagster's TimeWindowPartitionsDefinition(cron_schedule=...). |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule. |
| `partition_start` | `str` | — | Partition start date (ISO), required for time-based types. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static partitioning. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts (type: daily/weekly/monthly/hourly/static/dynamic). One entry is a single axis; several make a MultiPartitionsDefinition. Mutua… _(full docs in schema.json + component README)_ |

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
| `incremental` | `bool` | `true` | execution_mode='sql' only (ignored in python mode, which has embed `reuse_from`). Re-use the embeddings and categories already in `output_table` instead of paying the warehouse AI function for the whole corpus on every r… _(full docs in schema.json + component README)_ |
| `emit_data_version` | `bool` | `true` | Emit a content-derived Dagster data version (hash of every chunk id + content hash + the build recipe). Dagster's default data version derives from the asset's code version and its inputs' data versions, so it changes on… _(full docs in schema.json + component README)_ |
| `stamp_provenance` | `bool` | `true` | Write source_asset_key, source_version (the emitted data version), source_partition, source_id, chunk_id, content_hash and embedding_model onto every chunk in the vector store, so a retrieval gateway can build receipts d… _(full docs in schema.json + component README)_ |
| `per_step_ops` | `bool` | `false` | When True, emit each configured step as its own @dg.op inside one op graph instead of folding the whole chain into a single @asset -- one visible, individually re-executable-on-failure asset node per step (chunk/classify… _(full docs in schema.json + component README)_ |
| `receipts` | `Dict[str, Any]` | — | Learn from real use: read the retrieval receipts recorded by resources/context_receipt_store back into every build. {resource_key: <registered ContextReceiptStoreComponent>, window_days: 30, min_retrievals: 5, min_outcom… _(full docs in schema.json + component README)_ |
| `retrieval_eval` | `Dict[str, Any]` | — | A golden set that gates this knowledge base: {queries: [{query, expected: [chunk or source ids]}], k: 5, min_recall: 0.8, min_mrr: null, severity: ERROR\|WARN, blocking: false}. After the index is written, each query is… _(full docs in schema.json + component README)_ |
| `context_policy` | `Dict[str, Any]` | — | Declare the governance policy a retrieval gateway should enforce for this knowledge base: {scope: <chunk metadata field naming the tenant>, sensitivity: public\|internal\|confidential\|restricted, max_age_hours, token_bu… _(full docs in schema.json + component README)_ |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output data in metadata. |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when include_preview_metadata is True. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition. |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run). |

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
- `execution_mode: sql` — every dialect's statement is parsed with sqlglot in that dialect. The chunk, provenance and receipts-feedback statements of all three dialects are additionally executed (sqlglot -> DuckDB, `tests/bq_fake.py` and `tests/sql_fake.py`, which patches the few constructs sqlglot mistranslates) against real tables, and the feedback columns and `recipe_outcomes` numbers are asserted equal to `ContextReceiptStore.item_feedback` / `outcomes_by_code_version` on the same events. The warehouse AI functions (`ML.GENERATE_EMBEDDING`, `AI.CLASSIFY`, Cortex, `ai_query`/`ai_classify`, Cortex's text splitter) are stood in for by call-counting DuckDB UDFs (`tests/ai_stubs.py`) in the incremental and partition tests (so reuse is asserted as exact call counts) and are never run for real, and DuckDB is not Snowflake or Databricks (JSON, NULL and type-coercion corners can differ). Flip to `validation.level: live` only once that's confirmed against a real Snowflake/BigQuery/Databricks account.
- `execution_mode: sql` + `per_step_ops: true` — same DuckDB-backed execution path (`tests/test_sql_per_step_ops.py`): real `dg.materialize()` builds three real, separate physical tables, asserts the per-step asset lineage (`AssetsDefinition.asset_deps`) and that incremental reuse is scoped per table, not shared across them. Also caught (and fixed, both execution modes) a real Dagster `AssetsDefinition.from_graph` gap along the way: a check declared via `check_specs` needs a literally-named graph output (`AssetCheckSpec.get_python_identifier()`), not just a bare `yield AssetCheckResult(...)` — `retrieval_quality`/`recipe_outcomes` would otherwise crash `build_defs()` the first time they were combined with `per_step_ops` in either mode.
- `per_step_ops: true` (python mode) + `source: {kind: warehouse_query}` + partitioning, all three together — real against DuckDB (`test_per_step_ops_with_source_warehouse_query_and_no_upstream_asset_key`, `test_per_step_ops_partitioned_source_reads_only_its_own_partition`). Caught and fixed a real bug: the first step's op always declared an `upstream: pd.DataFrame = None` parameter, even with `source:` (no `upstream_asset_key`) wired to it — `_pipeline_fn` calls it with zero args, but Dagster still gives that declared-but-unconnected parameter a real input slot (default value `None`) and type-checks it against the `pd.DataFrame` annotation, raising `DagsterTypeCheckDidNotPass`. Every existing committed `per_step_ops` test only ever used `upstream_asset_key`, so this combination had never actually been exercised. Fixed by omitting the `upstream` parameter entirely from the first step's op when there's no `upstream_asset_key` to connect to it.
- `execution_mode: sql` + `filter`/`dedupe`/`sensitivity`/`stamp` — real against DuckDB, all 3 dialects (`tests/test_sql_filter_dedupe_sensitivity_stamp.py`): min/max_chars, min_alnum_ratio, portable `exclude_patterns`, both dedupe modes, `within`-scoped dedupe, sensitivity `tag`/`drop` for the email/ssn/ipv4 detectors, stamp's `columns` templating and `ttl_days`, and all four combined in one build alongside classify/embed. Also real, executed rejections (not just structural): `exclude_patterns` using lookahead/lookbehind/backreferences, the `phone`/`credit_card` sensitivity detectors, `action: redact`, a stamp template with a Python format spec, and `per_step_ops: true` combined with any of these four (that path stays chunk/classify/embed-only). Fixing the hardcoded `FROM stamped` references these new steps needed to interpose before also surfaced that the receipts-feedback join previously assumed "stamped" was always literally the frontmost CTE -- now driven off a `cur` pointer like the rest of the statement.
- `execution_mode: sql` + `enrich` — real against DuckDB for Snowflake (`SNOWFLAKE.CORTEX.COMPLETE`) and Databricks (`ai_query`, the SAME function Databricks' real `ai_query` already serves embed with -- `tests/test_sql_enrich.py` tells the two calls apart by argument shape, the same way a real model-agnostic endpoint dispatch would): incremental reuse invalidated by a model **or** mode change (not just model, unlike embed), custom `output_column`, combined with classify/embed in one build, and `prepend: true` rejected with a clear error. **BigQuery's `ML.GENERATE_TEXT`/`AI.GENERATE_TEXT` is checked structurally only** (string assertions on the generated SQL, checked by hand against the real BigQuery docs) -- confirmed directly that sqlglot 28.0.0 has no parser support for either spelling's `MODEL`/`TABLE`/`STRUCT` table-function syntax (`sqlglot.parse_one` raises `ParseError` on both), so there is no fake client to execute it against, unlike every other BigQuery path in this component.

Opt-in Insights metrics: add insights_custom_metrics_sync with presets: auto (this component tags its asset(s) `insights/metrics`).
