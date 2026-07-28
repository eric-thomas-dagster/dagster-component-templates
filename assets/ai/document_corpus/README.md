# document_corpus

Materialize a directory of documents as a versioned Dagster asset. Every materialization is a snapshot of "the corpus as of time T" — doc count, per-doc content hashes, and a `corpus_hash` metadata field that downstream vector-index snapshots pin to.

**Why this exists.** RAG pipelines usually treat "the corpus" as an implicit input to the embed step. When docs change silently, retrieval quality drifts silently. Making the corpus itself a first-class asset means:

- **Corpus changes are lineage events** — downstream vector snapshots and RAG eval assets show the corpus version they were built from.
- **Freshness policies apply** — set `freshness_max_lag_minutes` and Dagster alerts when docs go stale.
- **Row-count-drop check** — `min_doc_count` guards against ingest-side regressions (broken file scanner, S3 permissions change, etc.).
- **Idempotent** — the `content_hash` per doc lets you dedup identical content across materializations.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | string | — | Dagster asset name (required) |
| `source_dir` | string | — | Absolute path to the directory of docs (required) |
| `file_glob` | string | `**/*.md` | Glob relative to source_dir |
| `encoding` | string | `utf-8` | Text file encoding |
| `min_doc_count` | int | `1` | Asset check fails if fewer docs than this |
| `freshness_max_lag_minutes` | int | — | Freshness policy — alerts if corpus stale |
| `group_name` / `description` / `owners` / `asset_tags` / `kinds` | — | — | Standard asset metadata |

## Output DataFrame

| Column | Description |
|---|---|
| `doc_id` | Path relative to `source_dir` |
| `content` | Full text of the document |
| `source_path` | Absolute filesystem path |
| `content_hash` | SHA-256 of the content bytes |
| `byte_size` | Content size in bytes |
| `ingested_at` | ISO-8601 UTC timestamp of the materialization |

## Output metadata

| Key | Type | Purpose |
|---|---|---|
| `doc_count` | int | Number of docs in this corpus version |
| `total_bytes` | int | Sum of `byte_size` |
| `corpus_hash` | text | SHA-256 of sorted per-doc hashes — the stable identifier for this exact set of doc contents |
| `source_dir` | path | The directory scanned |
| `file_glob` | text | The glob used |

Downstream components (see `vector_index_snapshot`, `rag_eval`) read `corpus_hash` from upstream metadata to establish provenance.
