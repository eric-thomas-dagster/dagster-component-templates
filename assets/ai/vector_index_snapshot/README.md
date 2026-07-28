# vector_index_snapshot

Chunk + embed an upstream corpus into a **versioned ChromaDB snapshot on disk**. Every materialization writes a new subdirectory (`{snapshot_root_dir}/{snapshot_id}/`), updates the `latest` symlink, AND registers `snapshot_id` as a new key on a shared dynamic-partitions definition.

Downstream RAG assets (`rag_eval`, RAG query components you write yourself) are partitioned by that same dynamic-partitions name — so each snapshot is addressable as a partition in the Dagster UI, and "roll back to Monday's index" is just materializing the downstream against Monday's partition. No rebuild, no restore-from-backup — a graph-native edge.

**Why this exists.** Most RAG stacks overwrite the vector index in place. If today's rebuild embedded broken PDFs or the chunker regressed, there's no "last-week's-good-index" to fall back to. Treating each snapshot as an immutable materialization + a partition key means:

- **Rollback = re-materialize downstream against an older snapshot partition** — no rebuild, no restore-from-backup, one click in the UI.
- **Backfill across snapshots** — re-run `rag_eval` across the last 30 days of snapshots against today's golden set to plot retrieval quality over time. Native partition backfill.
- **Compare embeddings across versions** — snapshot v42 vs v43 diff'able by opening both ChromaDB clients.
- **Provenance** — `corpus_hash` in output metadata proves which corpus version produced this index.

Default embedder is ChromaDB's built-in ONNX MiniLM (no API key needed). Swap to OpenAI via `embedder_provider: openai` + `api_key_env_var`.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | string | — | Dagster asset name (required) |
| `upstream_asset_key` | string | — | Upstream `document_corpus` asset key (supports slash-paths for cross-loc) |
| `snapshot_root_dir` | string | — | Directory that holds all snapshots (required) |
| `chunk_size` | int | `800` | Max chars per chunk |
| `chunk_overlap` | int | `100` | Overlap chars between adjacent chunks |
| `embedder_provider` | string | `chromadb_default` | `chromadb_default` (local, no key) or `openai` |
| `embedder_model` | string | provider default | Model name |
| `api_key_env_var` | string | — | Required when `embedder_provider: openai` |
| `collection_name` | string | `rag_corpus` | ChromaDB collection name inside each snapshot |
| `dynamic_partition_name` | string | `rag_snapshot` | Name of the dynamic-partitions def used to key each snapshot. Downstream assets partitioned by this name become addressable per snapshot. |

## Output

Returns a dict:

```python
{
  "snapshot_path": "/data/rag/snapshots/20260728T193045Z-a1b2c3d4",
  "snapshot_id": "20260728T193045Z-a1b2c3d4",
  "collection_name": "product_docs",
  "embedder": "chromadb_default:all-MiniLM-L6-v2",
  "chunk_count": 342,
  "dimension": 384,
  "corpus_hash": "a1b2c3d4e5f6..."
}
```

Same values surface as materialization metadata for the Dagster UI.

## Reading a snapshot downstream

```python
import chromadb
client = chromadb.PersistentClient(path="/data/rag/snapshots/latest")
collection = client.get_collection("product_docs")
results = collection.query(query_texts=["how do I ..."], n_results=5)
```

For rollback, point `path` at a specific past snapshot dir instead of `latest` — OR (the graph-native way) materialize your downstream `rag_eval` against the older snapshot partition.

## What you get that a plain "rebuild in place" workflow doesn't

- **Corpus-version → index-version lineage edge.** The materialization metadata records `corpus_hash` on every snapshot; every retrieval traces back to a specific corpus version.
- **Corpus-hash assertion.** `corpus_hash` on the upstream corpus and on the snapshot's metadata can be equality-checked in an asset check — one line, one gate.
- **Rollback via asset selection in the UI.** Each `snapshot_id` is a dynamic partition. "Roll back to Monday" is a partition selector, not a file-path change.
- **Backfill downstream across all past snapshots.** Re-run `rag_eval` against every snapshot of the last 30 days to plot retrieval quality over time — native partition backfill.
