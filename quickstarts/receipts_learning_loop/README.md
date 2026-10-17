# Receipts Learning Loop Quickstart

One runnable, offline example of the whole **context receipts** loop, built from the real components in this repo. It is also an
end-to-end regression test for that loop. No cloud account, no API key, no paid call.

```bash
pip install dagster dagster-duckdb duckdb chromadb litellm pandas pyyaml pytest
python3 quickstarts/receipts_learning_loop/run_demo.py          # narrated before/after
python3 -m pytest quickstarts/receipts_learning_loop/tests -q   # the same cycle, asserted
```

## What it demonstrates

| Step | What happens | Real component |
|---|---|---|
| 1. Build | 30 support docs in a **DuckDB** table (one outdated policy page, one duplicate) -> chunk -> dedupe -> embed -> **Chroma**. `receipts:` stamps `feedback_*` on every chunk, quarantines chunks people keep rating badly, and reports `rewrite_candidates` and dead weight. | `assets/ai/context_engineering_pipeline` (python mode, `source: warehouse_query`) |
| 2. Answer | 14 questions go through retrieve (top 3) -> generate. `receipts.rerank` reorders by learned per-chunk priors and a **receipt** (which chunks, from which recipe `code_version`) is stored per answer. | `assets/ai/rag_pipeline` + `resources/context_receipt_store` (SQLite) |
| 3. Outcomes | Thumbs up/down recorded on the receipts: some straight through `store.record_outcome`, the rest from `data/feedback.csv` through the file-source ingestion asset (the CSV is first joined to this run's receipt ids, as an app would carry them). Falls back to a plain `record_outcome` loop if the component is absent. | `ContextReceiptStore`, `assets/ai/receipt_outcome_ingestion` |
| 4. Rebuild | The build reads receipts + outcomes: the outdated page (retrieved 6 times, always rated down) is **removed from the index**, listed in `rewrite_candidates`, and every other chunk gets `feedback_*` metadata. | `context_engineering_pipeline` |
| 5. Re-ask | Same questions: the outdated page is gone from every answer; the right page is now first for the refund questions it used to lose to the outdated one. | `rag_pipeline` |
| 6. Shadow | A **candidate recipe** (`recipe-8`: embeds title+body, retires pages marked outdated) gets 50% of a fresh question set (seeded split). Outcomes are compared per `code_version` by the `shadow_vs_live` asset check. | `rag_pipeline` `receipts.shadow` |

`defs/` holds the component YAML (`${WORKDIR}` is substituted by `run_demo.py`); `data/` holds the corpus (`support_docs.csv`),
the questions with their gold document (`questions.csv`) and the feedback (`feedback.csv`).

## What is NOT real (read this)

- **Embeddings are a stand-in.** `fake_embeddings.py` replaces exactly the two seams the components use
  (`litellm.embedding` at build time, `rag_pipeline._embed_query` at question time; the same seams the committed component tests patch)
  with a deterministic **bag-of-words hashing embedding**. Texts sharing words land close together; there are no synonyms and no
  real semantics. Retrieval quality, the rankings and the shadow verdict below are properties of this stand-in, not of a real model.
- **No LLM is called.** The `generate` step's completion is stubbed; only retrieval and receipts are exercised.
- **The "people" are simulated.** A rule rates an answer thumbs-up only when the right chunk came first and the outdated page
  was not shown. `feedback.csv` is written to match that rule for the training questions.
- Credit assignment is chunk-level and blunt, so chunks served next to the bad page share some of the blame. In the run the right
  refund page is co-served in 6 thumbs-down answers and still survives (mean outcome about -0.2 against a -0.6 quarantine threshold), and it
  loses a slot in one unrelated answer. The quarantine and rewrite thresholds in `defs/support_kb.yaml` are tuned to this tiny corpus
  (`min_retrievals: 3`; production defaults are 5 / 3).

### Use a real embedding model

Delete the `with fake_embeddings.installed(...)` wrappers in `run_demo.py` and set a real model in `defs/support_kb.yaml`,
`defs/support_kb_candidate.yaml` and the retrievers' `embedding:` blocks. For the build that is the one line:

```yaml
    - {id: embedded, op: embed, provider: litellm, model: text-embedding-3-small}   # needs OPENAI_API_KEY
```

(and `retrievers.kb.embedding: {provider: openai, model: text-embedding-3-small}` for `rag_pipeline`). The receipts, rerank, quarantine
and shadow logic do not change.

## Mutation check

`DEMO_DISABLE_FEEDBACK=1` makes `run_cycle()` record no outcomes and rebuild without `receipts:`. The test suite then fails
(`test_the_loop_learned`, `test_outcomes_came_from_both_paths`, `test_shadow_verdict...`), and a permanent test
(`test_mutation_without_feedback_the_loop_does_not_learn`) asserts that this switched-off cycle still leaves the bad chunk indexed.

## Swap in BigQuery (not run on a real warehouse)

Everything above uses DuckDB for the corpus and SQLite for receipts. The same loop with BigQuery is configured, not rewritten:

- **Build in BigQuery** (`execution_mode: sql`, corpus never leaves the warehouse, receipts joined in SQL):
  see `assets/ai/context_engineering_pipeline/example_bigquery_sql_receipts.yaml`.
- **Receipts in BigQuery**: `backend: bigquery` with `bigquery_dataset: my-project.context_receipts` on the
  `ContextReceiptStoreComponent` (`defs/context_receipt_store.yaml`).

**These BigQuery paths were not run against a real warehouse by this example.** The repo's SQL-mode tests execute the generated SQL
on DuckDB stand-ins for the warehouse AI functions (`tests/sql_fake.py`, `tests/bq_fake.py` under the component), not on BigQuery.
Treat them as unverified until you try them on your project.
