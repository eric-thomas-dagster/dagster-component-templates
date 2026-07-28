# rag_eval

Run a golden-set retrieval eval against a specific `vector_index_snapshot` partition and attach an **asset check that fails the run if quality regresses**.

- **Partitioned by `snapshot_id`** — each snapshot gets its own eval materialization. Browse the history to see precision@k over time.
- **Two-sided quality gate** — asset check fails if the score falls below `min_score_threshold` (absolute floor) OR drops more than `regression_pct_threshold` percentage points vs the immediately-prior materialization.
- **No LLM cost** — the eval is retrieval-only (does the top-k contain the expected terms?). Cheap enough to run on every snapshot.
- **Backfillable** — re-run the eval against every past snapshot partition to plot retrieval quality over time.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | string | — | Dagster asset name (required) |
| `snapshot_root_dir` | string | — | Must match the upstream `vector_index_snapshot.snapshot_root_dir` |
| `collection_name` | string | `rag_corpus` | Must match the upstream snapshot's `collection_name` |
| `golden_set` | list | — | List of `{query, expected_terms}` dicts |
| `k` | int | `3` | Top-k retrieval count per query |
| `min_score_threshold` | float | `0.5` | Absolute floor — asset check fails below |
| `regression_pct_threshold` | float | `10.0` | Max allowed drop vs prior materialization (percentage points) |
| `dynamic_partition_name` | string | `rag_snapshot` | Must match the upstream snapshot's `dynamic_partition_name` |
| `upstream_snapshot_asset_key` | string | — | Optional graph dep on the snapshot asset (execution loads from disk regardless) |

## Metric

For each golden query:
1. Retrieve top-k chunks from the snapshot's ChromaDB collection.
2. Concatenate the retrieved chunks' text (case-insensitive).
3. `score = fraction of expected_terms present in the combined text`.

Asset's `precision_at_k` = mean of per-query scores.

## Asset check

The `<asset_name>_retrieval_quality_check` reads the two most recent materializations across all partitions (independent of which partition triggered the check), then:

1. Fails if `current_score < min_score_threshold` (absolute floor).
2. Otherwise fails if `current_score < prior_score - (regression_pct_threshold / 100)`.
3. Passes on the first materialization (no prior to compare against).

This is what turns "we track a metric" into "**we block bad snapshots from advancing downstream**." Combine with `AutomationCondition` on any RAG-answer asset that consumes the snapshot to make quality-gated promotions automatic.

## What the check unlocks

- **Cross-materialization comparison for free.** The check reads the two most recent materializations of this asset from the instance — no bespoke metrics store, no external DB.
- **Downstream materialization is gated.** Asset checks with severity ERROR block runs that depend on this asset. Bad snapshots don't advance to answer generation.
- **Retrospective per-partition checking.** Re-run the check against any past snapshot partition — "was Monday's snapshot fine?" is one CLI invocation.
