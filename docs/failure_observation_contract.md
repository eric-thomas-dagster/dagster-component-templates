# Failure observation contract

**Why.** Dagster defines `ASSET_FAILED_TO_MATERIALIZE` / `AssetMaterializationFailure`, but open-source Dagster (verified on 1.13.24) never emits it: a failing
asset step writes `ASSET_MATERIALIZATION_PLANNED` and `STEP_FAILURE` only, the event log drops the event unless the storage sets `can_store_asset_failure_events`
(the base class returns `False`), and `report_runless_asset_event` rejects it. A failed asset is visible only as a failed *run*.

**Contract.** When an asset step of agentic_pipeline, context_engineering_pipeline, rag_pipeline, context_receipt_bigquery_sync or receipt_outcome_ingestion
raises, it first logs an `AssetObservation` (`context.log_event`, partition included) for each asset the step was producing, then re-raises the original
exception (bare `raise`; emission has its own try/except and can only warn). Metadata, exactly: `failure_status`=`failed`, `failure_reason` (`FAILED_TO_MATERIALIZE`
`POLICY_VIOLATION` `BUDGET_EXCEEDED` `APPROVAL_REJECTED` `UPSTREAM_ERROR` `UNKNOWN`), `error_class`, `error_message` (redacted, 500 chars), `will_retry`
(`true`/`false`, omitted if not knowable), `failed_step`, `run_id`, and an int `<prefix>_failures`=1 (`agentic_`, `context_pipeline_`, `rag_`, `receipts_sync_`,
`outcome_ingestion_`). Single-op builds mark every selected asset not yet yielded; per-step builds mark the failing step's asset (context_engineering) or every
selected asset (agentic: its terminal op emits them all). Downstream assets that never ran are not marked. A parked agentic run is not a failure.

**Read.** `dagster_instance_tools`: `asset_status.last_failure` (a real `ASSET_FAILED_TO_MATERIALIZE` event if the storage ever keeps one, else the newest failure
observation, else the failed run), `list_recent_failures[].asset_failures`, `stale_assets[].last_failure`. Insights: the `*_failures` keys are in the
`insights_custom_metrics_sync` presets. Code: the helper is inlined per component between `# BEGIN/END failure-observation contract`, drift-tested by
`tests/test_failure_observation_contract.py`.

## Upstream issue (ready to paste)

**Title:** Emit ASSET_FAILED_TO_MATERIALIZE when an asset step fails (OSS), let storage opt in, accept it in `report_runless_asset_event`

`DagsterEventType.ASSET_FAILED_TO_MATERIALIZE` and `AssetMaterializationFailure` exist, but in OSS (1.13.24) a failing asset step yields only
`ASSET_MATERIALIZATION_PLANNED` then `STEP_FAILURE`; no per-asset failure event is written, so "this asset/partition failed, why, will it retry" cannot be queried per asset,
only by joining planned events to failed runs. `EventLogStorage.can_store_asset_failure_events` returns `False` in the base class, so an event a user logs is dropped, and
`report_runless_asset_event` rejects `AssetMaterializationFailure`.
Repro: a two-asset `multi_asset` that raises; `instance.all_logs(run_id)` contains no `ASSET_FAILED_TO_MATERIALIZE`.
Proposal: (1) on step failure emit one `AssetMaterializationFailure` per asset key the step was producing (reason `FAILED_TO_MATERIALIZE`, `will_retry` from the retry policy,
error attached), and `UPSTREAM_FAILED_TO_MATERIALIZE` for assets of steps skipped because of it; (2) let the built-in event-log storages opt in to
`can_store_asset_failure_events`; (3) accept `AssetMaterializationFailure` in `report_runless_asset_event` so sensors/integrations can report failures of assets they observe.
Workaround today: log an `AssetObservation` with `failure_*` metadata before re-raising.
