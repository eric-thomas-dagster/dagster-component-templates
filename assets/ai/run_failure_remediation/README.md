# run_failure_remediation

Self-healing runs with a safety gate. When a Dagster run fails, this component gathers redacted evidence, asks your **triage
pipeline** (typically an [`agentic_pipeline`](../agentic_pipeline/README.md)) what to do, puts the proposal in front of a person
(or an explicit auto-approve policy), and only then executes the approved, allow-listed actions -- and records whether they fixed it.

```
failed run ──► failure sensor ──► evidence/<run_id>.json  (secrets redacted)
                    │              + dynamic partition <run_id>
                    └─ RunRequest ─► TRIAGE job (partition = run_id) ──► proposals/<run_id>.json
                                                                              │
              approval sensor ◄───────────────────────────────────────────────┘
                    │  pending record: approvals/pending/<run_id>.json   (agent_review_queue lists it)
                    │  a token bound to that exact proposal's fingerprint:  approvals/<run_id>.json
                    └─ approved ─► allow-listed actions ──► results/<run_id>.json
                                   (retry_run, rerun_partition, open_ticket ...)
                                   └─ after N minutes: outcome fixed | not_fixed ──► fed into the NEXT triage's evidence
```

## Safe by default

- **Nothing executes without a token** whose `fingerprint` matches the proposal as written. Re-writing the proposal after approval
  makes the old token stale (the pending record says why). Tokens are single-use (`consumed_at`).
- **Allow-list**: `allowed_actions` defaults to `retry_run`, `rerun_partition`, `open_ticket`. `materialize_assets` and `notify` are
  opt-in. Anything else, or any action whose params fail validation, is rejected and the reason is recorded.
- `auto_approve` is **off**. When you set `{kinds: [retry_run], max_per_hour: 3, min_confidence: 0.8}` a proposal whose every action is
  in `kinds` and whose confidence is high enough is approved by the policy (token `approver: auto_approve`), at most `max_per_hour`.
- Sensors start `STOPPED` (`default_status`). A `retry_run` chain is capped (`max_retries_per_run`, default 2), a failing retry does not
  start another triage (`triage_remediation_runs: false`), and the triage job's own failures are never triaged.
- Evidence, pending records, results, tickets and webhook payloads are redacted: `password=...`, tokens, `Authorization`/`Bearer`
  headers, URL credentials, `sk-`/`ghp_`/`xox*`/AWS/JWT-shaped tokens, private keys, long base64 blobs, the value of any
  secret-looking environment variable, plus your `redact_patterns`. Webhook URLs come from an env var and are never logged or stored.

## Wire it up

```yaml
type: dagster_community_components.RunFailureRemediationComponent
attributes:
  evidence_dir: ./evidence
  proposals_dir: ./proposals
  approval_dir: ./approvals
  results_dir: ./remediation_results
  triage_asset_selection: "group:triage"       # the dynamically partitioned assets of your triage pipeline
  dynamic_partitions_name: run_failures        # must equal the pipeline's dynamic_partition_name
  remediation_jobs: {core: "group:core", daily: "group:daily"}
```

Then deploy [`examples/triage_pipeline.yaml`](examples/triage_pipeline.yaml) (an `agentic_pipeline` that reads
`evidence/{partition_key}.json` and writes `proposals/{partition_key}.json`). Any pipeline works if it reads the evidence and writes a
proposal:

```json
{"run_id": "<run id>", "summary": "...", "root_cause": "...", "confidence": 0.85,
 "actions": [{"kind": "retry_run", "params": {}, "reason": "transient network error"}]}
```

The flat shape above, agentic_pipeline's `json_sinks` step dump (`{"extracted": {...}}`), and a JSON string in `text` (optionally in a
code fence) are all accepted. `run_id` defaults to the file name and must equal it.

Approve like any `agentic_pipeline` approval (it is the same store): `agentic_pipeline.approve_pending(approval_dir, run_id)`, or copy the
pending record's `approve_by_writing.json` to `<approval_dir>/<run_id>.json` with your name. `agent_review_queue` lists the pending
records.

## Actions

| kind | params | what happens |
|---|---|---|
| `retry_run` | `{}` (only this proposal's run) | requests a run of the assets that were planned but not materialized, same partition, same run config, tagged with `dagster/parent_run_id` / `dagster/root_run_id` |
| `rerun_partition` | `{partition_key?, asset_keys?}` | requests a run of the failed run's assets (or the listed ones) for the failed run's partition (or the given one) |
| `materialize_assets` | `{asset_keys, partition_key?}` | requests a run of the listed assets; restrict with `allowed_materialize_assets` |
| `open_ticket` | `{title, body?, severity?}` | writes `<tickets_dir>/<run_id>-<n>.json`; also POSTs it to `ticket_webhook_url_env_var` if set |
| `notify` | `{message}` | POSTs to `webhook_url_env_var` |

`retry_run` / `rerun_partition` / `materialize_assets` launch runs as **Dagster sensor run requests** through `remediation_jobs`
(`{name: asset selection}`; each becomes job `<name_prefix>_remediate_<name>`). A sensor can only request runs for the jobs it targets and
one job needs a single partitions definition, so group assets accordingly. Without `remediation_jobs` those three kinds are rejected
(with the reason recorded) and only `open_ticket` / `notify` work.

## Outcomes

Each executed action carries a `dagster_remediation/proposal` tag. `outcome_check_after_minutes` later (default 30) the approval sensor looks
the runs up: all succeeded -> `outcome: fixed`; any failed, or never started -> `not_fixed` (written to `results/<run_id>.json`
with `new_run_ids`). The next failure of the same job or error signature gets `recent_outcomes` in its evidence, so the triage model can
see that "retry" already failed to fix this error. If `store_resource_key` names a `ContextReceiptStore` resource and the proposal
carries a `receipt_id`, the outcome is also recorded with `record_outcome` (duck-typed, skipped when absent).

## Evidence

`run_id, job_name, status, partition, failure_reason, tags, failed_step_keys, errors[{step_key, error_class (root cause),
wrapper_error_class, message_tail, stack_tail}], run_failure_message, failed_assets / materialized_assets [{asset_key, partition}],
error_signature, upstream[{asset_key, downstream_failed_asset, last_materialization{timestamp, run_id, partition, data_version}}],
recent_runs, recent_outcomes, remediation_contract`.

## Rate limiting

`max_triage_per_hour` (rolling), `cooldown_minutes` per (job, error signature) so a flapping job does not spawn a triage per failure,
`only_jobs` / `ignore_jobs` / `only_tags`. The state is append-only: one small file per event under `<evidence_dir>/_state/ratelimit/<epoch_ms>-<run_id>-<kind>.json` (exclusive-create, so two sensors ticking at the same instant, or two daemon threads, never lose an entry; a count is a listing, and expired events are deleted on each claim). A claim is serialized in-process and, across processes, written-then-verified by rank (a loser withdraws its files); the only theoretical gap is a few-millisecond clock-read-to-create window between processes. Replaying a tick (`run_key = run_id`) never
double-launches and does not consume budget.

## Airflow / Airlift and other external runs

Verified against Dagster 1.13.24 with `dagster-airlift` 0.29.26's code and an OSS sqlite instance (tests/test_external_runs.py):

- **dagster-airlift's failed Airflow runs DO trigger the default `detect: events` sensor.** Its monitoring job calls
  `run_storage.add_historical_run(...)` with status `NOT_STARTED`, then `instance.report_dagster_event(PIPELINE_START)` and, when the
  Airflow DAG run fails, `PIPELINE_FAILURE`. That event moves the run to FAILURE and `run_failure_sensor` fires, even when the events are
  persisted hours after the run (the sensor's cursor is the event-log id, not a timestamp). The failure event has `error=None`.
- **A run written to run storage directly with status `FAILURE` and no event does not** -- no event log entry exists and
  `run_failure_sensor` never fires. `detect: runs` (or `both`) adds `<prefix>_failed_runs_sensor`, which polls run *records*
  (`get_run_records(statuses=[FAILURE], updated_after=...)`), for integrations that record failures that way.
- Evidence for such runs is built from the run record (job, status, tags, start/end time) and any events that exist:
  `evidence.source` is `events` or `run_record`, `evidence.limited: true` when there are no step errors, and `evidence.external_source`
  names the orchestrator (`dagster/external_job_source` tag, or any `dagster-airlift/*` tag).
- **`retry_run` and `rerun_partition` are rejected** for these runs (no Dagster job to re-execute; the reason is recorded in the result).
  `open_ticket` and `notify` work.
- Scope to Airflow with `only_tags: {dagster-airlift/airflow-dag-id: "*"}` (the tags airlift puts on its runs are
  `dagster-airlift/airflow-dag-id`, `.../airflow-dag-run-id`, `.../airflow-dag-run-url`, `.../observation-run`, plus
  `dagster/external_job_source: airflow`). `only_tags` applies to every mode.
- The record sensor does **not** use an `updated_timestamp` high-water cursor: a historical run's update time is its last event time,
  which can be hours older than runs already seen, and a row can turn FAILURE after rows with a higher id (both reproduced in the
  tests). It scans FAILURE runs updated within `runs_lookback_hours` (default 24) and judges each once, oldest `(update_timestamp,
  run_id)` first, `runs_max_per_tick` at a time, using a per-run marker `<evidence_dir>/_state/seen/<run_id>.json` that both sensors share
  -- so `both` never triages a run twice (also `run_key = run_id`). Failures that exist when the sensor is first armed are marked seen, not
  triaged. A rate-limited run is left undecided and retried on a later tick (the event sensor drops it, as before).
- Not verified: Dagster+ run storage (the `add_historical_run` server mutation fix in dagster-io/internal#27424 was not exercised) and a
  live Airflow instance.

## Limits (verified by the tests, not assumed)

- One run-failure-sensor tick can return `RunRequest`s for another job **and** `AddDynamicPartitionsRequest`s together; the requested
  partition is validated against the pending add.
- The sensor sees which assets and partitions failed from the run's `ASSET_MATERIALIZATION_PLANNED` vs `ASSET_MATERIALIZATION` events (not
  from `run.asset_selection`, which is empty for a plain asset job). Op-level jobs have no planned assets: `retry_run` cannot retry them
  (rejected with that reason); evidence still works.
- Re-execution runs from the **same code location** as the sensor: a sensor run request can only target that location's jobs, and
  `instance.create_reexecuted_run` needs a daemon-side workspace (`request_context`, `code_location`, `remote_job`) that sensor code does
  not have. Failures from other code locations are triaged (the sensor monitors all locations) but their retries are rejected.
- `retry_run` is a fresh run of the failed + not-materialized assets, tagged with parent/root run ids; it is not Dagster's "retry from
  failure" step-level re-execution.
- Local `file` sources / `json_sinks` in `agentic_pipeline` are local paths; `evidence_dir` and `proposals_dir` may be fsspec URIs only if
  your triage pipeline reads/writes them another way.
- Not verified here: a real daemon launching the requests (tests launch them with `dg.materialize` carrying the requested tags), a real
  code-location reload (tests only show the cursor survives rebuilding the definitions), a live LLM (the pipeline test fakes the model call),
  and webhooks other than against a local HTTP server.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `evidence_dir` | `str` | Where evidence JSON is written (<dir>/<run_id>.json): a local path or fsspec URI (s3://, gs://, abfs://). The triage pipeline reads it (agentic_pipeline: `source: {kind: file, path: <dir>/{partition_key}.json}`, local paths only). |
| `proposals_dir` | `str` | Where the triage pipeline writes proposals (<dir>/<run_id>.json): {run_id, summary, root_cause, confidence, actions:[{kind, params, reason}]}. |
| `approval_dir` | `str` | Approval directory (same layout as agentic_pipeline's `approve` op): pending records in <dir>/pending/<run_id>.json, tokens at <dir>/<run_id>.json. agent_review_queue can list it. |
| `results_dir` | `str` | Where execution results + outcomes are recorded (<dir>/<run_id>.json). |
| `triage_asset_selection` | `str` | Asset selection string for the triage job (e.g. 'key:run_triage*' or 'group:triage'): the dynamically partitioned assets of your triage pipeline. |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `minimum_interval_seconds` | `int` | `30` | Sensor tick interval. |
| `default_status` | `str` | `"STOPPED"` | Initial sensor status: STOPPED (default) or RUNNING. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `name_prefix` | `str` | `"run_remediation"` | Prefix for the sensor names (<prefix>_failure_sensor, <prefix>_approval_sensor). |
| `tickets_dir` | `str` | — | Where `open_ticket` writes ticket JSON. Default: <results_dir>/tickets. |
| `triage_job_name` | `str` | `"run_failure_triage_job"` | Name of the TRIAGE job this component defines over `triage_asset_selection` (a job request per failed run, partition = the failed run id). |
| `triage_run_config` | `Dict[str, Any]` | — | Run config passed to every triage run request. |
| `dynamic_partitions_name` | `str` | `"run_failures"` | Name of the DynamicPartitionsDefinition the failed run ids are registered on. The triage assets must use this same name (agentic_pipeline: partition_type: dynamic, dynamic_partition_name: <this>). |
| `only_jobs` | `List[str]` | — | Only triage failures of these jobs (default: all jobs in all code locations). |
| `ignore_jobs` | `List[str]` | — | Never triage failures of these jobs. The triage job itself is always ignored (no recursion). |
| `detect` | `str` | `"events"` | How failed runs are detected. `events` (default): Dagster's run_failure_sensor, which listens to RUN_FAILURE events. `runs`: a regular sensor polling run RECORDS with status FAILURE -- also sees runs written straight to… _(full docs in schema.json + component README)_ |
| `only_tags` | `Dict[str, str]` | — | Only triage failed runs carrying ALL of these tags: {tag_key: value}; value '*' (or '') matches any value. E.g. {'dagster-airlift/airflow-dag-id': '*'} or {'dagster/external_job_source': 'airflow'}. Applies to both detection modes. |
| `runs_lookback_hours` | `float` | `24.0` | detect runs\|both: how far back (by the run's last update time) the run-record sensor looks for failed runs it has not judged yet. A failure persisted later than this after it happened is not seen. |
| `runs_max_per_tick` | `int` | `10` | detect runs\|both: at most this many new failed runs are triaged per tick (oldest first); the rest wait for the next tick. |
| `triage_remediation_runs` | `bool` | `false` | Also triage failures of runs this component launched. Off by default so a failing retry cannot start another triage. |
| `max_triage_per_hour` | `int` | `10` | Rate limit: at most this many triage runs requested per rolling hour. |
| `cooldown_minutes` | `float` | `60.0` | After a triage for (job, error signature), further failures with the same signature are suppressed for this long. |
| `history_runs` | `int` | `10` | How many recent runs of the same job to include in the evidence. |
| `recent_outcomes` | `int` | `5` | How many recent remediation outcomes (same job or error signature) to include in the evidence. |
| `log_tail_chars` | `int` | `2000` | Max characters of each step's error message kept (the tail). |
| `redact_patterns` | `List[str]` | — | Extra regexes to redact from evidence, results, tickets and webhook payloads (on top of the built-in token/key/password/Authorization/base64/env-secret rules). |
| `allowed_actions` | `List[str]` | `lambda: list(DEFAULT_ALLOWED_ACTIONS)()` | Allow-list of action kinds a proposal may use: retry_run, rerun_partition, materialize_assets, open_ticket, notify. Anything else is rejected with a recorded reason. Default: the safe ones (retry_run, rerun_partition, open_ticket). |
| `allowed_materialize_assets` | `List[str]` | — | When materialize_assets is allowed, restrict it to these asset keys (e.g. ['raw/orders']). Default: any asset in the code location. |
| `max_actions_per_proposal` | `int` | `5` | Actions beyond this count in one proposal are rejected. |
| `max_retries_per_run` | `int` | `2` | retry_run launches at most this many retries for one failure (the chain's root run). |
| `remediation_jobs` | `Dict[str, str]` | — | The jobs retry_run / rerun_partition / materialize_assets launch runs through: {name: asset selection string}, e.g. {core: 'group:core', daily: 'group:daily'}. Each becomes job `<name_prefix>_remediate_<name>`. A Dagster… _(full docs in schema.json + component README)_ |
| `approval_step_id` | `str` | `"remediation"` | Step id used in the approval fingerprint / pending record (shown by agent_review_queue). |
| `approval_max_age_hours` | `float` | — | An approval token older than this is expired. Default: no expiry. |
| `auto_approve` | `Dict[str, Any]` | — | OFF by default. {kinds: [retry_run], max_per_hour: 3, min_confidence: 0.8}: proposals whose every action is in `kinds` and whose confidence is high enough are approved by the policy itself (token approver: auto_approve)… _(full docs in schema.json + component README)_ |
| `webhook_url_env_var` | `str` | — | Name of the environment variable holding the webhook URL used by `notify` (never logged or written). |
| `ticket_webhook_url_env_var` | `str` | — | Name of the environment variable holding a webhook URL that also receives each `open_ticket` ticket (never logged or written). |
| `webhook_timeout_seconds` | `float` | `10.0` | Timeout for webhook POSTs. |
| `outcome_check_after_minutes` | `float` | `30.0` | How long after an action launched runs to judge whether it fixed the failure (outcome: fixed \| not_fixed). |
| `store_resource_key` | `str` | — | Optional resource key of a ContextReceiptStore: when a proposal carries a `receipt_id`, the outcome is also recorded with `record_outcome`. Skipped cleanly when absent. |

[//]: # (FIELDS:END)
