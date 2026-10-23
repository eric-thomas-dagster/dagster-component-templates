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
`only_jobs` / `ignore_jobs`. State lives in `<evidence_dir>/_state/failure_sensor.json`. Replaying a tick (`run_key = run_id`) never
double-launches and does not consume budget.

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
