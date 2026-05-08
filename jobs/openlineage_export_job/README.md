# OpenLineageExportJobComponent

Op-shaped job that emits OpenLineage `RunEvent` (START / COMPLETE / FAIL)
to a configured collector — Marquez, OpenLineage Proxy, Datakin, or any
other consumer of the OpenLineage v2 event spec.

## When to use this vs `lineage_to_openlineage`

The registry has two complementary OpenLineage paths:

| Component | Shape | Use case |
|---|---|---|
| `lineage_to_openlineage` | **asset** sink | One-shot or scheduled snapshot of the asset graph. Useful for bulk catalog sync. |
| `openlineage_export_job` (this) | **op job** | Per-run lifecycle event (start/complete/fail). Useful for real-time run-tracking in an existing OpenLineage collector alongside Spark / Airflow / dbt jobs. |

Pick this one when your team already operates an OpenLineage collector
and wants Dagster runs to appear on the same operational lineage surface
as everything else.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `job_name` | `str` | required | Dagster job name |
| `schedule` | `Optional[str]` | `null` | Cron schedule; `null` = manual / sensor-triggered |
| `default_status` | `str` | `STOPPED` | `STOPPED` \| `RUNNING` |
| `tags` | `Optional[dict]` | `null` | Dagster job tags |
| `endpoint_env_var` | `str` | `OPENLINEAGE_URL` | Env var with the collector URL |
| `api_key_env_var` | `Optional[str]` | `null` | Env var with bearer token (if collector requires auth) |
| `namespace` | `str` | `dagster` | OpenLineage namespace |
| `ol_job_name` | `Optional[str]` | `null` (= `job_name`) | OpenLineage job name |
| `producer` | `str` | this component's URI | OpenLineage producer URI |
| `inputs_template` | `Optional[list]` | `null` | Static inputs attached to each event |
| `outputs_template` | `Optional[list]` | `null` | Static outputs attached to each event |
| `fail_on_collector_error` | `bool` | `false` | If `true`, fail the run when the collector is unreachable |

## YAML example

```yaml
type: dagster_component_templates.OpenLineageExportJobComponent
attributes:
  job_name: emit_openlineage_run_events
  schedule: "*/15 * * * *"            # every 15 minutes
  default_status: RUNNING
  endpoint_env_var: OPENLINEAGE_URL   # http://marquez:5000/api/v1/lineage
  namespace: dagster.prod
  ol_job_name: nightly_pipeline
  fail_on_collector_error: false      # best-effort emit
```

## Required env vars

```bash
OPENLINEAGE_URL=http://marquez:5000/api/v1/lineage
# Optional, if your collector requires auth:
OPENLINEAGE_API_KEY=...
```

## Behavior

- On each tick, emits two events to the collector:
  - `START` — before any work
  - `COMPLETE` — after the op body returns
- If the op raises, Dagster emits a step failure (Dagster's run history
  records the failure; this job doesn't currently send a `FAIL` event —
  see Limitations below).
- All events share a single `run.runId` (Dagster's run id), so events
  correlate to the Dagster run in the UI.

## Limitations

- The op currently emits `START` and `COMPLETE` synchronously around its
  own body. It does **not** wrap *other* ops in your code location — to
  emit per-op lineage for an entire pipeline, prefer Dagster's run-event
  hooks (a future iteration of this component).
- Inputs / outputs are static templates. Dynamic, per-run discovery of
  upstream / downstream lineage requires a deeper integration with the
  asset graph. Pair with `lineage_to_openlineage` for that.
- The HTTP emitter uses Python's stdlib `urllib.request` — no auth
  beyond bearer tokens, no retries beyond a single attempt. Production
  deployments should swap in the official OpenLineage Python client
  for retry / batching / error handling guarantees.

## Roadmap (see [TODO.md](../../TODO.md))

- Per-op lineage emission via Dagster `EventLogEntry` hooks
- Dynamic inputs / outputs derived from the asset graph
- Built-in retry + backoff on collector errors
