# Streaming Run Health Sensor

**Self-healing loop for a long-running asset.** Polls Dagster's run store every N seconds — if no run of the target job is currently `RUNNING` / `STARTING` / `QUEUED` / `NOT_STARTED`, fires a new `RunRequest`. Companion to `StreamingConsumerComponent`.

Result: **24/7 uptime from a chain of bounded runs.** The streaming consumer exits cleanly at `max_seconds` (or crashes / times out) — this sensor detects the gap and launches the next run. Restart gap ≤ `minimum_interval_seconds`.

## Quick example

```yaml
type: dagster_community_components.StreamingRunHealthSensorComponent
attributes:
  sensor_name: order_events_health
  job_name: __ASSET_JOB                    # default for asset-based deployments
  asset_selection: [order_events]          # narrow the run to just the streamer
  minimum_interval_seconds: 60             # detect + relaunch within 60s
  default_status: running                  # start enabled
```

Pair with:

```yaml
type: dagster_community_components.StreamingConsumerComponent
attributes:
  asset_name: order_events
  max_seconds: 3600                        # 1h bounded — sensor restarts
  # ... queue + sink config
```

## Behavior

Every `minimum_interval_seconds`:

1. Query `instance.get_runs(filters=RunsFilter(job_name=job_name, statuses=[QUEUED, NOT_STARTED, STARTING, STARTED]))`.
2. If any run is returned → emit `SkipReason` (nothing to do).
3. If none → emit a `RunRequest` with a `run_key` derived from the current millisecond timestamp (dedupe within a tick).

The `RunRequest` includes the sensor's `asset_selection` if set, so the launched run materializes just the streaming asset (not everything in the job).

## Serverless notes

- Set `minimum_interval_seconds` as low as 30s for tight restart windows on Serverless. The sensor itself is essentially free (one run-store query per interval).
- Combine with `default_status: running` so the sensor auto-starts on code-location load — no manual "flip switch on" in the UI.
- If your streaming asset's `max_seconds` is 3600 (1h), and the sensor's `minimum_interval_seconds` is 60, expect ~1h of ingest per run + up to 60s gap between runs → ~98% uptime. Drop the sensor interval to 30s → ~99% uptime. Set `max_seconds: null` on the streamer → gap goes to zero except on crash / timeout.

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `sensor_name` | ✅ | — | Unique sensor name. |
| `job_name` | ✅ | — | Job to watch + restart. Typically `__ASSET_JOB`. |
| `asset_selection` |  | — | Optional list of asset keys to narrow the launched run. |
| `minimum_interval_seconds` |  | `60` | How often to check run health. Min 5. |
| `default_status` |  | `running` | `running` or `stopped`. |
| `description` |  | — | Optional sensor description. |

## When to reach for this vs. a regular sensor

- **`filesystem_monitor` / `s3_monitor` / etc.** — fire a run per detected event (file, S3 key). Many short runs. Use when work is naturally batched around discrete external events.
- **`streaming_run_health_sensor`** — keep ONE long run alive. Use when the source is a queue that you want to consume continuously.

The two are composable: use `filesystem_monitor` for batch file drops AND `streaming_run_health_sensor` for the Kafka consumer, running side-by-side in the same code location.
