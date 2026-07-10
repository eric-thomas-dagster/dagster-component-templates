# SplunkComputeLogManager

Send Dagster op stdout/stderr to **Splunk via the HTTP Event Collector (HEC)** at the instance layer — configured in `dagster.yaml`, not in your project's `defs/`.

A Dagster instance has exactly one compute log manager. Use this when:

- You're on Dagster OSS and want compute logs to land directly in Splunk (no Dagster+ involvement)
- You're on Dagster+ but compliance / SIEM workflows require logs in Splunk too — wrap this in [`TeeComputeLogManager`](../tee/) to send to **both**

This is *operational telemetry plumbing*. Distinct from `audit_logs_to_splunk` (which ships Dagster-managed audit-event DataFrames). This manager intercepts the captured-stdout/stderr files Dagster's executor produces from every op.

## What lands in Splunk

One HEC event per line of captured stdout/stderr, with structured `fields`:

```
{
  "event": "INFO - my asset processed 1234 rows",
  "sourcetype": "dagster:compute_log",
  "source": "dagster",
  "host": "prod-east",
  "index": "dagster",
  "fields": {
    "dagster_run_id": "abc-123-uuid",
    "dagster_step_key": "process_orders",
    "dagster_io_type": "stdout",
    "dagster_partial": "false",
    "dagster_step_start_epoch": "1748534560.123",
    "dagster_step_start_iso": "2025-05-29T18:22:40.123000+00:00",
    "dagster_step_end_epoch": "1748534567.456",
    "dagster_step_end_iso": "2025-05-29T18:22:47.456000+00:00",
    "dagster_step_duration_ms": "7333",
    "dagster_step_status": "SUCCESS"
  }
}
```

### Step-timing fields

Alongside the run/step identifiers, every HEC event carries step-level timing metadata so you can filter, group, or sort logs by how long the step took and what its outcome was.

| Field | Type | Populated when | Notes |
|---|---|---|---|
| `dagster_step_start_epoch` | float | step has started | unix seconds |
| `dagster_step_start_iso` | string | step has started | UTC ISO-8601 |
| `dagster_step_end_epoch` | float | step has finished | unix seconds |
| `dagster_step_end_iso` | string | step has finished | UTC ISO-8601 |
| `dagster_step_duration_ms` | int | step has finished | `end - start`, milliseconds |
| `dagster_step_status` | string | step has finished | `SUCCESS`, `FAILURE`, `SKIPPED` |

For long-running steps, partial uploads mid-execution have `dagster_step_start_*` but not the end/duration/status fields — Dagster only knows those values after the step completes. The final upload after the step ends carries all six. If you want duration on every log line for a long step, backfill in Splunk with `eventstats`:

```spl
search index=dagster dagster_run_id="abc-123-uuid"
| eventstats
    max(dagster_step_end_epoch)   as _end
    max(dagster_step_duration_ms) as _duration
    values(dagster_step_status)   as _status
    by dagster_run_id dagster_step_key
| eval dagster_step_duration_ms = coalesce(dagster_step_duration_ms, _duration)
| eval dagster_step_end_epoch   = coalesce(dagster_step_end_epoch, _end)
| eval dagster_step_status      = coalesce(dagster_step_status, _status)
```

### Query examples

All logs for one run, chronologically:

```spl
search index=dagster dagster_run_id="abc-123-uuid" | sort _time
```

Log volume by step:

```spl
search index=dagster dagster_run_id="abc-123-uuid"
| stats count by dagster_step_key, dagster_io_type
```

Top 10 slowest steps in the last 24h:

```spl
search index=dagster earliest=-24h
| dedup dagster_run_id dagster_step_key
| sort -dagster_step_duration_ms
| head 10
| table dagster_run_id dagster_step_key dagster_step_duration_ms dagster_step_status
```

All logs from failed steps in the last hour:

```spl
search index=dagster dagster_step_status="FAILURE" earliest=-1h
```

## What the Dagster UI shows

Per step, the UI surfaces a **"View logs in Splunk →"** button that opens a pre-filtered Splunk Web search. The inline log viewer shows a stub message ("Compute logs were shipped to Splunk; view them at …") rather than re-fetching from Splunk on every load.

The deep-link URL is built from `splunk_web_url` + the run/step/io_type fields:

```
{splunk_web_url}/en-US/app/search/search?q=search+index%3D{index}+dagster_run_id%3D%22{run}%22+dagster_step_key%3D%22{step}%22+dagster_io_type%3D%22stdout%22+%7C+sort+_time
```

If `splunk_web_url` is unset, no deep-link is rendered and the UI shows the stub only.

## Setup

### 1. Configure HEC on the Splunk side

In Splunk Web → **Settings → Data inputs → HTTP Event Collector**:

1. **Global Settings** → set "All Tokens" to Enabled, listen port 8088.
2. **New Token**:
   - Name: `dagster`
   - Source type: `dagster:compute_log` (or whatever you want — has to match the manager's `sourcetype` field if you set it)
   - Indexes: pick or create one (e.g. `dagster`). Set "Default Index" to that one.
3. Copy the generated token — set it on every machine running Dagster:
   - **OSS deployments**: export `SPLUNK_HEC_TOKEN` on the daemon + every user-code container
   - **Dagster+ Hybrid**: set on the agent + every code-location container
4. **Splunk Cloud**: HEC endpoint is `https://<your-instance>.splunkcloud.com/services/collector` and HEC port is 443 (not 8088). Otherwise the same.

### 2. Add to `dagster.yaml`

```yaml
compute_logs:
  module: dagster_community_components.compute_log_managers.splunk
  class: SplunkComputeLogManager
  config:
    hec_url: https://splunk.acme.com:8088/services/collector
    hec_token:
      env: SPLUNK_HEC_TOKEN
    splunk_web_url: https://splunk.acme.com:8000
    index: dagster
    sourcetype: dagster:compute_log
    upload_interval: 30
```

A full annotated config is in [`example_dagster.yaml`](example_dagster.yaml).

### 3. Restart Dagster

The Dagster daemon + each code location loads `dagster.yaml` at startup. Restart everything (`dagster-daemon`, `dagster-webserver`, user-code containers) so the new compute log manager takes effect.

## Config reference

| Field | Type | Default | Description |
|---|---|---|---|
| `hec_url` | str (env) | — | Splunk HEC endpoint, e.g. `https://splunk.acme.com:8088/services/collector`. |
| `hec_token` | str (env) | — | Splunk HEC token. Required. Usually `{env: SPLUNK_HEC_TOKEN}`. |
| `splunk_web_url` | str (env) | none | Splunk Web base for "View in Splunk" deep-links. If unset, no deep-link is rendered. |
| `index` | str (env) | (HEC token default) | Splunk index. Override the token's default. |
| `sourcetype` | str (env) | `dagster:compute_log` | Splunk `sourcetype` field. |
| `source` | str (env) | `dagster` | Splunk `source` field. |
| `host` | str (env) | local hostname | Splunk `host` field. |
| `verify_ssl` | bool | `true` | TLS cert verification. Set `false` for self-signed dev splunks. |
| `batch_size` | int | `100` | HEC events per HTTP POST. Tune up to ~1000 if your Splunk allows. |
| `request_timeout_seconds` | int | `30` | HTTP timeout per HEC POST. |
| `skip_empty_files` | bool | `false` | Skip uploads when the local log file is empty. |
| `local_dir` | str (env) | system temp dir | Local capture directory for in-flight streaming + UI stub files. |
| `upload_interval` | int \| null | `null` | Interval in seconds for partial uploads of long-running ops. `null` = upload only on step finish. |

## Sending to BOTH Splunk and Dagster+ (or any other CLM)

Use [`TeeComputeLogManager`](../tee/) in `dagster.yaml`:

```yaml
compute_logs:
  module: dagster_community_components.compute_log_managers.tee
  class: TeeComputeLogManager
  config:
    local_dir: /tmp/dagster_compute_logs
    display_manager_index: 0    # show Splunk's URL in the UI
    managers:
      - module: dagster_community_components.compute_log_managers.splunk
        class: SplunkComputeLogManager
        config:
          hec_url: https://splunk.acme.com:8088/services/collector
          hec_token: {env: SPLUNK_HEC_TOKEN}
          splunk_web_url: https://splunk.acme.com:8000
      - module: dagster_cloud.storage.compute_logs
        class: CloudComputeLogManager
        config: {}    # Dagster+'s default config
```

## Cost / performance notes

- **HEC throughput**: a single HEC token can handle hundreds of events/sec; for very chatty workloads bump `batch_size` to reduce HTTP overhead. Splunk's per-byte ingest pricing is what bites — keep `sourcetype` distinct so you can scope retention policies aggressively.
- **Local disk**: the captured log file is on disk for the duration of the step + briefly after upload. `upload_interval: N` flushes partials every N seconds so long-running ops don't pile up locally.
- **HEC outages**: if Splunk is unreachable, the upload raises and Dagster's CLM machinery logs the failure. The run itself doesn't fail — the local file stays around. There's no built-in retry queue; for at-least-once delivery semantics, run Splunk HEC behind a small reliable forwarder (vmagent, OTel Collector, Cribl, etc.).

## See also

- [Dagster's CloudStorageComputeLogManager docs](https://docs.dagster.io/_apidocs/libraries/dagster-aws#dagster_aws.s3.S3ComputeLogManager) — the abstract base class this manager implements
- [Splunk HEC overview](https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector)
- [`tee/`](../tee/) — compose this with Dagster+'s CloudComputeLogManager to write to both
- `audit_logs_to_splunk` (in `assets/sinks/`) — **distinct** — ships audit-event DataFrames, not compute logs
