# OtlpComputeLogManager

Send Dagster op stdout/stderr to **any OpenTelemetry-compatible logs backend** via OTLP/HTTP (JSON). One wire protocol, many destinations:

- **Splunk** via [Splunk Distribution of OpenTelemetry Collector](https://docs.splunk.com/observability/en/gdi/opentelemetry/opentelemetry.html)
- **Datadog** OTel ingest (`https://otlp-intake.logs.datadoghq.com`)
- **Honeycomb** (`https://api.honeycomb.io`)
- **Sumo Logic** OTel
- **Grafana Loki** via OTel Collector
- **AWS CloudWatch** via OTel Collector
- **New Relic** (`https://otlp.nr-data.net`)
- Any OTel Collector → forwards to anything

Compute log manager — sits at the Dagster **instance** layer (`dagster.yaml`), not the project layer.

## Why OTLP vs the dedicated `SplunkComputeLogManager`?

| | HEC direct (`splunk/`) | OTLP (`otlp/`, this manager) |
|---|---|---|
| Hops | Dagster → Splunk | Dagster → Collector → Splunk |
| Setup | HEC token + endpoint | Operate an OTel Collector |
| Reliability | Splunk down = events drop | Collector usually buffers + retries |
| Vendor portability | Splunk-only | Splunk + Datadog + Honeycomb + Sumo + Loki + CloudWatch + ... |
| Splunk's docs lean | "Still supported, mature" | "Modern recommended path" |

Pick OTLP if you already run an OTel Collector for app traces/metrics (Dagster slots into the existing pipeline), or if you need vendor portability. Pick HEC if Splunk is your only observability backend and adding a Collector is unwelcome ops surface.

## What lands at the backend

One OTLP `LogRecord` per line of captured stdout/stderr:

```json
{
  "resourceLogs": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "dagster"}},
        {"key": "service.instance.id", "value": {"stringValue": "prod-east"}}
      ]
    },
    "scopeLogs": [{
      "scope": {"name": "dagster_community_components.compute_log_managers.otlp"},
      "logRecords": [
        {
          "timeUnixNano": "1748534567000000000",
          "severityNumber": 9,
          "severityText": "INFO",
          "body": {"stringValue": "INFO - my asset processed 1234 rows"},
          "attributes": [
            {"key": "dagster.run_id", "value": {"stringValue": "abc-123-uuid"}},
            {"key": "dagster.io_type", "value": {"stringValue": "stdout"}},
            {"key": "dagster.partial", "value": {"stringValue": "false"}},
            {"key": "dagster.step_key", "value": {"stringValue": "process_orders"}},
            {"key": "dagster.step_start_epoch", "value": {"doubleValue": 1748534560.123}},
            {"key": "dagster.step_start_iso", "value": {"stringValue": "2025-05-29T18:22:40.123000+00:00"}},
            {"key": "dagster.step_end_epoch", "value": {"doubleValue": 1748534567.456}},
            {"key": "dagster.step_end_iso", "value": {"stringValue": "2025-05-29T18:22:47.456000+00:00"}},
            {"key": "dagster.step_duration_ms", "value": {"intValue": 7333}},
            {"key": "dagster.step_status", "value": {"stringValue": "SUCCESS"}}
          ]
        }
      ]
    }]
  }]
}
```

Severity: stdout → 9 (INFO), stderr → 13 (WARN) by default. **Not ERROR** for stderr because Dagster doesn't distinguish app errors from operational stderr (most Python loggers write everything there). Override per-backend via `severity_stdout` / `severity_stderr` if your observability strategy treats stderr differently.

### Step-timing attributes

Alongside the run/step identifiers, every LogRecord carries step-level timing metadata so you can filter, group, or sort logs by how long the step took and what its outcome was.

| Attribute | Type | Populated when | Notes |
|---|---|---|---|
| `dagster.step_start_epoch` | double | step has started | unix seconds |
| `dagster.step_start_iso` | string | step has started | UTC ISO-8601 |
| `dagster.step_end_epoch` | double | step has finished | unix seconds |
| `dagster.step_end_iso` | string | step has finished | UTC ISO-8601 |
| `dagster.step_duration_ms` | int | step has finished | `end - start`, milliseconds |
| `dagster.step_status` | string | step has finished | `SUCCESS`, `FAILURE`, `SKIPPED` |

For long-running steps, partial uploads mid-execution have `dagster.step_start_*` but not the end/duration/status attributes — Dagster only knows those values after the step completes. The final upload after the step ends carries all six. If you want duration on every log line for a long step, backfill in your query language:

- **Splunk (via OTel Collector → HEC)**: `| eventstats max(dagster.step_duration_ms) as _dur by dagster.run_id dagster.step_key`
- **Datadog**: `@dagster.step_duration_ms` on the final event; use log-based metrics grouped by `@dagster.run_id`, `@dagster.step_key`
- **Honeycomb / any tracing UI**: filter by `dagster.step_status = FAILURE` for post-mortem, group by `dagster.step_key` for slow-step dashboards
- **Grafana Loki / LogQL**: use recording rules keyed on `dagster_run_id` + `dagster_step_key` to promote duration into metrics

## UI behavior

Per step, the Dagster UI shows a "View logs →" button that opens the URL from your `display_url_template` with `{run_id}` / `{step_key}` / `{io_type}` / `{location}` / `{service}` substituted. If `display_url_template` is unset, no deep-link is rendered (UI shows the stub message only).

Example templates per backend:

```yaml
# Honeycomb
display_url_template: "https://ui.honeycomb.io/acme/datasets/dagster_compute_logs?query=dagster.run_id%3D{run_id}"

# Datadog (logs query syntax)
display_url_template: "https://app.datadoghq.com/logs?query=%40dagster.run_id%3A{run_id}"

# Splunk Web (via OTel Collector)
display_url_template: "https://splunk.acme.com:8000/en-US/app/search/search?q=search%20dagster.run_id%3D%22{run_id}%22"

# Grafana Loki
display_url_template: "https://grafana.acme.com/explore?left=%7B%22queries%22%3A%5B%7B%22expr%22%3A%22%7Bservice_name%3D%5C%22{service}%5C%22%7D%20%7C%3D%20%5C%22{run_id}%5C%22%22%7D%5D%7D"
```

## Setup

### 1. Stand up an OTel Collector (or use a hosted one)

Most observability vendors offer a hosted OTLP endpoint. Examples:

| Vendor | Endpoint | Headers |
|---|---|---|
| Honeycomb | `https://api.honeycomb.io` | `x-honeycomb-team`, `x-honeycomb-dataset` |
| Datadog (US1) | `https://otlp-intake.logs.datadoghq.com` | `DD-API-KEY` |
| New Relic (US) | `https://otlp.nr-data.net` | `api-key` |
| Splunk Observability Cloud | `https://ingest.us0.signalfx.com/v2/log` (NB: SignalFx path) | `X-SF-Token` |
| Splunk Enterprise via OTC | `http://your-otc:4318` (no auth typical) | (intra-cluster) |

If you're running your own Collector, the receiver config looks like:

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

exporters:
  splunk_hec:
    token: ${SPLUNK_HEC_TOKEN}
    endpoint: https://splunk.acme.com:8088/services/collector

service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [splunk_hec]
```

### 2. Add to `dagster.yaml`

```yaml
compute_logs:
  module: dagster_community_components.compute_log_managers.otlp
  class: OtlpComputeLogManager
  config:
    otlp_endpoint: http://otel-collector.svc:4318
    service_name: dagster
    location_label: prod-east
    display_url_template: "https://splunk.acme.com:8000/en-US/app/search/search?q=search%20dagster.run_id%3D%22{run_id}%22"
    upload_interval: 30
```

Full annotated config in [`example_dagster.yaml`](example_dagster.yaml).

### 3. Restart

Daemon + webserver + any user-code containers pick up `dagster.yaml` at startup.

## Config reference

| Field | Type | Default | Description |
|---|---|---|---|
| `otlp_endpoint` | str (env) | — | Base URL. `/v1/logs` is appended. |
| `otlp_headers` | dict[str, str] | `{}` | Headers added to every OTLP POST (vendor-specific auth). |
| `service_name` | str (env) | `dagster` | OTel `service.name` resource attribute. |
| `location_label` | str (env) | `default` | OTel `service.instance.id` resource attribute. |
| `display_url_template` | str (env) | none | URL template for the Dagster UI deep-link. Supports `{run_id}` / `{step_key}` / `{io_type}` / `{location}` / `{service}` placeholders. |
| `severity_stdout` | int | `9` (INFO) | OTel severity number for stdout lines. |
| `severity_stderr` | int | `13` (WARN) | OTel severity number for stderr lines. |
| `verify_ssl` | bool | `true` | TLS cert verification. |
| `batch_size` | int | `100` | LogRecords per OTLP POST. |
| `request_timeout_seconds` | int | `30` | HTTP timeout per POST. |
| `skip_empty_files` | bool | `false` | Skip uploads when local file is empty. |
| `local_dir` | str (env) | system temp | Local capture directory. |
| `upload_interval` | int \| null | `null` | Partial-upload interval in seconds. `null` = upload on step finish only. |

## Sending to OTLP AND Dagster+

Wrap in [`TeeComputeLogManager`](../tee/):

```yaml
compute_logs:
  module: dagster_community_components.compute_log_managers.tee
  class: TeeComputeLogManager
  config:
    local_dir: /tmp/dagster_compute_logs
    display_manager_index: 0    # OTLP URL surfaces in UI
    managers:
      - module: dagster_community_components.compute_log_managers.otlp
        class: OtlpComputeLogManager
        config:
          otlp_endpoint: http://otel-collector.svc:4318
          display_url_template: "..."
      - module: dagster_cloud.storage.compute_logs
        class: CloudComputeLogManager
        config: {}
```

## Cost / performance notes

- **Latency**: OTLP/HTTP JSON over HTTPS — single round-trip per batch, ~5–50ms typical to a local Collector, ~50–200ms to hosted vendors. Set `batch_size` higher (e.g. 500) for chatty workloads to reduce HTTP overhead.
- **Reliability**: Collector failure modes vary. Most Collectors buffer locally and retry to the backend, so transient backend outages are absorbed. If your Collector is misconfigured or down, OTLP POSTs raise and Dagster's CLM machinery logs the error — the local file stays around but isn't auto-retried by this manager.
- **Cardinality**: each log line has the same low-cardinality attributes (`dagster.run_id`, `dagster.step_key`, `dagster.io_type`). Won't blow up tag-based pricing systems like Datadog or Honeycomb at sensible volumes.
- **Severity tagging**: stderr defaults to WARN, not ERROR. If your observability strategy treats stderr=ERROR (PagerDuty rules, etc.), bump `severity_stderr: 17` — but be aware Dagster + Python's `logging` write a lot of non-error content to stderr by default.

## See also

- [`splunk/`](../splunk/) — Splunk HEC direct (no Collector required, single hop)
- [`tee/`](../tee/) — compose OTLP with Dagster+ or any other CLM
- [OTLP/HTTP spec — logs](https://opentelemetry.io/docs/specs/otlp/#otlphttp-logs-binary-protocol)
- [Splunk Distribution of OpenTelemetry Collector](https://docs.splunk.com/observability/en/gdi/opentelemetry/opentelemetry.html)
- `dataframe_to_otlp_logs` (in `assets/sinks/`) — distinct: ships data-plane DataFrames as OTLP logs, not compute logs
- `dagster_failures_to_otlp_sensor` (in `sensors/`) — distinct: ships Dagster run-failure events to OTLP, not compute logs
