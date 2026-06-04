# TeeComputeLogManager

A Dagster instance has exactly one compute log manager. `TeeComputeLogManager` composes N of them — fan out writes, first-success reads — so you can route compute logs to multiple destinations from one `dagster.yaml`.

Common use cases:

- **Splunk + Dagster+** — OSS-managed SIEM compliance, plus Dagster+'s inline UI viewer
- **S3 + Splunk** — long-term archive in S3, real-time alerting in Splunk
- **S3 + OTLP Collector** — archive in S3, observability stack receives via OTel
- **Local + Splunk** during cutover — keep the existing local-dir viewer working while you bring Splunk online

## Semantics

| Method | Behavior |
|---|---|
| `upload_to_cloud_storage` | Fan out to all inner managers. Inner failures are logged and skipped (use `fail_on_partial_upload: true` for strict mode). |
| `cloud_storage_has_logs` | True if **any** inner reports logs for the key. |
| `download_from_cloud_storage` | Try inner managers in order; first success wins. |
| `display_path_for_type` | Returns the URL from the inner at `display_manager_index` — that's the deep-link the Dagster UI surfaces per step. |
| `delete_logs` | Fan out to all. Inner failures logged. |
| `upload_interval` | Minimum of all inner intervals (or `null` if none set one). |

Inner managers **share Tee's `local_manager`** — Tee patches each inner's `_local_manager` attribute at construction. This is intentional: there's only one local copy of the captured logs on disk, and every inner reads from it. Inner `local_dir` config is ignored.

## Config reference

| Field | Type | Default | Description |
|---|---|---|---|
| `managers` | list[dict] | required | Inner CLM configs. Each: `{module, class, config}`. |
| `local_dir` | str (env) | system temp dir | Shared local capture directory. See [About `local_dir`](#about-local_dir). |
| `display_manager_index` | int | `0` | Which inner manager's URL the UI shows. |
| `fail_on_partial_upload` | bool | `false` | If True, raise on any inner upload failure. |

### About `local_dir`

`local_dir` is where Dagster captures op stdout/stderr to disk *during* execution — the CLM reads from this path at step finish and ships to each inner destination (Splunk, OTLP, Dagster+, …). After upload the local file isn't load-bearing; each inner destination is the system of record.

Defaults to the system temp directory (`/tmp` on Linux containers) when omitted. That default works fine in:

- **Dagster+ Serverless** — containers are ephemeral but each has its own `/tmp` for the duration of the step
- **Dagster+ Hybrid** — same: the user-code container's `/tmp` lives long enough for capture → upload
- **OSS in K8s** — `/tmp` lives on the default `emptyDir` volume already
- **Local `dg dev`** — `/tmp` on macOS / Linux

Set it explicitly when you want either:

- Compute log captures to survive a mid-step container restart (rare — point at a mounted persistent volume)
- A dedicated directory for ops reasons (audit policy, cleanup automation, separate volume sizing)

The Tee value is shared with every inner manager — Tee patches each inner's `_local_manager` at construction so there's a single source of truth on disk. Inner `local_dir` config is **ignored**.

## Example — Splunk + Dagster+

```yaml
compute_logs:
  module: dagster_community_components.compute_log_managers.tee
  class: TeeComputeLogManager
  config:
    local_dir: /tmp/dagster_compute_logs
    display_manager_index: 0
    managers:
      - module: dagster_community_components.compute_log_managers.splunk
        class: SplunkComputeLogManager
        config:
          hec_url: https://splunk.acme.com:8088/services/collector
          hec_token: {env: SPLUNK_HEC_TOKEN}
          splunk_web_url: https://splunk.acme.com:8000
      - module: dagster_cloud.storage.compute_logs
        class: CloudComputeLogManager
        config: {}
```

Per step in the Dagster UI: "View logs in Splunk →" button (Splunk is index 0). Logs are ALSO uploaded to Dagster+'s storage so the inline UI viewer works against the Dagster+-cached copy.

## Example — S3 + Splunk + Datadog OTLP

```yaml
compute_logs:
  module: dagster_community_components.compute_log_managers.tee
  class: TeeComputeLogManager
  config:
    local_dir: /tmp/dagster_compute_logs
    display_manager_index: 0    # S3 presigned URL in the UI
    fail_on_partial_upload: false
    managers:
      - module: dagster_aws.s3.compute_log_manager
        class: S3ComputeLogManager
        config:
          bucket: dagster-compute-logs
          prefix: prod/
      - module: dagster_community_components.compute_log_managers.splunk
        class: SplunkComputeLogManager
        config:
          hec_url: https://splunk.acme.com:8088/services/collector
          hec_token: {env: SPLUNK_HEC_TOKEN}
      - module: dagster_community_components.compute_log_managers.otlp
        class: OtlpComputeLogManager  # (future — see ../otlp/)
        config: {...}
```

## Cost / performance notes

- **Wall-clock latency at step finish**: serial fan-out. Each inner's `upload_to_cloud_storage` runs sequentially. For N managers, expect the upload phase to take roughly `sum(individual_upload_times)`. If you have a slow destination (e.g. cross-region S3), put it last so the fast destinations clear first.
- **Idempotency**: not guaranteed. If `fail_on_partial_upload: false` (default) and Splunk succeeds but Dagster+ fails, you'll have a partial state on the next upload. For at-least-once semantics across all destinations, set `fail_on_partial_upload: true` — the run will surface the error and the local capture stays around for replay.
- **Memory**: zero — each inner reads from the shared local file, no buffer copies.

## Caveats

- **Subscriptions**: live UI streaming uses `Tee`'s local file watcher, NOT each inner's. Visitors see the local capture in real-time; inner destinations get the file in one batch at step finish (or every `upload_interval` seconds).
- **The `_local_manager` patch is private API**: We mutate each inner's `_local_manager` attribute after construction. This works because every `CloudStorageComputeLogManager` subclass we've inspected uses that attribute by convention. If Dagster ever changes the contract, this breaks — flagged here for future maintainers.

## See also

- [`splunk/`](../splunk/) — the Splunk inner CLM
- [`otlp/`](../otlp/) — coming next: OTLP/HTTP for Datadog / Honeycomb / Sumo / OTel Collector
- [Dagster's S3ComputeLogManager](https://docs.dagster.io/_apidocs/libraries/dagster-aws#dagster_aws.s3.S3ComputeLogManager) — the reference cloud CLM
