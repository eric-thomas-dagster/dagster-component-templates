# TeeComputeLogManager

A Dagster instance has exactly one compute log manager. `TeeComputeLogManager` composes N of them — fan out writes, first-success reads — so you can route compute logs to multiple destinations from one `dagster.yaml`.

Common use cases:

- **Your object storage (Azure Blob / S3 / GCS) + Dagster+** — keep the Dagster+ UI's inline log viewer working when you also send to your own bucket for long-term retention / compliance. Configuring only your bucket leaves the UI with a link, no inline viewer — Tee gives you both. See [example below](#example--your-own-object-storage--dagster-recover-the-inline-ui-viewer).
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

## Install into your Dagster+ project (no pip package required)

This is a **self-contained single file** — you don't need to `pip install dagster-community-components`. Copy the two files into your project and reference the copied module path from `dagster.yaml`.

**Drop these two files** into your project. Namespace them wherever fits; `src/<your_pkg>/compute_log_managers/tee/` is a clean default for `create-dagster` projects:

- [`compute_log_manager.py`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/compute_log_managers/tee/compute_log_manager.py) → `src/<your_pkg>/compute_log_managers/tee/compute_log_manager.py`
- [`__init__.py`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/compute_log_managers/tee/__init__.py) → `src/<your_pkg>/compute_log_managers/tee/__init__.py`

Quick fetch:

```bash
mkdir -p src/<your_pkg>/compute_log_managers/tee
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/compute_log_managers/tee/compute_log_manager.py \
  -o src/<your_pkg>/compute_log_managers/tee/compute_log_manager.py
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/compute_log_managers/tee/__init__.py \
  -o src/<your_pkg>/compute_log_managers/tee/__init__.py
```

**Runtime deps**: none — `pyyaml` is already a transitive Dagster dep. Nothing to `pip install`.

**Important — inner managers must be importable too.** Tee dynamically imports each inner manager listed under `managers:` by its `module:` string. Every one has to resolve at import time. So:

- `dagster_cloud.storage.compute_logs.CloudComputeLogManager` — resolves as long as `dagster-cloud` is installed (default on Dagster+).
- **The custom compute log managers you copy in** (e.g. splunk / otlp from this repo) — also need to be at their copied path. Point Tee's `managers[i].module` at your own package path (e.g. `my_dagster_project.compute_log_managers.splunk`), NOT `dagster_community_components.compute_log_managers.splunk`.

**Where the config + code need to live (Dagster+ Hybrid):**
The `compute_logs:` config goes on the **agent** (agent's `dagster.yaml` or Helm values under `computeLogs.custom`). Tee itself is a custom CLM, so it has to be importable in **both** the code-location image AND the agent image. Copying the files into `src/<your_pkg>/` covers the code-location image; the agent image needs one extra step — see [Making a custom CLM available to the agent image](../README.md#making-a-custom-clm-available-to-the-agent-image-dagster-hybrid) in the parent README. Every inner manager Tee wraps has the same requirement: shipped ones (`dagster_aws.s3`, `dagster_azure.blob`, `dagster_gcp.gcs`, `dagster_cloud.storage.compute_logs`) are already in both images; custom inner managers need the same treatment as Tee.

**Alternative for the "own object storage + link-out UI" case:** if you don't need the Dagster+ inline log viewer, skip Tee entirely and set `show_url_only: true` on the shipped S3 / Blob / GCS manager — no custom CLM, no image work, Helm-values change only. Use Tee only when you specifically want *both* the bucket copy AND the Dagster+ inline viewer.

## Example — Splunk + Dagster+

Use whichever `module:` paths match how you installed each manager — your own package (recommended) or the pip package:

```yaml
compute_logs:
  module: <your_pkg>.compute_log_managers.tee    # standalone copy (recommended)
  # module: dagster_community_components.compute_log_managers.tee    # alt: pip package
  class: TeeComputeLogManager
  config:
    local_dir: /tmp/dagster_compute_logs
    display_manager_index: 0
    managers:
      - module: <your_pkg>.compute_log_managers.splunk    # matches your copied path
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

## Example — your own object storage + Dagster+ (recover the inline UI viewer)

**The problem this solves.** If you configure `dagster.yaml` with only your own object storage (Azure Blob / S3 / GCS), the Dagster+ UI can only surface a *link* to the external storage — the inline log viewer stops working, because Dagster+ has no cached copy to render. That's a real regression in developer experience: every "look at this run's logs" click becomes a redirect + auth prompt + external tab.

**Tee fixes this cleanly.** Configure one Tee that writes to both your object storage (for long-term retention / compliance / your own SIEM ingest) AND Dagster+'s managed storage (for the inline UI viewer + fast dev iteration). Same log, two destinations, and the UI viewer stays first-class.

Azure Blob + Dagster+:

```yaml
compute_logs:
  module: <your_pkg>.compute_log_managers.tee
  class: TeeComputeLogManager
  config:
    local_dir: /tmp/dagster_compute_logs
    display_manager_index: 1              # let Dagster+ own the display URL — inline viewer works
    managers:
      - module: dagster_azure.blob.compute_log_manager
        class: AzureBlobComputeLogManager
        config:
          storage_account: myacme
          container: dagster-compute-logs
          secret_credential: {env: AZURE_STORAGE_KEY}
          prefix: prod/
      - module: dagster_cloud.storage.compute_logs
        class: CloudComputeLogManager
        config: {}
```

Same shape for S3 or GCS — just swap the first inner manager:

```yaml
# S3 variant — first inner is:
      - module: dagster_aws.s3.compute_log_manager
        class: S3ComputeLogManager
        config:
          bucket: dagster-compute-logs
          prefix: prod/

# GCS variant — first inner is:
      - module: dagster_gcp.gcs.compute_log_manager
        class: GCSComputeLogManager
        config:
          bucket: dagster-compute-logs
          prefix: prod/
```

`display_manager_index: 1` selects Dagster+ as the display manager so the UI keeps the inline viewer (with its cached copy). Set `display_manager_index: 0` if you'd rather have the UI link out to your object storage (matches the behavior you'd get if you only configured the object storage manager).

## Example — S3 + Splunk + Datadog OTLP

```yaml
compute_logs:
  module: <your_pkg>.compute_log_managers.tee
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
      - module: <your_pkg>.compute_log_managers.splunk
        class: SplunkComputeLogManager
        config:
          hec_url: https://splunk.acme.com:8088/services/collector
          hec_token: {env: SPLUNK_HEC_TOKEN}
      - module: <your_pkg>.compute_log_managers.otlp
        class: OtlpComputeLogManager
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
