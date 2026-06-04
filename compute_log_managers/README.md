# Community-maintained Dagster compute log managers

Unlike the components in [`assets/`](../assets/), [`integrations/`](../integrations/), etc. — which are defs-folder entities loaded by `load_from_defs_folder` at code-location load time — **compute log managers are instance-level infrastructure** configured in `dagster.yaml`:

```yaml
compute_logs:
  module: dagster_community_components.compute_log_managers.splunk
  class: SplunkComputeLogManager
  config:
    hec_url: https://splunk.acme.com:8088/services/collector
    hec_token: {env: SPLUNK_HEC_TOKEN}
```

A Dagster instance has **exactly one** compute log manager. The default `LocalComputeLogManager` writes to local disk. Dagster+ uses `CloudComputeLogManager` (uploads to Dagster+'s managed storage). The dagster-aws / dagster-gcp / dagster-azure packages provide S3 / GCS / ADLS implementations. **This package adds:**

| Manager | What it does | When to use |
|---|---|---|
| [`splunk.SplunkComputeLogManager`](splunk/) | Streams op stdout/stderr to Splunk HEC. UI surfaces "View in Splunk →" deep-links per step. | Customers running Dagster OSS who want compute logs in Splunk and never touching Dagster+. Or Dagster+ customers who need a Splunk copy for compliance — compose via Tee. |
| [`tee.TeeComputeLogManager`](tee/) | Composes N inner CLMs. Fan-out writes, first-success reads. | Sending to multiple destinations from one `dagster.yaml`. Common case: Splunk + Dagster+. |

Coming soon:

| Manager | What it'll do |
|---|---|
| `otlp.OtlpComputeLogManager` | OTLP/HTTP logs — works against any OTel Collector. Targets Splunk (via Splunk OTel Collector), Datadog, Honeycomb, Sumo, etc. through one wire protocol. |

## How this differs from the `audit_logs_to_*` sinks

Both touch Splunk. They operate at different layers:

| | `audit_logs_to_splunk` (sink) | `SplunkComputeLogManager` (this package) |
|---|---|---|
| Layer | Project (defs.yaml, asset graph) | Instance (`dagster.yaml`) |
| What ships | Audit-event DataFrames (pulled from Dagster+ GraphQL or similar) | Op stdout/stderr from every step the executor runs |
| When it runs | On schedule, as a Dagster asset materialization | Continuously — at every step finish |
| Dagster+ dependency | Often pulls from Dagster+ GraphQL | None — works on Dagster OSS |
| Use case | "Send Dagster's run history to my SIEM" | "Send op print statements to my SIEM" |

Use both if you need both. They're complementary, not redundant.

## Setup

Compute log managers are configured in `dagster.yaml`. Drop the YAML block above into:

- **OSS local dev**: `$DAGSTER_HOME/dagster.yaml`
- **OSS deployments**: same file, baked into the agent / daemon / webserver image
- **Dagster+ Hybrid**: agent-side `dagster.yaml`, mounted into the agent container

After editing, restart `dagster-daemon` + `dagster-webserver` + any user-code containers so the new manager takes effect.

## Pip packaging

These ship inside the `dagster-community-components` wheel and import as:

```python
from dagster_community_components.compute_log_managers.splunk import SplunkComputeLogManager
from dagster_community_components.compute_log_managers.tee import TeeComputeLogManager
```

But you don't need to import them in Python code — `dagster.yaml` does the import via `module` + `class` strings. The package just needs to be installed in whatever environment loads `dagster.yaml`.

Optional runtime deps:

- `requests` for the Splunk HEC POST path. Standard in most Python environments — but if you're shipping a minimal image, `pip install dagster-community-components[splunk]` pulls it explicitly (see `pyproject.toml`).
- `pyyaml` for the Tee CLM's inner-manager rehydration (already a transitive dep via Dagster).
