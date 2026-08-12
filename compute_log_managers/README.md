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
| [`otlp.OtlpComputeLogManager`](otlp/) | Streams op stdout/stderr via OTLP/HTTP to any OTel-compatible backend (Splunk via Splunk OTel Collector, Datadog, Honeycomb, Sumo, Loki, CloudWatch, ...). | Customers who already run an OTel Collector or want vendor portability. One config swap = different backend. |
| [`tee.TeeComputeLogManager`](tee/) | Composes N inner CLMs. Fan-out writes, first-success reads. | Sending to multiple destinations from one `dagster.yaml`. Common cases: **your object storage (Azure Blob / S3 / GCS) + Dagster+** to keep the Dagster+ UI's inline log viewer working when you also send to your own bucket; Splunk + Dagster+ for compliance dual-write. |

### HEC direct vs OTLP — which Splunk path?

Both target Splunk. Different ergonomics:

| | HEC direct ([`splunk/`](splunk/)) | OTLP ([`otlp/`](otlp/)) → Splunk OTel Collector |
|---|---|---|
| Hops | Dagster → Splunk | Dagster → Collector → Splunk |
| Setup | HEC token + endpoint | Operate an OTel Collector |
| Reliability | Splunk down = events drop | Collector usually buffers + retries |
| Vendor portability | Splunk-only | Multi-vendor (Splunk, Datadog, Honeycomb, Sumo, Loki, CloudWatch, ...) |
| Field mapping | Direct → Splunk fields | Collector translates OTel semantic conventions |
| Splunk's current docs | Still supported, mature | "Modern recommended path" |

**Use HEC** if Splunk is your only observability backend and adding an OTel Collector is unwelcome ops surface. **Use OTLP** if you already run a Collector for app traces/metrics, or want to keep `dagster.yaml` portable across vendors. They coexist cleanly — Tee can write to both at once if you want belt-and-suspenders.

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

## Install — no `dagster-community-components` pip package required

**Every compute log manager in this folder is a self-contained single file** (plus a tiny `__init__.py`). Zero cross-file imports, no dependency on any other part of `dagster-community-components` beyond Dagster itself. Customers who want a single manager can copy two files into their own Dagster+ project and reference the copied module path from `dagster.yaml` — no wheel install of this package required.

### The 4-step recipe

1. **Copy the two files** for the manager you want into a directory anywhere in a Python module path Dagster can import. For a standard `create-dagster` project, `src/<your_pkg>/compute_log_managers/<name>/` works cleanly:

   ```
   src/my_dagster_project/compute_log_managers/splunk/
   ├── __init__.py             # from splunk/__init__.py
   └── compute_log_manager.py  # from splunk/compute_log_manager.py
   ```

   Direct raw-file URLs (also linked from each per-manager README):

   - **splunk**: [`compute_log_manager.py`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/compute_log_managers/splunk/compute_log_manager.py) + [`__init__.py`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/compute_log_managers/splunk/__init__.py)
   - **otlp**: [`compute_log_manager.py`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/compute_log_managers/otlp/compute_log_manager.py) + [`__init__.py`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/compute_log_managers/otlp/__init__.py)
   - **tee**: [`compute_log_manager.py`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/compute_log_managers/tee/compute_log_manager.py) + [`__init__.py`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/compute_log_managers/tee/__init__.py)

2. **`pip install` the runtime deps** (per manager) into whatever environment loads `dagster.yaml`:

   | Manager | Runtime deps |
   |---|---|
   | splunk | `requests` |
   | otlp   | `requests` |
   | tee    | *(none — `pyyaml` is already a transitive Dagster dep)* |

3. **Point `dagster.yaml` at the copied module path** — use your own package name, not `dagster_community_components`:

   ```yaml
   compute_logs:
     module: my_dagster_project.compute_log_managers.splunk    # ← your path
     class: SplunkComputeLogManager
     config:
       hec_url: https://splunk.acme.com:8088/services/collector
       hec_token: {env: SPLUNK_HEC_TOKEN}
       # ... (see per-manager README for full field list)
   ```

4. **Restart Dagster** (`dagster-daemon` + `dagster-webserver` + user-code containers) so the new manager takes effect.

### Where `dagster.yaml` lives per Dagster+ deployment shape

- **OSS local dev** — `$DAGSTER_HOME/dagster.yaml`.
- **Dagster+ Hybrid** — on the **agent** container, not the code-location container. Bake the copied files into the agent image, or mount them, so the daemon's Python can `import my_dagster_project.compute_log_managers.splunk`. Restart the agent to pick up changes.
- **Dagster+ Serverless** — compute-log-manager configuration goes through your deployment settings / `dagster_cloud.yaml`. The copied files need to ship with your code-location container image.

### Tee caveat — inner managers must be importable too

`TeeComputeLogManager` dynamically imports each inner manager by its `module:` string. If you copy Tee, every manager listed under `managers:` must resolve at import time — either via pip install (e.g. `dagster_cloud.storage.compute_logs` for `CloudComputeLogManager` when `dagster-cloud` is installed) or via its own copied path in your project.

### Alternative: pip install the whole package (only if you want other community components too)

If you're already using other components from `dagster-community-components` in the same project, the manager classes are also available under:

```python
from dagster_community_components.compute_log_managers.splunk import SplunkComputeLogManager
```

But this is optional — the standalone copy-paste path above is the recommended install for customers who only want the compute log manager and nothing else.

## Where `dagster.yaml` lives

After installing (via either path above), the `compute_logs:` YAML block goes into your `dagster.yaml`. Location depends on the deployment shape:

- **OSS local dev**: `$DAGSTER_HOME/dagster.yaml`
- **OSS production**: same file, baked into the agent / daemon / webserver container image
- **Dagster+ Hybrid**: agent-side `dagster.yaml`, mounted into the agent container
- **Dagster+ Serverless**: configure via `dagster_cloud.yaml` / deployment settings; the manager code ships with your code-location container image

After editing, restart `dagster-daemon` + `dagster-webserver` + any user-code containers so the new manager takes effect.
