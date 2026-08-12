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

## Where the config goes + where the CLM code needs to be importable

**Config goes on the agent.** Set the `compute_logs:` block in the agent's `dagster.yaml`, or (Helm-deployed agents) in the Helm values under `computeLogs.custom` (`module` / `class` / `config`). Default for Dagster+ is `CloudComputeLogManager` writing to a Dagster+-managed S3 bucket; drop in `dagster.NoOpComputeLogManager` if you want to disable forwarding to Dagster+ entirely.

**Where the CLM class has to be importable depends on whether it's shipped or custom:**

| CLM origin | Where it needs to live | Rollout effort |
|---|---|---|
| **Shipped** (`dagster_aws.s3` / `dagster_azure.blob` / `dagster_gcp.gcs` / `dagster_cloud.storage.compute_logs` / `dagster.NoOpComputeLogManager`) | Already in the Dagster+ Hybrid agent image. Also already in a standard code-location image via the corresponding pip package if you installed it. | **Helm upgrade only** — no image rebuild. Just edit Helm values / agent `dagster.yaml` and roll the agent. |
| **Custom** (**Splunk / OTLP / Tee in this folder, or anything you wrote**) | Importable in **both** the code-location image (run worker instantiates it after each step) AND the agent image (loads `dagster.yaml`). Copying the two files into `src/<your_pkg>/` covers the code-location image automatically. The agent image needs one extra step — see below. | Helm upgrade + agent-image work. |

### Making a custom CLM available to the agent image (Dagster+ Hybrid)

Three paths, from leanest to easiest:

1. **Bake the two files directly into a custom agent image (leanest).** Start from `dagster/dagster-cloud-agent`, `COPY` your two CLM files onto its Python path in the Dockerfile, install any runtime deps (`requests` for splunk/otlp), deploy that image as your agent. Zero extra Python packages installed; just the ~600-line file itself. Recommended when you want to keep the agent image slim.

2. **Package the two files as a tiny local pip package and install into both images.** Wrap them in a minimal `pyproject.toml` (deps: `dagster`, `requests`), publish to your internal PyPI (or install from a git URL), and `pip install <your-clm-package>` in both your code-location image AND a custom agent image built from `dagster/dagster-cloud-agent`. Helm-managed agents can pin the install via the agent's `extraPipInstalls` / equivalent values. Recommended when a team already has an internal-PyPI pattern.

3. **`pip install dagster-community-components[<extra>]` into both images.** Simplest one-line change. The base package's runtime deps are minimal (`dagster` + `pandas`) — per-component optional deps aren't pulled in unless you ask for them via extras:

   ```bash
   pip install dagster-community-components[splunk]                # adds requests
   pip install dagster-community-components[otlp]                  # adds requests
   pip install dagster-community-components[tee]                   # no extra runtime deps (symmetry)
   pip install dagster-community-components[compute-log-managers]  # bundle: splunk + otlp + tee
   ```

   The CLM classes are then available under `dagster_community_components.compute_log_managers.splunk` / `.otlp` / `.tee`. The wheel itself contains the full component registry, but nothing else installs unless you install its extra. Fine option if you'd rather manage this via `pip` than by copying files around.

**Alternative: skip the custom CLM entirely with `show_url_only: true`.** The shipped `dagster_aws.s3.S3ComputeLogManager` / `dagster_azure.blob.AzureBlobComputeLogManager` / `dagster_gcp.gcs.GCSComputeLogManager` accept `show_url_only: true` — Dagster+ never sees log contents, the run page just links out to the object in your bucket. This is a **Helm-values-only change** and needs no image work at all. Good when compliance / retention is the goal and a link-out UI is acceptable. Reach for Tee when you want *both* the bucket copy AND the Dagster+ inline log viewer.

### Dagster+ Serverless

Custom CLMs are constrained on Serverless — Dagster+ manages the agent, so you can't add packages to it. Use a shipped CLM (with `show_url_only: true` if you want a bucket link-out pattern), or contact Dagster support.
