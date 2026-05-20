# JupyterNotebookAssetComponent

Execute a Jupyter notebook as a Dagster asset using [Papermill](https://papermill.readthedocs.io/). Point at a `.ipynb` file, define parameters in YAML, and Dagster will run the notebook, save the fully-executed copy as an artifact, and surface execution metadata in the UI.

## Use case

Data science and analytics teams often author work in Jupyter notebooks. `JupyterNotebookAssetComponent` lets you:

- **Schedule or trigger notebooks** from the same Dagster UI used for all other pipeline assets.
- **Inject run-specific parameters** (date ranges, environment names, output schemas) without touching the notebook source.
- **Preserve full cell output** — every chart, table, and print statement — in a timestamped output notebook so results are auditable long after the run.
- **Gate downstream assets** on a successful notebook run via `deps`.
- **Pass data back to Dagster** through a lightweight convention (see [Returning metadata from the notebook](#returning-metadata-from-the-notebook) below).

## Prerequisites

Install Papermill and a Jupyter kernel in the environment that runs the Dagster worker:

```bash
pip install papermill>=2.4.0 ipykernel>=6.0.0
```

If your notebook uses a non-default kernel, install that kernel too and register it:

```bash
pip install ipykernel
python -m ipykernel install --user --name my_env --display-name "Python (my_env)"
```

Confirm the kernel name with `jupyter kernelspec list`.

## Quick start

```yaml
type: dagster_component_templates.JupyterNotebookAssetComponent
attributes:
  asset_name: monthly_revenue_report
  notebook_path: "{{ project_root }}/notebooks/revenue_analysis.ipynb"
  parameters:
    start_date: "2024-01-01"
    end_date: "2024-01-31"
    output_schema: analytics
  execution_timeout: 1800
  group_name: notebooks
  description: Execute monthly revenue analysis notebook
```

## Papermill parameterization

Papermill injects parameters by finding a cell in the notebook tagged `parameters` and inserting a new cell immediately after it with the overridden values.

### Tag the parameters cell

1. In JupyterLab: open the notebook, select the cell that defines your parameter defaults, and add the tag `parameters` via **View > Cell Toolbar > Tags**.
2. In Classic Notebook: enable the **Tags** cell toolbar and add `parameters` to the target cell.

The tagged cell typically looks like:

```python
# parameters
start_date = "2024-01-01"
end_date   = "2024-01-31"
output_schema = "dev"
```

When you run the component with `parameters: {start_date: "2024-02-01", end_date: "2024-02-29"}`, Papermill inserts an injected cell that reassigns only those variables — the rest of the notebook runs normally.

If no cell is tagged `parameters`, Papermill will still execute the notebook; the `parameters` dict you supply is simply ignored.

## Output notebook as artifact

By default the executed notebook is saved to:

```
{notebook_stem}_executed_{YYYYMMDD}.ipynb
```

in the same directory as the input notebook. Set `output_path` to override:

```yaml
output_path: "{{ project_root }}/outputs/revenue_{{ run_id }}.ipynb"
```

The output notebook contains all cell outputs — tracebacks, DataFrames, matplotlib figures, etc. — frozen at execution time. Tools like [nbconvert](https://nbconvert.readthedocs.io/) can convert it to HTML or PDF for sharing.

Set `store_output_ipynb: false` if you want to suppress the log message about the output path (the file is still written — Papermill requires an output destination).

## Kernel setup

| Field | Effect |
|---|---|
| `kernel_name: null` (default) | Uses the kernel name stored in the notebook's metadata |
| `kernel_name: "python3"` | Forces execution with the `python3` kernel |
| `kernel_name: "my_env"` | Uses a custom registered kernel |

Mismatched kernels are a common source of `KernelNotFoundError`. Run `jupyter kernelspec list` to see available kernels in your environment.

## Returning metadata from the notebook back to Dagster

Because Papermill executes the notebook in a separate kernel, values cannot be returned directly. The component supports an optional convention: if the **last cell** of the notebook prints or displays a Python dict named `dagster_metadata`, the component will parse it and merge those keys into the asset's materialization metadata.

Example last cell:

```python
dagster_metadata = {
    "rows_processed": len(df),
    "output_table": "analytics.monthly_revenue",
    "revenue_total": float(df["revenue"].sum()),
}
print(dagster_metadata)
```

These values will appear as asset metadata in the Dagster UI alongside the standard `notebook`, `output`, and `parameters` keys. Only the last cell is scanned; if parsing fails the component logs a warning and continues.

## Comparison with dagstermill

| Feature | `JupyterNotebookAssetComponent` | dagstermill |
|---|---|---|
| Parameterization | Papermill tagged cell | Papermill tagged cell |
| Setup complexity | Low — one YAML block | Medium — requires `define_dagstermill_asset` in Python |
| Dagster context inside notebook | Not available | Full `context` object injected |
| Pass data back to Dagster | Convention (last cell print) | `yield_result` / `yield_event` |
| Retry / resume | Re-executes entire notebook | Re-executes entire notebook |
| Best for | Scheduled reports, one-shot analyses | Notebooks that need Dagster resources or mid-notebook events |

Use `dagstermill` when notebooks need access to Dagster resources (e.g. database connections, I/O managers) or need to yield multiple events. Use this component when you want the simplest possible configuration with no Python boilerplate.

## Advanced examples

### Pass environment variables to the notebook

```yaml
env_vars:
  SNOWFLAKE_ACCOUNT: my_account.snowflakecomputing.com
  DBT_TARGET: production
```

Variables are set in the worker process before execution; they are available via `os.environ` inside the notebook.

### Run with a custom kernel and working directory

```yaml
kernel_name: pyspark_kernel
working_dir: "{{ project_root }}/spark_jobs"
execution_timeout: 3600
```

### Declare an upstream asset dependency

```yaml
asset_name: enriched_revenue_report
deps:
  - raw_revenue_events
  - dim_customers
notebook_path: "{{ project_root }}/notebooks/enrich_revenue.ipynb"
```

Dagster will not materialize `enriched_revenue_report` until both upstream assets are current.

## Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset key |
| `notebook_path` | `str` | required | Path to input `.ipynb` file |
| `output_path` | `str` | `None` | Path for executed notebook artifact |
| `parameters` | `dict` | `None` | Parameters injected into tagged cell |
| `kernel_name` | `str` | `None` | Kernel name; `None` uses notebook default |
| `execution_timeout` | `int` | `600` | Per-cell timeout in seconds (`-1` = unlimited) |
| `working_dir` | `str` | `None` | cwd for kernel process |
| `env_vars` | `dict[str, str]` | `None` | Extra environment variables |
| `store_output_ipynb` | `bool` | `true` | Log output notebook path to event log |
| `group_name` | `str` | `"notebooks"` | Dagster asset group |
| `description` | `str` | `None` | Asset description shown in the UI |
| `deps` | `list[str]` | `None` | Upstream asset keys |

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset key for this component. |
| `notebook_path` | `str` | Path to the input ``.ipynb`` file to execute. Supports ``{{ project_root }}`` template substitution. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `parameters` | `dict` | — | Parameters to inject into the notebook's tagged parameters cell. Keys and values are passed directly to ``papermill.execute_notebook``. Overrides any defaults set in the cell. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"notebooks"` | Dagster asset group name. |
| `description` | `str` | — | Human-readable description surfaced in the Dagster UI. |
| `deps` | `list[str]` | — | Upstream asset keys this asset depends on. |
| `owners` | `List[str]` | — | Asset owners — team names ('team:analytics') or email addresses. |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags applied to the asset in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}. |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `str` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `str` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `output_path` | `str` | — | Path where the executed notebook (with outputs) will be saved. When None, defaults to ``{notebook_stem}_executed_{date}.ipynb`` in the same directory as the input notebook. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `kernel_name` | `str` | — | Jupyter kernel to use for execution (e.g. ``'python3'``). When None, papermill uses the kernel recorded in the notebook metadata. |
| `execution_timeout` | `int` | `600` | Maximum number of seconds allowed per cell. Cells that exceed this limit raise a ``CellTimeoutError``. Set to ``-1`` to disable the per-cell timeout. |
| `working_dir` | `str` | — | Working directory for notebook execution. When None, the notebook runs in the directory that contains it. |
| `env_vars` | `dict[str, str]` | — | Environment variables made available to the notebook kernel via ``os.environ`` before execution begins. |
| `store_output_ipynb` | `bool` | `true` | Whether to persist the executed notebook. When True the output path is logged to the Dagster event log. When False the file is still written by papermill (it needs somewhere to record outputs) but you can treat it as ephemeral. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |

<!-- FIELDS:END -->
