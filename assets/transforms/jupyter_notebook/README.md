# JupyterNotebookComponent

Execute a Jupyter notebook (`.ipynb`) or inline Python code against an
upstream DataFrame and return the result as a Dagster asset.

## Backends

| `backend` | When to use | Pros | Cons |
|---|---|---|---|
| `exec` (default) | Inline Python script body | Pure Python, no extra deps. Fast. | No cell-level outputs; not a "real" notebook execution. |
| `papermill` | "Notebook-as-pipeline-step" pattern with executed outputs saved for inspection | True notebook execution. Captures cell outputs / errors per cell. | Needs `pip install papermill` + parquet handoff via temp files. |

## Use Cases

- Drop an existing data-science notebook into a Dagster pipeline without
  refactoring it into per-cell assets.
- Keep notebook-first dev loops but materialize / schedule them with
  full lineage.
- Port a script-style code block that lives in YAML.

## Code Contract

Both backends expect:

- Input DataFrame available as `df` in the execution scope.
- Output DataFrame assigned to `out_df`.

For `papermill` backend, the notebook reads / writes parquet via
papermill parameters (`input_dataframe_path` / `output_dataframe_path`).
Your notebook's parameters cell should look like:

```python
# papermill parameters cell
input_dataframe_path = None    # injected
output_dataframe_path = None   # injected
```

Then somewhere in the notebook:

```python
import pandas as pd
df = pd.read_parquet(input_dataframe_path)
# ... your transforms ...
out_df.to_parquet(output_dataframe_path)
```

## Example

```yaml
type: dagster_community_components.JupyterNotebookComponent
attributes:
  asset_name: enriched_orders
  upstream_asset_key: raw_orders
  backend: exec
  code: |
    out_df = df.copy()
    out_df["order_total"] = out_df["price"] * out_df["quantity"]
    out_df = out_df[out_df["order_total"] > 50]
  group_name: ml_pipeline
```

## Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `asset_name` | yes | — | Output Dagster asset name |
| `upstream_asset_key` | yes | — | Upstream DataFrame asset key |
| `notebook_path` | one-of | `None` | Path to `.ipynb` (mutually exclusive with `code`) |
| `code` | one-of | `None` | Inline Python body (mutually exclusive with `notebook_path`) |
| `backend` | no | `exec` | `exec` (in-process inline) or `papermill` (notebook execution) |
| `output_notebook_path` | no | `None` | Where to save the executed notebook (papermill only) |
| `parameters` | no | `None` | Extra papermill parameters dict |
| `timeout_seconds` | no | `600` | Max execution time (papermill backend) |
| `group_name` | no | `None` | Dagster asset group name |

## Notes

- Use `exec` for short inline transforms (faster, no extra deps).
- Prefer `papermill` + a real `.ipynb` you can version-control for
  bigger notebook-first workflows.
- See [`dagstermill`](https://docs.dagster.io/integrations/libraries/jupyter)
  for the full Dagster-native notebook integration if you need
  cell-level outputs as asset metadata.
