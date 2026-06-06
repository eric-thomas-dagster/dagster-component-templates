# RScriptComponent

Run an R script against an upstream DataFrame and return the result as
a Dagster asset. Use this when you have existing statsmodels / dplyr /
tidyverse code you'd rather not port to Python.

## Backends

| `backend` | When to use | Pros | Cons |
|---|---|---|---|
| `rscript` (default) | Production / portable | No Python ↔ R version coupling; works in slim containers as long as R is installed. | Disk roundtrip via parquet / csv per run. |
| `rpy2` | Small / medium data, dev loops | In-process; fastest. | Requires `pip install rpy2` and a matching R install — fragile across OS / R versions. |

## Use Cases

- Port an R-based ETL or modeling step into a Dagster asset.
- Run dplyr / tidyverse cleaning that's already battle-tested in R.
- Use R packages with no good Python equivalent (e.g. `lme4` for mixed-effects).

## Script Contract

Your R script receives the upstream DataFrame as `df` and must assign
its result to `out_df`:

```r
out_df <- df %>%
  dplyr::filter(amount > 100) %>%
  dplyr::mutate(amount_log = log(amount))
```

The component reads `out_df` back, converts to pandas, and returns it
as the Dagster asset's value.

## Example

```yaml
type: dagster_community_components.RScriptComponent
attributes:
  asset_name: clean_transactions
  upstream_asset_key: raw_transactions
  backend: rscript
  script: |
    library(dplyr)
    out_df <- df %>%
      filter(amount > 100) %>%
      mutate(amount_log = log(amount))
  r_packages: [dplyr]
  intermediate_format: parquet
  group_name: data_quality
```

## Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `asset_name` | yes | — | Output Dagster asset name |
| `upstream_asset_key` | yes | — | Upstream DataFrame asset key |
| `script` | yes | — | R script body (reads `df`, must assign `out_df`) |
| `backend` | no | `rscript` | `rscript` (subprocess) or `rpy2` (in-process) |
| `rscript_executable` | no | `Rscript` | Path to the `Rscript` binary |
| `intermediate_format` | no | `parquet` | DataFrame handoff format (`parquet` requires R `arrow` pkg; `csv` is the safest fallback) |
| `r_packages` | no | `None` | R packages to ensure are installed before the script runs |
| `timeout_seconds` | no | `300` | Max subprocess runtime (rscript backend only) |
| `group_name` | no | `None` | Dagster asset group name |

## Notes

- R packages requested via `r_packages` get `install.packages()`-ed on
  first run if not already present. For production, pre-install them in
  your worker image to avoid network roundtrips per materialization.
- The `arrow` R package is required when `intermediate_format='parquet'`;
  the wrapper auto-installs it if missing.
- `out_df` must be a `data.frame` (or coercible to one).
