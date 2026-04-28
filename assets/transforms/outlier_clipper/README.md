# Outlier Clipper Component

Detect and handle outliers in numeric columns using IQR, z-score, or quantile thresholds. The matched rows can be clipped (winsorized), dropped, or flagged.

## Strategies

| Strategy | Outlier Definition | Best For |
|---|---|---|
| `iqr` | `x < Q1 − k·IQR` or `x > Q3 + k·IQR` (k = `iqr_multiplier`, default 1.5) | Robust default — distribution-free, resistant to a few extreme values |
| `zscore` | `\|z\| > zscore_threshold` (default 3.0) | Approximately Gaussian distributions |
| `quantile` | `x < quantile(lower_quantile)` or `x > quantile(upper_quantile)` | When you know the cut you want — e.g. drop top/bottom 1% |

## Actions

| Action | Effect |
|---|---|
| `clip` (default) | Replace outliers with the boundary value (winsorize). Row count unchanged. |
| `drop` | Remove rows where any target column is an outlier. |
| `flag` | Keep rows; add a boolean column `<col>_is_outlier` per checked column. |

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset providing a DataFrame |
| `strategy` | `str` | `"iqr"` | Detection strategy |
| `action` | `str` | `"clip"` | What to do with detected outliers |
| `columns` | `Optional[List[str]]` | `null` | Columns to check. Empty = all numeric columns |
| `iqr_multiplier` | `float` | `1.5` | IQR fence multiplier (use 3.0 for "extreme outliers only") |
| `zscore_threshold` | `float` | `3.0` | Absolute z-score threshold |
| `lower_quantile` | `float` | `0.01` | Lower quantile cut for `quantile` strategy |
| `upper_quantile` | `float` | `0.99` | Upper quantile cut for `quantile` strategy |

## Example YAML

```yaml
type: dagster_component_templates.OutlierClipperComponent
attributes:
  asset_name: cleaned_transactions
  upstream_asset_key: raw_transactions
  strategy: iqr
  iqr_multiplier: 1.5
  action: clip
  columns:
    - amount_usd
    - latency_ms
```

## Choosing an Action

- **`clip`** is the safest default for ML pipelines: it preserves row count and keeps the records but caps the influence of extreme values on downstream models.
- **`drop`** is appropriate when outliers represent invalid data (sensor errors, broken records). Be aware: dropping skews distributions and can mask upstream data-quality bugs — consider running with `flag` first to inspect what would be removed.
- **`flag`** is useful for analytics/observability. Pair with a downstream filter or a Dagster asset check to alert when the rate spikes.

## Output Metadata

Every materialization emits:

- `outlier_strategy` and `outlier_action` — the configuration used
- `outlier_bounds` — `{col: {lower, upper}}` thresholds applied
- `outlier_counts` — `{col: n_outliers}`
- `rows_in` / `rows_out` — useful with `action: drop`
- Standard `dagster/row_count`, `dagster/column_schema`, `dagster/column_lineage`

## Caveats

- For `zscore`, columns with zero standard deviation produce infinite bounds — no rows flagged. Logged but not failed.
- Bounds are computed from the **current** input each run. If you need fixed thresholds across runs, compute them once and use a `formula` component instead.
- NaN values are not treated as outliers; they pass through.
