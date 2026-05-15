# DataframeDescribeComponent

Profile every column in a DataFrame — pandas `df.describe()` exposed as a Dagster asset. Right for the first pass of data exploration, ongoing data-quality monitoring, or as a sibling check asset for schema/distribution drift detection.

## Use cases

- Auto-profile an upstream raw asset before downstream transforms
- Surface column-level statistics in the Dagster UI for review
- Diff today's profile against a baseline asset to detect drift
- Quick health check after an ingestion update

## Output shape

One row per source column, with stat columns depending on the dtype:

| Numeric column | Object / categorical |
|---|---|
| count, mean, std, min, percentiles..., max | count, unique, top, freq |

Plus optional enrichments:
- `dtype` — the pandas dtype of the source column (default on)
- `null_pct` — percentage of null values per source column (default on)

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | `str` | Yes | — | Output asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset providing a DataFrame |
| `include` | `str` | No | `"all"` | `"all"`, `"number"`, `"object"`, or `"datetime"` — which columns to profile |
| `percentiles` | `List[float]` | No | `[0.25, 0.5, 0.75]` | Percentiles to compute (values 0..1) |
| `include_dtypes_column` | `bool` | No | `True` | Add a `dtype` column to the output |
| `include_null_pct` | `bool` | No | `True` | Add a `null_pct` column to the output |
| `group_name` | `str` | No | — | Dagster asset group |

## Example

```yaml
type: dagster_component_templates.DataframeDescribeComponent
attributes:
  asset_name: orders_profile
  upstream_asset_key: orders
  include: number
  percentiles: [0.25, 0.5, 0.75, 0.95, 0.99]
  group_name: profiling
```

## See also

- `summarize` — group + aggregate (when you want sum/avg/max per group, not full profile)
- `count_records` — just row counts (single number or per-group)
- `schema_validator` — assert specific column expectations (paired well with describe to discover thresholds, then assert)
