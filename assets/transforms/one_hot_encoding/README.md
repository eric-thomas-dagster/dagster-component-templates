# One-Hot Encoding Component

Convert categorical columns into binary indicator (dummy) columns. Wraps pandas `get_dummies` with options for collinearity handling, NaN encoding, rare-category bucketing, and configurable output dtype.

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default `FilesystemIOManager` handles serialization automatically.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `columns` | `List[str]` | required | Categorical columns to encode |
| `drop_first` | `bool` | `false` | Drop the first level per column (avoid multicollinearity) |
| `dummy_na` | `bool` | `false` | Add an explicit `<col>_nan` indicator for NaN rows |
| `prefix_sep` | `str` | `"_"` | Separator between column name and value, e.g. `color_red` |
| `prefix` | `Dict[str, str]` | `null` | Per-column prefix override, e.g. `{"state": "st"}` → `st_CA` |
| `max_categories` | `Optional[int]` | `null` | Cap categories per column; rare values collapse to `__other__` |
| `keep_original` | `bool` | `false` | Retain the source categorical columns alongside the dummies |
| `dtype` | `str` | `"int"` | Output dtype: `int` (int8), `bool`, or `float` (float32) |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.OneHotEncodingComponent
attributes:
  asset_name: encoded_customer_features
  upstream_asset_key: cleaned_customer_features
  columns:
    - country
    - subscription_tier
    - acquisition_channel
  drop_first: true
  max_categories: 20
  dtype: int
  group_name: feature_engineering
```

## How It Works

For each column listed in `columns`:

1. If `max_categories=N` is set, the top N most frequent values are kept and the remainder are replaced with the literal string `__other__` before encoding.
2. `pandas.get_dummies` expands the column into one indicator per remaining value (or N − 1 indicators when `drop_first=true`).
3. Each indicator column is named `<prefix><prefix_sep><value>` — e.g. `country_US`, `country_CA`. Override the prefix per-column with the `prefix` field.
4. Indicator columns are appended to the DataFrame. The original columns are dropped unless `keep_original=true`.

## Choosing `drop_first`

- **Linear models, regressions:** `drop_first: true` — avoids the dummy-variable trap (perfect multicollinearity).
- **Tree-based models, neural nets:** `drop_first: false` — keep all levels; redundancy doesn't hurt and interpretability is better.

## Handling NaN

- `dummy_na: false` (default): rows with NaN in a source column produce all-zero dummies for that column.
- `dummy_na: true`: an extra `<col>_nan` indicator is added.

## Rare-Category Bucketing

Setting `max_categories` is recommended whenever a column has a long tail of rare values (free-text-ish fields, IDs, etc.). Without it, each unique value becomes its own column, which explodes dimensionality and produces sparse, unstable features. Rows whose value isn't in the top N are reassigned to `__other__` before encoding, so they all share one indicator column.

## Output Metadata

The asset emits these metadata entries on every materialization:

- `dagster/row_count` — output row count
- `dagster/column_schema` — full output schema
- `encoded_columns` — JSON map from each source column to the list of indicator columns it produced
- `dagster/column_lineage` — auto-derived: passthrough columns map 1:1, dummy columns map to their source categorical column

A `column_schema_change_check` asset check is also generated, so schema drift between runs is surfaced automatically.
