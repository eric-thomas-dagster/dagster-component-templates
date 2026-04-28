# Label Encoder Component

Encode categorical columns into integer codes. Each unique value in a column becomes a non-negative integer; NaN values map to a configurable sentinel (default `-1`).

## When to Use Label Encoding vs One-Hot Encoding

| Situation | Use |
|---|---|
| Tree-based models (XGBoost, LightGBM, RandomForest) | `label_encoder` — no need for one-hot expansion |
| Linear models, neural networks, distance-based models | `one_hot_encoding` — avoid implying false ordinal relationships |
| Very high-cardinality column (1000+ unique values) | `label_encoder` (or `one_hot_encoding` with `max_categories`) |
| Truly ordinal data (e.g. `low`/`medium`/`high`) | `label_encoder` with `ordering: alphabetical` and curated values |

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset providing a DataFrame |
| `columns` | `List[str]` | required | Columns to encode |
| `ordering` | `str` | `"frequency"` | Code-assignment order (see below) |
| `na_code` | `int` | `-1` | Code assigned to NaN values |
| `suffix` | `Optional[str]` | `null` | Output column suffix; empty = overwrite |
| `keep_original` | `bool` | `false` | Retain original columns (auto-sets `suffix="_code"` if unset) |

## Ordering Modes

| Mode | Code Assignment | Use When |
|---|---|---|
| `frequency` | Most-frequent value → `0`, next → `1`, ... | Default — keeps low codes on common values, useful for memory-mapped lookups |
| `alphabetical` | Sorted unique values | Stability across runs is critical, or values have natural lexical ordering |
| `appearance` | First-seen order in the input DataFrame | You want codes to match document/event order |

## Example YAML

```yaml
type: dagster_component_templates.LabelEncoderComponent
attributes:
  asset_name: encoded_lookup_features
  upstream_asset_key: cleaned_lookup_features
  columns:
    - region
    - product_category
  ordering: frequency
  suffix: _code
  keep_original: true
```

## Caveats

- **Encoding stability**: with `frequency` or `appearance` ordering, codes can shift between runs if the input distribution changes. Use `alphabetical` if you need stable codes across materializations (or persist the mapping yourself).
- **Train/serve skew**: if you train a model on these codes, the same encoding must be applied at inference. Consider materializing the mapping table as a separate asset — the component emits it under `encoding_mappings` metadata for now.
- **NaN sentinel**: `na_code: -1` flags missing values explicitly. Use `na_code: 0` to merge them with the most-frequent category, but be aware this loses information.
