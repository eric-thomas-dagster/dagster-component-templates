# precision_match

Use an LLM to fuzzy-match varied string values in a DataFrame column to a canonical list. Values are batched and sent to the model, which returns a mapping and optional confidence score. Useful for standardizing free-text fields like product categories, country names, job titles, or industry labels against a controlled vocabulary.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset providing values to standardize |
| `column` | `str` | required | Column with varied strings to match |
| `reference_asset_key` | `Optional[str]` | `null` | Upstream asset key with canonical values DataFrame |
| `reference_column` | `Optional[str]` | `null` | Column in reference DataFrame with canonical values |
| `reference_values` | `Optional[List[str]]` | `null` | Explicit list of canonical values |
| `model` | `str` | `"gpt-4o-mini"` | LLM model identifier (via litellm) |
| `api_key_env_var` | `str` | `"OPENAI_API_KEY"` | Env var name for the API key |
| `output_column` | `str` | `"matched_value"` | Column name for matched canonical value |
| `confidence_column` | `Optional[str]` | `"match_confidence"` | Column for confidence score (null to skip) |
| `batch_size` | `int` | `20` | Unique values per LLM call |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Reference Values

Provide canonical values in one of two ways:
- **`reference_asset_key` + `reference_column`**: Load from an upstream Dagster asset (adds a second `ins` dependency).
- **`reference_values`**: Provide a static list inline in the YAML.

## Example YAML

```yaml
component_type: precision_match
description: Standardize product categories against a canonical taxonomy.

asset_name: crm_categories_standardized
upstream_asset_key: crm_products_raw
column: product_category
reference_asset_key: canonical_product_taxonomy
reference_column: category_name
output_column: matched_category
confidence_column: match_confidence
batch_size: 20
group_name: data_standardization
```

## Metadata Logged

- `unique_input_values` — Distinct values matched
- `canonical_values` — Number of canonical values in reference
- `matched_rows` — Rows with a successful match
- `match_rate` — Fraction of rows matched
- `llm_model` — Model used for matching

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `litellm`
