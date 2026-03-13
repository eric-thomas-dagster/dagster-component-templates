# synthetic_data

Generate synthetic DataFrame rows using an LLM based on a declarative schema. Rows are generated in batches and concatenated into a single DataFrame. Useful for creating test fixtures, populating demo environments, or augmenting training data. Optionally provide an upstream seed data asset for few-shot examples.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `schema` | `dict` | required | Column definitions e.g. `{name: "full name", age: "integer 18-80"}` |
| `n_rows` | `int` | `100` | Total rows to generate |
| `model` | `str` | `"gpt-4o-mini"` | LLM model identifier (via litellm) |
| `api_key_env_var` | `str` | `"OPENAI_API_KEY"` | Env var name for the API key |
| `context` | `Optional[str]` | `null` | Business context to guide generation |
| `batch_size` | `int` | `25` | Rows per LLM call |
| `seed_data_asset_key` | `Optional[str]` | `null` | Upstream asset key for few-shot seed rows |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Schema Format

The `schema` field is a dictionary where keys are column names and values are plain-English descriptions:

```yaml
schema:
  customer_id: "UUID string"
  age: "integer between 18 and 75"
  revenue: "float between 0 and 10000"
  country: "country name"
  tier: "one of: free, basic, pro, enterprise"
```

## Example YAML

```yaml
component_type: synthetic_data
description: Generate 500 synthetic e-commerce customer records.

asset_name: synthetic_customers
schema:
  full_name: "realistic full name"
  email: "valid email address"
  age: "integer between 18 and 75"
  total_orders: "integer between 0 and 200"
  subscription_tier: "one of: free, basic, pro, enterprise"
n_rows: 500
context: e-commerce SaaS platform customers
batch_size: 25
group_name: synthetic_datasets
```

## Metadata Logged

- `rows_generated` — Actual rows produced
- `columns` — Number of columns in the output
- `schema_fields` — Number of schema fields defined
- `llm_model` — Model used for generation
- `batches` — Number of LLM calls made

## Requirements

- `dagster`
- `pandas`
- `litellm`
