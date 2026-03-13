# Schema Fit Component

Use an LLM to automatically map a source DataFrame's columns to a target schema, applying direct copies, concatenation, splitting, formatting, or computed transformations.

## Overview

The Schema Fit Component removes the tedium of writing manual column mapping code. You describe your target schema as a dictionary of `{column_name: description}`, and the LLM figures out how to map source columns to each target, applying the appropriate transformation. This is especially powerful for integrating data from systems with different naming conventions.

## Features

- **LLM-driven Column Mapping**: No hand-coded mapping required
- **Five Transformation Types**: direct, concat, split, format, compute
- **Automatic Gap Filling**: Missing target columns become `None` instead of crashing
- **Any litellm Model**: OpenAI, Anthropic, Azure, Cohere, etc.
- **Configurable Sample Context**: Control how many rows the LLM sees

## Use Cases

1. **CRM Integration**: Map legacy CRM exports to canonical schemas
2. **API Normalization**: Standardize responses from different vendors
3. **Data Migration**: Transform source tables to new warehouse schemas
4. **ETL Pipelines**: Automate schema alignment across data sources

## Prerequisites

- `litellm>=1.0.0`, `pandas>=1.5.0`
- API key for your chosen model provider

## Configuration

### Basic Schema Mapping

```yaml
type: dagster_component_templates.SchemaFitComponent
attributes:
  asset_name: normalized_customers
  upstream_asset_key: raw_crm_data
  target_schema:
    customer_id: "unique customer identifier"
    full_name: "first and last name combined"
    email: "customer email address"
    signup_date: "account creation date as ISO 8601"
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  sample_rows: 3
```

### With Anthropic

```yaml
type: dagster_component_templates.SchemaFitComponent
attributes:
  asset_name: mapped_orders
  upstream_asset_key: raw_orders
  target_schema:
    order_id: "unique order identifier"
    order_date: "order date as ISO 8601"
    customer_id: "customer who placed the order"
    total_usd: "order total in US dollars"
    status: "current order status"
  model: claude-3-haiku-20240307
  api_key_env_var: ANTHROPIC_API_KEY
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `target_schema` | dict | required | `{column: description}` target definition |
| `model` | string | `gpt-4o-mini` | litellm model name |
| `api_key_env_var` | string | `OPENAI_API_KEY` | Env var for API key |
| `sample_rows` | int | `3` | Sample rows sent to LLM |
| `batch_size` | int | `10` | Processing batch size |
| `group_name` | string | `None` | Asset group |

## Transformation Types

| Type | Description | Example Expression |
|------|-------------|-------------------|
| `direct` | Copy column as-is | (no expression needed) |
| `concat` | Combine columns | `col1 + ' ' + col2` |
| `split` | Extract part of column | `col.str.split()[0]` |
| `format` | Type conversion | `pd.to_datetime(col)` |
| `compute` | Arithmetic expression | `col * 100` |

## Troubleshooting

- **LLM JSON parse error**: The component strips markdown fences automatically; if still failing, try a more capable model
- **Missing target columns**: Any column the LLM doesn't map is filled with `None`
- **Incorrect mapping**: Add more descriptive descriptions in `target_schema` to guide the LLM
- **ImportError**: Run `pip install litellm`
