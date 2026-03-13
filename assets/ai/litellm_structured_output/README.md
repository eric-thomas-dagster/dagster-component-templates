# LiteLLM Structured Output

Extract structured JSON data from text using LiteLLM's JSON mode. Expands extracted fields as new DataFrame columns.

## Overview

`LitellmStructuredOutputComponent` reads an upstream Dagster asset as a DataFrame, extracts structured fields from a text column using LiteLLM with JSON mode (`response_format={"type": "json_object"}`), and expands each extracted key as a new column in the output DataFrame.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | string | required | Output Dagster asset name |
| `upstream_asset_key` | string | required | Upstream asset key providing a DataFrame |
| `text_column` | string | required | Column containing input text |
| `schema_definition` | object | required | JSON Schema describing fields to extract |
| `model` | string | `"gpt-4o-mini"` | LiteLLM model string |
| `prompt_prefix` | string | null | Instruction prepended to each extraction request |
| `output_prefix` | string | `""` | Prefix for extracted column names |
| `on_error` | enum | `"null"` | Error handling: `skip`, `null`, or `raise` |
| `api_key_env_var` | string | null | Env var name for API key |
| `group_name` | string | null | Dagster asset group name |

## Example

```yaml
type: dagster_component_templates.LitellmStructuredOutputComponent
attributes:
  asset_name: extracted_contact_info
  upstream_asset_key: raw_emails
  text_column: email_body
  schema_definition:
    name: {type: string}
    company: {type: string}
    phone: {type: string}
  output_prefix: "extracted_"
  api_key_env_var: OPENAI_API_KEY
```

## Requirements

```
litellm>=1.0.0
pandas>=1.5.0
```
