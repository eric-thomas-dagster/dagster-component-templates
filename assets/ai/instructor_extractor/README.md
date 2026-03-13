# Instructor Extractor

Extract structured Pydantic models from text using the Instructor library. Works with any OpenAI-compatible LLM.

## Overview

`InstructorExtractorComponent` uses the Instructor library to reliably extract structured typed data from text. It dynamically creates a Pydantic model from the `extraction_schema` definition, uses `instructor.from_openai()` for guaranteed structured output with automatic retry, and expands extracted fields as new DataFrame columns.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | string | required | Output Dagster asset name |
| `upstream_asset_key` | string | required | Upstream asset key providing a DataFrame |
| `text_column` | string | required | Column containing input text |
| `model` | string | `"gpt-4o-mini"` | LLM model string |
| `extraction_schema` | object | required | Fields to extract with type and description |
| `output_prefix` | string | `""` | Prefix for extracted column names |
| `max_retries` | integer | `3` | Max retries for structured extraction |
| `api_key_env_var` | string | null | Env var name for API key |
| `group_name` | string | null | Dagster asset group name |

## Example

```yaml
type: dagster_component_templates.InstructorExtractorComponent
attributes:
  asset_name: extracted_invoice_data
  upstream_asset_key: raw_invoice_text
  text_column: invoice_text
  extraction_schema:
    vendor_name: {type: str, description: "Name of the vendor"}
    total_amount: {type: float, description: "Total amount in dollars"}
  output_prefix: "inv_"
  api_key_env_var: OPENAI_API_KEY
```

## Requirements

```
instructor>=0.4.0
openai>=1.0.0
pandas>=1.5.0
```
