# LiteLLM Function Calling

Use LiteLLM function/tool calling to invoke structured tool definitions against each DataFrame row.

## Overview

`LitellmFunctionCallingComponent` processes each row of an upstream DataFrame through an LLM with tool/function definitions. The model decides which tool(s) to call and with what arguments. Results are serialized as JSON and written to the output column.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | string | required | Output Dagster asset name |
| `upstream_asset_key` | string | required | Upstream asset key providing a DataFrame |
| `text_column` | string | required | Column containing input text |
| `tools` | array | required | List of tool definitions in OpenAI format |
| `model` | string | `"gpt-4o-mini"` | LiteLLM model string |
| `output_column` | string | `"tool_calls"` | Column to write tool call results as JSON |
| `system_prompt` | string | null | System message prepended to each request |
| `api_key_env_var` | string | null | Env var name for API key |
| `group_name` | string | null | Dagster asset group name |

## Example

```yaml
type: dagster_component_templates.LitellmFunctionCallingComponent
attributes:
  asset_name: routed_user_queries
  upstream_asset_key: raw_user_queries
  text_column: query_text
  tools:
    - type: function
      function:
        name: search_products
        description: Search the product catalog
        parameters:
          type: object
          properties:
            query: {type: string}
          required: [query]
  api_key_env_var: OPENAI_API_KEY
```

## Requirements

```
litellm>=1.0.0
pandas>=1.5.0
```
