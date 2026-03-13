# Shipping Label Extractor Component

Extract logistics data from shipping labels and packing slips using an LLM via litellm. Supports any OpenAI-compatible model and returns extracted data as additional DataFrame columns.

## Overview

The Shipping Label Extractor Component uses a large language model to parse unstructured shipping label text and extract key fields such as tracking number, carrier, sender/recipient addresses, weight, and estimated delivery. It works with any model accessible through litellm (OpenAI, Anthropic, Azure, etc.).

## Features

- **Flexible Field Extraction**: Configure exactly which fields to extract
- **litellm Integration**: Works with OpenAI, Anthropic, Azure, Cohere, and 100+ providers
- **Batch Processing**: Configurable batch size for throughput control
- **Null-safe**: Missing fields become `None` instead of raising errors
- **Text or File Input**: Pass raw text or file paths

## Use Cases

1. **Logistics Data Automation**: Extract label data for shipment tracking systems
2. **Shipment Tracking Enrichment**: Add structured fields to tracking pipelines
3. **Last-Mile Delivery Analytics**: Analyze carrier and delivery performance
4. **Returns Processing**: Extract sender information from return labels

## Prerequisites

- `litellm>=1.0.0`, `pandas>=1.5.0`
- API key for your chosen model provider (set as an environment variable)

## Configuration

### Basic Shipping Label Extraction

```yaml
type: dagster_component_templates.ShippingLabelExtractorComponent
attributes:
  asset_name: parsed_shipping_labels
  upstream_asset_key: raw_shipping_labels
  input_column: text
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  output_fields:
    - tracking_number
    - carrier
    - recipient_name
    - recipient_address
    - ship_date
    - estimated_delivery
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `input_column` | string | `text` | Column with label content |
| `input_type` | string | `text` | text or file |
| `model` | string | `gpt-4o-mini` | litellm model name |
| `api_key_env_var` | string | `OPENAI_API_KEY` | Env var for API key |
| `output_fields` | list | see defaults | Fields to extract |
| `batch_size` | int | `5` | Rows per LLM batch |
| `group_name` | string | `None` | Asset group |

## Output

The output DataFrame contains all original columns plus one column per extracted field.

## Troubleshooting

- **ImportError**: Run `pip install litellm`
- **API authentication errors**: Ensure env var is set correctly
- **JSON parse errors**: The component uses `response_format: json_object` to minimize parse failures
- **Slow processing**: Reduce `batch_size` or switch to a faster model
