# Purchase Order Extractor Component

Extract structured data from purchase orders using an LLM via litellm. Supports any OpenAI-compatible model and returns extracted data as additional DataFrame columns.

## Overview

The Purchase Order Extractor Component uses a large language model to parse unstructured PO text and extract key fields such as PO number, vendor, line items, totals, and delivery dates. It works with any model accessible through litellm (OpenAI, Anthropic, Azure, etc.).

## Features

- **Flexible Field Extraction**: Configure exactly which fields to extract
- **litellm Integration**: Works with OpenAI, Anthropic, Azure, Cohere, and 100+ providers
- **Batch Processing**: Configurable batch size for throughput control
- **Null-safe**: Missing fields become `None` instead of raising errors
- **Text or File Input**: Pass raw text or file paths

## Use Cases

1. **Procurement Automation**: Extract PO data for ERP system entry
2. **Spend Analytics**: Aggregate spend by vendor or category
3. **Supplier Management**: Track delivery dates and payment terms
4. **Invoice Matching**: Extract PO fields for three-way match workflows

## Prerequisites

- `litellm>=1.0.0`, `pandas>=1.5.0`
- API key for your chosen model provider (set as an environment variable)

## Configuration

### Basic Purchase Order Extraction

```yaml
type: dagster_component_templates.PurchaseOrderExtractorComponent
attributes:
  asset_name: parsed_purchase_orders
  upstream_asset_key: raw_purchase_orders
  input_column: text
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  output_fields:
    - po_number
    - vendor
    - issue_date
    - total
    - line_items
```

### Full Extraction with File Input

```yaml
type: dagster_component_templates.PurchaseOrderExtractorComponent
attributes:
  asset_name: parsed_purchase_orders
  upstream_asset_key: po_file_list
  input_column: file_path
  input_type: file
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  output_fields:
    - po_number
    - vendor
    - buyer
    - issue_date
    - delivery_date
    - line_items
    - subtotal
    - tax
    - total
    - payment_terms
    - shipping_address
    - billing_address
  batch_size: 3
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `input_column` | string | `text` | Column with PO content |
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
