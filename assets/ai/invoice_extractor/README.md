# Invoice Extractor Component

Extract structured fields from invoice text using an LLM via litellm. Supports any OpenAI-compatible model and returns extracted data as additional DataFrame columns.

## Overview

The Invoice Extractor Component uses a large language model to parse unstructured invoice text and extract key fields such as invoice number, date, vendor name, total amount, and line items. It works with any model accessible through litellm (OpenAI, Anthropic, Azure, etc.).

## Features

- **Flexible Field Extraction**: Configure exactly which fields to extract
- **litellm Integration**: Works with OpenAI, Anthropic, Azure, Cohere, and 100+ providers
- **Batch Processing**: Configurable batch size for throughput control
- **Null-safe**: Missing fields become `None` instead of raising errors
- **Text or File Input**: Pass raw text or file paths

## Use Cases

1. **Accounts Payable Automation**: Extract invoice data for ERP systems
2. **Vendor Analysis**: Aggregate spend by vendor
3. **Audit Preparation**: Structure invoice data for compliance
4. **Purchase Order Matching**: Extract fields for PO reconciliation

## Prerequisites

- `litellm>=1.0.0`, `pandas>=1.5.0`
- API key for your chosen model provider (set as an environment variable)

## Configuration

### Basic Invoice Extraction

```yaml
type: dagster_component_templates.InvoiceExtractorComponent
attributes:
  asset_name: parsed_invoices
  upstream_asset_key: raw_invoice_text
  input_column: invoice_body
  model: gpt-4o
  api_key_env_var: OPENAI_API_KEY
  output_fields:
    - invoice_number
    - date
    - vendor
    - total_amount
    - currency
```

### Custom Fields with Anthropic

```yaml
type: dagster_component_templates.InvoiceExtractorComponent
attributes:
  asset_name: parsed_invoices
  upstream_asset_key: raw_invoice_text
  input_column: invoice_body
  model: claude-3-5-sonnet-20241022
  api_key_env_var: ANTHROPIC_API_KEY
  output_fields:
    - invoice_number
    - date
    - due_date
    - vendor_name
    - vendor_address
    - line_items
    - subtotal
    - tax
    - total_amount
    - payment_terms
    - currency
  batch_size: 3
```

### File Path Input

```yaml
type: dagster_component_templates.InvoiceExtractorComponent
attributes:
  asset_name: parsed_invoices
  upstream_asset_key: invoice_file_list
  input_column: file_path
  input_type: file
  model: gpt-4o
  api_key_env_var: OPENAI_API_KEY
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `input_column` | string | `invoice_text` | Column with invoice content |
| `input_type` | string | `text` | text or file |
| `model` | string | `gpt-4o` | litellm model name |
| `api_key_env_var` | string | `OPENAI_API_KEY` | Env var for API key |
| `output_fields` | list | see defaults | Fields to extract |
| `batch_size` | int | `5` | Invoices per LLM batch |
| `group_name` | string | `None` | Asset group |

## Output

The output DataFrame contains all original columns plus one column per extracted field.

## Troubleshooting

- **ImportError**: Run `pip install litellm`
- **API authentication errors**: Ensure env var is set correctly
- **JSON parse errors**: May occur if the model returns markdown fences; the component strips them automatically
- **Slow processing**: Reduce `batch_size` or switch to a faster model
