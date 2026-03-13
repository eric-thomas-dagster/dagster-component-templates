# Receipt Extractor Component

Extract structured fields from retail receipts using an LLM via litellm. Supports any OpenAI-compatible model and returns extracted data as additional DataFrame columns.

## Overview

The Receipt Extractor Component uses a large language model to parse unstructured receipt text and extract key fields such as merchant name, date, line items, totals, and payment method. It works with any model accessible through litellm (OpenAI, Anthropic, Azure, etc.).

## Features

- **Flexible Field Extraction**: Configure exactly which fields to extract
- **litellm Integration**: Works with OpenAI, Anthropic, Azure, Cohere, and 100+ providers
- **Batch Processing**: Configurable batch size for throughput control
- **Null-safe**: Missing fields become `None` instead of raising errors
- **Text or File Input**: Pass raw text or file paths

## Use Cases

1. **Expense Management Automation**: Extract receipt data for expense systems
2. **Retail Analytics**: Aggregate spend by merchant or category
3. **Loyalty Program Integration**: Extract items and totals for reward calculation
4. **Audit Preparation**: Structure receipt data for compliance

## Prerequisites

- `litellm>=1.0.0`, `pandas>=1.5.0`
- API key for your chosen model provider (set as an environment variable)

## Configuration

### Basic Receipt Extraction

```yaml
type: dagster_component_templates.ReceiptExtractorComponent
attributes:
  asset_name: parsed_receipts
  upstream_asset_key: raw_receipt_text
  input_column: text
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  output_fields:
    - merchant_name
    - date
    - total
    - payment_method
```

### File Path Input with Anthropic

```yaml
type: dagster_component_templates.ReceiptExtractorComponent
attributes:
  asset_name: parsed_receipts
  upstream_asset_key: receipt_file_list
  input_column: file_path
  input_type: file
  model: claude-3-haiku-20240307
  api_key_env_var: ANTHROPIC_API_KEY
  output_fields:
    - merchant_name
    - merchant_address
    - date
    - time
    - items
    - subtotal
    - tax
    - total
    - payment_method
    - card_last_four
    - receipt_number
  batch_size: 3
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `input_column` | string | `text` | Column with receipt content |
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
