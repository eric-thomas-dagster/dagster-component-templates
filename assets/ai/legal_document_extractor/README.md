# Legal Document Extractor Component

Extract key information from legal documents (NDAs, agreements, pleadings, briefs) using an LLM via litellm. Supports any OpenAI-compatible model and returns extracted data as additional DataFrame columns.

## Overview

The Legal Document Extractor Component uses a large language model to parse unstructured legal document text and extract key fields such as document type, jurisdiction, parties, case number, filing dates, obligations, and penalties. It works with any model accessible through litellm (OpenAI, Anthropic, Azure, etc.).

## Features

- **Flexible Field Extraction**: Configure exactly which fields to extract
- **litellm Integration**: Works with OpenAI, Anthropic, Azure, Cohere, and 100+ providers
- **Batch Processing**: Configurable batch size for throughput control
- **Null-safe**: Missing fields become `None` instead of raising errors
- **Text or File Input**: Pass raw text or file paths

## Use Cases

1. **Legal Due Diligence Automation**: Rapidly extract key fields from large document sets
2. **Litigation Support Pipelines**: Organize case information for legal teams
3. **Compliance Monitoring**: Track obligations and penalty clauses
4. **Contract Repository Enrichment**: Add structured metadata to document management systems

## Prerequisites

- `litellm>=1.0.0`, `pandas>=1.5.0`
- API key for your chosen model provider (set as an environment variable)

## Configuration

### Basic Legal Document Extraction

```yaml
type: dagster_component_templates.LegalDocumentExtractorComponent
attributes:
  asset_name: parsed_legal_documents
  upstream_asset_key: raw_legal_documents
  input_column: text
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  output_fields:
    - document_type
    - parties
    - jurisdiction
    - filing_date
    - obligations
```

### Full Extraction with Anthropic

```yaml
type: dagster_component_templates.LegalDocumentExtractorComponent
attributes:
  asset_name: parsed_legal_documents
  upstream_asset_key: raw_legal_documents
  input_column: text
  model: claude-3-5-sonnet-20241022
  api_key_env_var: ANTHROPIC_API_KEY
  output_fields:
    - document_type
    - jurisdiction
    - court
    - case_number
    - parties
    - filing_date
    - key_dates
    - relief_sought
    - defined_terms
    - obligations
    - penalties
  batch_size: 3
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `input_column` | string | `text` | Column with document content |
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
