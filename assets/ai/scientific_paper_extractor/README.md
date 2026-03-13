# Scientific Paper Extractor Component

Extract metadata and findings from scientific papers and research articles using an LLM via litellm. Supports any OpenAI-compatible model and returns extracted data as additional DataFrame columns.

## Overview

The Scientific Paper Extractor Component uses a large language model to parse unstructured academic paper text and extract key fields such as title, authors, DOI, abstract, methodology, key findings, and limitations. It works with any model accessible through litellm (OpenAI, Anthropic, Azure, etc.).

## Features

- **Flexible Field Extraction**: Configure exactly which fields to extract
- **litellm Integration**: Works with OpenAI, Anthropic, Azure, Cohere, and 100+ providers
- **Batch Processing**: Configurable batch size for throughput control
- **Null-safe**: Missing fields become `None` instead of raising errors
- **Text or File Input**: Pass raw text or file paths

## Use Cases

1. **Literature Review Automation**: Extract metadata from hundreds of papers rapidly
2. **Research Knowledge Graphs**: Build structured nodes from paper content
3. **Citation Analysis Pipelines**: Extract DOIs and author lists for network analysis
4. **Systematic Review Workflows**: Standardize data extraction across reviewers

## Prerequisites

- `litellm>=1.0.0`, `pandas>=1.5.0`
- API key for your chosen model provider (set as an environment variable)

## Configuration

### Basic Scientific Paper Extraction

```yaml
type: dagster_component_templates.ScientificPaperExtractorComponent
attributes:
  asset_name: parsed_scientific_papers
  upstream_asset_key: raw_scientific_papers
  input_column: text
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  output_fields:
    - title
    - authors
    - journal
    - doi
    - abstract
    - key_findings
```

### Full Extraction with Anthropic

```yaml
type: dagster_component_templates.ScientificPaperExtractorComponent
attributes:
  asset_name: parsed_scientific_papers
  upstream_asset_key: raw_scientific_papers
  input_column: text
  model: claude-3-5-sonnet-20241022
  api_key_env_var: ANTHROPIC_API_KEY
  output_fields:
    - title
    - authors
    - journal
    - publication_date
    - doi
    - abstract
    - keywords
    - methodology
    - key_findings
    - limitations
    - citations_count
    - data_availability
  batch_size: 3
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `input_column` | string | `text` | Column with paper content |
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
