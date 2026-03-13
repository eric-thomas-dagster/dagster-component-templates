# Job Posting Extractor Component

Extract structured data from job postings and descriptions using an LLM via litellm. Supports any OpenAI-compatible model and returns extracted data as additional DataFrame columns.

## Overview

The Job Posting Extractor Component uses a large language model to parse unstructured job posting text and extract key fields such as job title, company, location, salary range, required skills, and responsibilities. It works with any model accessible through litellm (OpenAI, Anthropic, Azure, etc.).

## Features

- **Flexible Field Extraction**: Configure exactly which fields to extract
- **litellm Integration**: Works with OpenAI, Anthropic, Azure, Cohere, and 100+ providers
- **Batch Processing**: Configurable batch size for throughput control
- **Null-safe**: Missing fields become `None` instead of raising errors
- **Text or File Input**: Pass raw text or file paths

## Use Cases

1. **Job Market Intelligence**: Track hiring trends and skill demand
2. **Talent Sourcing Automation**: Match candidates to job requirements
3. **Compensation Benchmarking**: Aggregate salary ranges by role and location
4. **Skills Demand Analysis**: Identify emerging required skills over time

## Prerequisites

- `litellm>=1.0.0`, `pandas>=1.5.0`
- API key for your chosen model provider (set as an environment variable)

## Configuration

### Basic Job Posting Extraction

```yaml
type: dagster_component_templates.JobPostingExtractorComponent
attributes:
  asset_name: parsed_job_postings
  upstream_asset_key: raw_job_postings
  input_column: text
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  output_fields:
    - job_title
    - company
    - location
    - salary_range
    - required_skills
    - employment_type
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `input_column` | string | `text` | Column with posting content |
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
