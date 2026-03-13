# Medical Record Extractor Component

Extract clinical data from medical records and clinical notes using an LLM via litellm. Supports any OpenAI-compatible model and returns extracted data as additional DataFrame columns.

## Overview

The Medical Record Extractor Component uses a large language model to parse unstructured clinical text and extract key fields such as diagnoses, medications, ICD codes, CPT codes, and follow-up instructions. It works with any model accessible through litellm (OpenAI, Anthropic, Azure, etc.).

## Features

- **Flexible Field Extraction**: Configure exactly which fields to extract
- **litellm Integration**: Works with OpenAI, Anthropic, Azure, Cohere, and 100+ providers
- **Batch Processing**: Configurable batch size for throughput control
- **Null-safe**: Missing fields become `None` instead of raising errors
- **Text or File Input**: Pass raw text or file paths

## Use Cases

1. **Clinical Data Warehousing**: Populate structured tables from free-text notes
2. **Population Health Analytics**: Aggregate diagnoses and medications at scale
3. **Coding Audit Automation**: Extract and verify ICD/CPT codes
4. **Care Gap Identification**: Surface missing follow-up or medication information

## Prerequisites

- `litellm>=1.0.0`, `pandas>=1.5.0`
- API key for your chosen model provider (set as an environment variable)

## Configuration

### Basic Medical Record Extraction

```yaml
type: dagster_component_templates.MedicalRecordExtractorComponent
attributes:
  asset_name: parsed_medical_records
  upstream_asset_key: raw_clinical_notes
  input_column: text
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  output_fields:
    - patient_name
    - visit_date
    - diagnoses
    - icd_codes
    - medications
```

### Full Extraction with File Input

```yaml
type: dagster_component_templates.MedicalRecordExtractorComponent
attributes:
  asset_name: parsed_medical_records
  upstream_asset_key: record_file_list
  input_column: file_path
  input_type: file
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  output_fields:
    - patient_name
    - dob
    - provider
    - visit_date
    - chief_complaint
    - diagnoses
    - icd_codes
    - medications
    - procedures
    - cpt_codes
    - follow_up
  batch_size: 3
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `input_column` | string | `text` | Column with record content |
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
