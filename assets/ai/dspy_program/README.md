# DSPy Program

Run a compiled DSPy program against a DataFrame column. DSPy optimizes prompts automatically through signature-based programming.

## Overview

`DspyProgramComponent` configures a DSPy LM, instantiates the chosen program type (Predict, ChainOfThought, ReAct, etc.) with the provided signature, and runs `.forward()` on each row. All output fields declared in the signature are written as new DataFrame columns.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | string | required | Output Dagster asset name |
| `upstream_asset_key` | string | required | Upstream asset key providing a DataFrame |
| `text_column` | string | required | Column containing input text |
| `output_column` | string | `"dspy_output"` | Primary output column name |
| `program_type` | enum | `"chain_of_thought"` | predict, chain_of_thought, react, multi_chain_comparison |
| `signature` | string | required | DSPy signature e.g. `"question -> answer"` |
| `model` | string | `"gpt-4o-mini"` | LLM model string |
| `api_key_env_var` | string | null | Env var name for API key |
| `group_name` | string | null | Dagster asset group name |

## Example

```yaml
type: dagster_component_templates.DspyProgramComponent
attributes:
  asset_name: summarized_articles
  upstream_asset_key: raw_articles
  text_column: body
  signature: "document -> summary, key_points"
  program_type: chain_of_thought
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
```

## Requirements

```
dspy-ai>=2.0.0
pandas>=1.5.0
```
