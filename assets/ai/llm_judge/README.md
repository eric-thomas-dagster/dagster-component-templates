# LLM Judge

Evaluate and score LLM outputs against a rubric using another LLM as a judge. Useful for automated quality assessment and regression testing of AI pipelines.

## Overview

`LlmJudgeComponent` uses a strong LLM (e.g. GPT-4o) to evaluate responses in a DataFrame column against configurable criteria. It builds a structured evaluation prompt, asks the judge to score each criterion from 0-10, and writes the average score and reasoning to new columns. Supports optional reference/ground truth comparison.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | string | required | Output Dagster asset name |
| `upstream_asset_key` | string | required | Upstream asset key providing a DataFrame |
| `response_column` | string | required | Column containing LLM responses to evaluate |
| `reference_column` | string | null | Column containing reference/ground truth |
| `criteria` | list | required | Evaluation criteria list |
| `score_column` | string | `"judge_score"` | Column to write numeric score (0-10) |
| `reason_column` | string | `"judge_reason"` | Column to write explanation |
| `model` | string | `"gpt-4o"` | Judge model |
| `rubric` | string | null | Custom scoring rubric |
| `api_key_env_var` | string | null | Env var name for API key |
| `group_name` | string | null | Dagster asset group name |

## Example

```yaml
type: dagster_component_templates.LlmJudgeComponent
attributes:
  asset_name: evaluated_responses
  upstream_asset_key: llm_responses
  response_column: response
  reference_column: ground_truth
  criteria:
    - accuracy
    - clarity
    - completeness
  model: gpt-4o
  api_key_env_var: OPENAI_API_KEY
```

## Requirements

```
litellm>=1.0.0
pandas>=1.5.0
```
