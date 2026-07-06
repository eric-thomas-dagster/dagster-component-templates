# Vanta Evidence Response Agent

Reads a DataFrame of Vanta controls (typically from `vanta_controls_ingestion`) and uses an LLM to draft evidence responses / control narratives per row.

Compliance auditors routinely ask "explain how you meet control X". This component drafts that narrative from the control description (and optional internal-context column, e.g. runbook excerpts, tool inventories). The output is a first draft — a human reviewer should still sign it off.

## Installation

```
pip install openai>=1.0.0 pandas>=1.5.0
```

For Anthropic (`llm_provider: anthropic`): `pip install anthropic`.

## Fields

### Core

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | (required) | Dagster asset name |
| `upstream_asset_key` | `str` | (required) | Upstream asset key providing a DataFrame of Vanta controls |
| `llm_provider` | `str` | `"openai"` | `"openai"` or `"anthropic"` |
| `model` | `str` | `"gpt-4o"` | Model name (provider-specific) |
| `api_key_env_var` | `str` | `"OPENAI_API_KEY"` | Env var with the LLM API key |
| `temperature` | `float` | `0.2` | Sampling temperature |
| `max_tokens` | `int` | `800` | Max tokens per response |
| `system_prompt` | `str` | (compliance analyst persona) | System prompt |

### Column mapping

| Field | Type | Default | Description |
|---|---|---|---|
| `control_id_column` | `str \| int` | `"control_id"` | Column holding the control identifier |
| `control_description_column` | `str \| int` | `"description"` | Column holding the control description |
| `context_column` | `str \| int \| None` | `None` | Optional column with internal context to ground the narrative |
| `response_column` | `str \| int` | `"draft_response"` | Column to write the draft into |

### Standard catalog

| Field | Type | Default | Description |
|---|---|---|---|
| `max_rows` | `int \| None` | `None` | Cap rows processed (testing / cost control) |
| `batch_log_every` | `int` | `10` | Log progress every N rows |
| `description` | `str \| None` | `None` | Asset description |
| `group_name` | `str` | `"vanta"` | Asset group |
| `owners` | `list[str] \| None` | `None` | Owners |
| `asset_tags` | `dict[str,str] \| None` | `None` | Catalog tags |
| `kinds` | `list[str]` | `["ai","vanta"]` | Asset kinds |
| `deps` | `list[str] \| None` | `None` | Lineage-only upstream keys |

## Configuration

```yaml
type: dagster_community_components.VantaEvidenceResponseAgentComponent
attributes:
  asset_name: vanta_control_narratives
  upstream_asset_key: vanta_soc2_controls
  llm_provider: openai
  model: gpt-4o
  api_key_env_var: OPENAI_API_KEY
  control_id_column: control_id
  control_description_column: description
  response_column: draft_response
```

## Pipeline pattern

```
vanta_resource (OAuth token)
   ↓
vanta_controls_ingestion   ->  DataFrame of controls
   ↓
vanta_evidence_response_agent  ->  DataFrame + draft_response column
```
