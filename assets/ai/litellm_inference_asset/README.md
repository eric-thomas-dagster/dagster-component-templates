# LiteLLM Inference Asset

> **🔑 API key required.** This component calls an LLM provider. Set `OPENAI_API_KEY` for OpenAI (default), or configure an alternate provider (Anthropic / Azure OpenAI / Ollama / etc.) via the component's `provider`, `model`, and `api_key_env_var` fields. See the schema for the exact field names this component exposes.

A Dagster component that reads rows from an upstream asset, enriches each row by running it through a configurable LLM prompt, and returns the enriched DataFrame as the asset value.

Powered by [LiteLLM](https://github.com/BerriAI/litellm), this component gives you a single unified interface to **100+ LLM providers** — including OpenAI, Anthropic, Azure OpenAI, AWS Bedrock, Google Gemini, Mistral, Cohere, Groq, Together AI, and many more. Switching providers requires changing only the `model` string and `api_key_env_var`; your prompt logic stays the same.

---

## What It Does

1. Loads a DataFrame from the upstream Dagster asset (`upstream_asset_key`).
2. For each row, formats your `prompt_template` string using the row's column values.
3. Sends the formatted prompt (plus an optional `system_prompt`) to the configured LLM via LiteLLM.
4. Appends the LLM response to a new column (`response_column`) in the DataFrame.
5. Returns the enriched DataFrame as the asset value (persist it with an IO manager or a downstream sink).
6. Emits Dagster materialization metadata: row count, model used, response column.

---

## Supported Providers (via LiteLLM)

LiteLLM routes to 100+ providers using a unified `model` string:

| Provider         | Example model string                          |
|------------------|-----------------------------------------------|
| OpenAI           | `gpt-4o`, `gpt-4o-mini`, `o1-preview`        |
| Anthropic        | `claude-3-5-sonnet-20241022`, `claude-3-haiku-20240307` |
| Azure OpenAI     | `azure/my-deployment-name`                    |
| AWS Bedrock      | `bedrock/anthropic.claude-3-sonnet-20240229`  |
| Google Gemini    | `gemini/gemini-1.5-pro`                       |
| Mistral          | `mistral/mistral-large-latest`                |
| Cohere           | `cohere/command-r-plus`                       |
| Groq             | `groq/llama-3.1-70b-versatile`               |
| Together AI      | `together_ai/Qwen/Qwen3.5-9B` — **note:** `api_key_env_var` must be `TOGETHERAI_API_KEY` (no underscore between TOGETHER and AI) |
| Cerebras         | `cerebras/llama3-70b-instruct`                |
| Fireworks AI     | `fireworks_ai/accounts/fireworks/models/kimi-k3` |
| Ollama (local)   | `ollama/llama3.2`                             |

See the [LiteLLM provider docs](https://docs.litellm.ai/docs/providers) for the full list. All of these (including fast-inference providers like Cerebras/Fireworks/Groq/Together) work today with zero code changes — just a `model` string and matching `api_key_env_var`.

---

## Required Packages

```
litellm>=1.30.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

Install with:

```bash
pip install litellm pandas sqlalchemy
```

You will also need any provider-specific SDK your chosen model requires (e.g. `boto3` for Bedrock, `google-cloud-aiplatform` for Vertex AI). LiteLLM will raise a clear error if a dependency is missing.

---

## Component Fields

### Source

| Field | Type | Required | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` or list | Yes | Dagster asset key to load as a DataFrame (e.g. `raw_orders` or `schema/table`) |

### LLM Configuration

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `model` | `str` | Yes* | — | LiteLLM model string (e.g. `gpt-4o`, `claude-3-5-sonnet-20241022`, `gemini/gemini-1.5-pro`) |
| `api_key_env_var` | `str` | No | — | Env var holding the provider API key |
| `api_base_env_var` | `str` | No | — | Env var holding a custom API base URL (for proxies or self-hosted endpoints) |
| `litellm_resource_key` | `str` | No | — | Key of a `LiteLLMResource` in the Dagster resources dict (alternative to inline config) |
| `prompt_template` | `str` | Yes | — | Python format string referencing column names: `"Summarize: {body}"` |
| `system_prompt` | `str` | No | — | System prompt prepended to every request |
| `temperature` | `float` | No | `0.0` | Sampling temperature (0 = deterministic) |
| `max_tokens` | `int` | No | `1024` | Maximum tokens in each completion |

*`model` is required unless a `LiteLLMResource` is wired via `litellm_resource_key`.

### Processing

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `batch_size` | `int` | No | `10` | Number of rows between progress log messages |
| `max_rows` | `int` | No | — | Cap on rows processed; useful for testing before a full run |

### Output

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `response_column` | `str` | No | `llm_response` | Name of the column added to store LLM responses |

The enriched DataFrame is returned as the asset value; there are no destination database fields. To write it to a table, add a downstream sink component or use a database IO manager.

### Asset Metadata

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | `str` | Yes | — | Dagster asset name |
| `group_name` | `str` | No | `ai` | Dagster asset group |
| `description` | `str` | No | — | Asset description (auto-generated from `prompt_template` if omitted) |
| `deps` | `list[str]` | No | — | Additional upstream asset keys beyond `upstream_asset_key` |

---

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name |
| `prompt_template` | `str` | Python format string using column names: 'Classify: {text_column}' |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `api_key_env_var` | `str` | — | Env var with provider API key — overrides resource if set |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `batch_size` | `int` | `10` | Rows to process per batch (for progress logging) |
| `max_rows` | `int` | — | Limit rows processed (useful for testing) |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"ai"` | Asset group name |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | Asset owners — team names ('team:analytics') or email addresses. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags applied to the asset in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or th… _(full docs in schema.json + component README)_ |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on Dagster's TimeWindowPartitionsDefinition(cron_schedule=...). |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule. |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `model` | `str` | — | LiteLLM model string — overrides resource if set |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `source` | `Dict[str, Any]` | — | Pull rows directly via SQL instead of from an upstream asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `upstream_asset_key` -- set exactly one. |
| `upstream_asset_key` | `Union[str, List[str]]` | — | Upstream asset key providing a DataFrame (e.g. 'raw_orders' or 'schema/table') |
| `api_base_env_var` | `str` | — | Env var with custom API base URL |
| `litellm_resource_key` | `str` | — | Key of a LiteLLMResource in resources dict |
| `system_prompt` | `str` | — | System prompt for chat completions |
| `response_column` | `Union[str, int]` | `"llm_response"` | Column name to store LLM responses |
| `temperature` | `float` | `0.0` | Sampling temperature |
| `max_tokens` | `int` | `1024` | Max tokens per completion |
| `include_reasoning` | `bool` | `false` | Reasoning models (Qwen3, DeepSeek-R1, ...) inline <think>...</think>. The thinking is always stripped from the response column; when true it is also written to an extra '<response_column>_reasoning' column. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_community_components.LiteLLMInferenceAssetComponent
attributes:
  asset_name: enriched_support_tickets
  upstream_asset_key: raw_support_tickets
  model: claude-3-5-sonnet-20241022
  api_key_env_var: ANTHROPIC_API_KEY
  prompt_template: "Classify the sentiment and urgency of this support ticket in JSON: {body}"
  system_prompt: "You are a support ticket classifier. Respond with JSON only: {\"sentiment\": \"positive|neutral|negative\", \"urgency\": \"low|medium|high\"}"
  response_column: ai_classification
```

### Switching to OpenAI

```yaml
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
```

### Switching to AWS Bedrock

```yaml
  model: bedrock/anthropic.claude-3-sonnet-20240229-v1:0
  # No api_key_env_var needed — uses boto3 credentials from environment
```

### Reading from a database instead of an upstream asset

Instead of `upstream_asset_key`, set `source` to run a SQL query and feed its rows straight into this component -- no intermediate asset required. Set exactly one of `upstream_asset_key` or `source` (setting both or neither raises a `ValueError` at load time).

`source` is `{kind: warehouse_query, sql: <query>, resource_key: <registered resource> OR database_url_env_var: <env var>}`. `resource_key` names a Dagster resource exposing `.get_engine()`, `.get_connection()` or `.get_client()` (Postgres, Snowflake, DuckDB, BigQuery, Redshift, ...); `database_url_env_var` names an env var holding a SQLAlchemy URL.

```yaml
type: dagster_community_components.LiteLLMInferenceAssetComponent
attributes:
  asset_name: enriched_tickets
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  prompt_template: "Classify this ticket: {body}"
  source:
    kind: warehouse_query
    database_url_env_var: WAREHOUSE_URL
    sql: "SELECT id, body FROM support.tickets WHERE created_at > current_date - 7"
```

Results stay a DataFrame (this adds no database write-back); compose with a sink component to persist them.

---

## Using LiteLLMResource for Shared Config

If multiple components in your project use the same LLM, define a `LiteLLMResource` once and reference it by key:

```python
from dagster_community_components.litellm_inference_asset.component import LiteLLMResource

defs = dg.Definitions(
    resources={
        "litellm": LiteLLMResource(
            model="gpt-4o",
            api_key_env_var="OPENAI_API_KEY",
            temperature=0.0,
            max_tokens=512,
        )
    }
)
```

Then in your component YAML:

```yaml
  litellm_resource_key: litellm
  # model and api_key_env_var can be omitted — inherited from resource
```

---

## Notes

- **Row-level processing**: each row is sent as a separate LLM request. For large datasets, costs and latency scale linearly with row count. Use `max_rows` to test on a sample first.
- **prompt_template columns**: if a column name referenced in `prompt_template` does not exist in the DataFrame, the component raises a `ValueError` listing the available columns.
- **Output format**: LLM responses are stored as raw strings. For structured output (JSON, etc.), include formatting instructions in `system_prompt` and parse `response_column` downstream.
- **Retries**: LiteLLM handles retries internally. The default is 3 retries with exponential backoff.

## Reasoning models (Qwen3, DeepSeek-R1, ...)

Reasoning models inline their chain of thought as `<think>...</think>`. The component strips it from the model's reply *before* parsing/storing, so the output is the clean answer (non-reasoning models are unaffected). Set `include_reasoning: true` to keep the thinking: adds a `<response_column>_reasoning` column (null when the model produced none). `LiteLLMResource.complete()` also returns the stripped answer.

## Partitioning

When `partition_type` is set, each run reads only the upstream rows that fall in its partition (by `partition_date_column` / `partition_static_column`); unpartitioned runs read everything.
