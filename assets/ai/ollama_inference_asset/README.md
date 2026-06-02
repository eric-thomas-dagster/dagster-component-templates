# Ollama Inference Asset

> **🟢 No external API key.** This component talks to a local Ollama server (default http://localhost:11434). Make sure Ollama is running and the requested model has been pulled (`ollama pull llama3`).

A Dagster component that reads rows from an upstream asset or database table, enriches each row by running it through a locally-hosted Ollama model, and writes the results to a destination database table.

**No API key or internet connection required.** Ollama runs open-source LLMs entirely on your own hardware, making this component ideal for sensitive data, air-gapped environments, cost-sensitive workloads, or local development and testing.

---

## What It Does

1. Loads a DataFrame either from an upstream Dagster asset or directly from a source database table via SQLAlchemy.
2. For each row, formats your `prompt_template` string using the row's column values.
3. Sends the formatted prompt (plus an optional `system_prompt`) to the Ollama `/api/generate` endpoint.
4. Appends the model response to a new column (`response_column`) in the DataFrame.
5. Writes the enriched DataFrame to a destination database table via SQLAlchemy.
6. Emits Dagster materialization metadata: row count, model used, Ollama host, response column, and destination table.

---

## Prerequisites: Running Ollama

Install Ollama from [https://ollama.com](https://ollama.com), then pull the model you want to use:

```bash
ollama pull llama3.2
ollama pull mistral
ollama pull gemma2
```

The Ollama server starts automatically on `http://localhost:11434`. To run it on a different host or port, set the `OLLAMA_HOST` environment variable and pass it via `ollama_host_env_var`.

---

## Supported Models

Any model available in the [Ollama model library](https://ollama.com/library) can be used. Common choices:

| Model | `model` value | Notes |
|---|---|---|
| Llama 3.2 (3B) | `llama3.2` | Fast, good general purpose |
| Llama 3.1 (8B) | `llama3.1` | Stronger reasoning |
| Llama 3.1 (70B) | `llama3.1:70b` | Highest quality, needs GPU |
| Mistral 7B | `mistral` | Efficient instruction follower |
| Gemma 2 (9B) | `gemma2` | Google open model |
| Phi-3 Mini | `phi3` | Very fast, lightweight |
| Qwen 2.5 | `qwen2.5` | Strong multilingual support |
| DeepSeek-R1 | `deepseek-r1` | Reasoning-focused |
| CodeLlama | `codellama` | Code generation and review |

---

## Required Packages

```
requests>=2.31.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

Install with:

```bash
pip install requests pandas sqlalchemy
```

No provider SDK or API credentials are needed beyond a running Ollama server.

---

## Component Fields

### Source (pick one)

| Field | Type | Required | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | No | Dagster asset key to load as a DataFrame (e.g. `raw_feedback` or `schema/table`) |
| `source_database_url_env_var` | `str` | No | Env var containing a SQLAlchemy connection URL for the source database |
| `source_table` | `str` | No | Table name to read when using `source_database_url_env_var` |
| `source_query` | `str` | No | Custom SQL query; overrides `source_table` when set |

### Ollama Configuration

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `model` | `str` | No | `llama3.2` | Ollama model name (must be pulled locally before use) |
| `ollama_host_env_var` | `str` | No | — | Env var containing the Ollama server URL; falls back to `http://localhost:11434` |
| `ollama_resource_key` | `str` | No | — | Key of an `OllamaResource` in the Dagster resources dict |
| `prompt_template` | `str` | Yes | — | Python format string referencing column names: `"Classify: {feedback_text}"` |
| `system_prompt` | `str` | No | — | System prompt sent with every request |
| `temperature` | `float` | No | `0.0` | Sampling temperature (0 = deterministic) |
| `timeout_seconds` | `int` | No | `120` | Per-row HTTP request timeout in seconds |

### Processing

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `batch_size` | `int` | No | `10` | Number of rows between progress log messages |
| `max_rows` | `int` | No | — | Cap on rows processed; useful for testing before a full run |

### Destination

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `database_url_env_var` | `str` | Yes | — | Env var containing the destination SQLAlchemy connection URL |
| `table_name` | `str` | Yes | — | Destination table name |
| `schema_name` | `str` | No | — | Destination schema (e.g. `analytics`) |
| `if_exists` | `str` | No | `replace` | Behavior when table exists: `replace`, `append`, or `fail` |
| `response_column` | `str` | No | `llm_response` | Name of the column added to store model responses |

### Asset Metadata

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | `str` | Yes | — | Dagster asset name |
| `group_name` | `str` | No | `ai` | Dagster asset group |
| `description` | `str` | No | — | Asset description (auto-generated from model and `prompt_template` if omitted) |
| `deps` | `list[str]` | No | — | Additional upstream asset keys beyond `upstream_asset_key` |

---

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name |
| `upstream_asset_key` | `str` | Upstream asset key providing a DataFrame |
| `prompt_template` | `str` | Python format string using column names: 'Classify: {text_column}' |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout_seconds` | `int` | `120` | Request timeout per row in seconds |
| `batch_size` | `int` | `10` | Rows per batch for progress logging |
| `max_rows` | `int` | — | Limit rows processed (for testing) |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"ai"` | Asset group name |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | Asset owners — team names ('team:analytics') or email addresses. |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags applied to the asset in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}. |
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
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `str` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `str` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `model` | `str` | `"llama3.2"` | Ollama model name (e.g. llama3.2, mistral, gemma2, phi3) |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `ollama_host_env_var` | `str` | — | Env var with Ollama server URL (default: http://localhost:11434) |
| `ollama_resource_key` | `str` | — | Key of an OllamaResource in resources dict |
| `system_prompt` | `str` | — | System prompt for the model |
| `response_column` | `str` | `"llm_response"` | Column name to store model responses |
| `temperature` | `float` | `0.0` | Sampling temperature |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |

[//]: # (FIELDS:END)

## Example YAML

```yaml
type: dagster_component_templates.OllamaInferenceAssetComponent
attributes:
  asset_name: categorized_feedback
  upstream_asset_key: raw_feedback
  ollama_host_env_var: OLLAMA_HOST
  model: llama3.2
  prompt_template: "Categorize this customer feedback into one of [bug, feature, praise, question]: {feedback_text}"
  system_prompt: "You are a feedback classifier. Respond with only one word: bug, feature, praise, or question."
  response_column: category
  database_url_env_var: DATABASE_URL
  table_name: categorized_feedback
```

### Reading from a database instead of an upstream asset

```yaml
type: dagster_component_templates.OllamaInferenceAssetComponent
attributes:
  asset_name: summarized_articles
  source_database_url_env_var: SOURCE_DB_URL
  source_query: "SELECT id, title, body FROM articles WHERE summary IS NULL"
  model: mistral
  prompt_template: "Write a two-sentence summary of this article titled '{title}': {body}"
  response_column: summary
  database_url_env_var: DATABASE_URL
  table_name: summarized_articles
  if_exists: append
```

### Using a larger model with a remote Ollama server

```yaml
  ollama_host_env_var: OLLAMA_HOST   # e.g. http://gpu-server:11434
  model: llama3.1:70b
  timeout_seconds: 300
```

---

## Using OllamaResource for Shared Config

If multiple components in your project connect to the same Ollama server, define an `OllamaResource` once and reference it by key. This avoids repeating host and model configuration across YAML files.

```python
from dagster_component_templates.ollama_inference_asset.component import OllamaResource

defs = dg.Definitions(
    resources={
        "ollama": OllamaResource(
            host="http://localhost:11434",
            model="llama3.2",
            temperature=0.0,
            timeout_seconds=120,
        )
    }
)
```

Then in your component YAML:

```yaml
  ollama_resource_key: ollama
  # ollama_host_env_var and model can be omitted — inherited from resource
```

The `OllamaResource` also exposes a `generate(prompt, system)` method you can use directly in custom Dagster ops and sensors.

---

## Notes

- **Row-level processing**: each row is sent as a separate HTTP request to Ollama. Latency per row depends on model size and hardware. Use `max_rows` to test on a subset first.
- **Model must be pulled first**: if the model is not available locally, Ollama returns a 404. Run `ollama pull <model>` before executing the asset.
- **prompt_template columns**: if a column name referenced in `prompt_template` does not exist in the DataFrame, the component raises a `ValueError` listing the available columns.
- **Output format**: model responses are stored as raw strings. For structured output (JSON, labels, etc.), use a concise `system_prompt` and parse `response_column` in a downstream asset.
- **Streaming**: this component uses `stream: false` for simplicity. For very large outputs, increasing `timeout_seconds` is preferable to enabling streaming.
- **GPU acceleration**: Ollama automatically uses GPU if available (Metal on macOS, CUDA on Linux). No configuration change is needed in this component.
