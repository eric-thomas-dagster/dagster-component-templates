# LangChain Chain Asset

Run a LangChain chain over every row of a DataFrame and write the enriched output to a database table.

## What It Does

This component reads tabular data from either an upstream Dagster asset (returning a pandas DataFrame) or a source database table, then runs each row through a configurable LangChain chain composed of:

1. A **prompt template** — column values are substituted as `{column_name}` placeholders.
2. An **LLM** — any supported LangChain chat model.
3. A **StrOutputParser** — extracts the string response.

The LLM response is appended to the DataFrame as a new column (`response_column`) and the full enriched DataFrame is written to a destination database table via SQLAlchemy.

Typical use cases include summarization, classification, entity extraction, sentiment analysis, and any other row-level LLM enrichment pattern.

## Supported LLM Providers

| `llm_provider` value | Provider | Required pip package |
|---|---|---|
| `openai` | OpenAI (GPT-4, GPT-4o, etc.) | `langchain-openai` |
| `anthropic` | Anthropic (Claude 3, etc.) | `langchain-anthropic` |
| `azure_openai` | Azure OpenAI | `langchain-openai` |
| `google` | Google Gemini / PaLM | `langchain-google-genai` |
| `ollama` | Ollama (local models) | `langchain-ollama` |

## Required Base Packages

Always install these regardless of provider:

```
langchain-core>=0.3.0
langchain-openai>=0.2.0   # or your chosen provider package
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Configuration Fields

### Source Data

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | str | required | Dagster asset name for this component |
| `upstream_asset_key` | str | None | Upstream Dagster asset key whose materialized value (a DataFrame) is used as input |
| `source_database_url_env_var` | str | None | Env var holding a SQLAlchemy connection URL for source data (alternative to `upstream_asset_key`) |
| `source_table` | str | None | Source table name when using `source_database_url_env_var` |
| `source_query` | str | None | Custom SQL query for source data (overrides `source_table`) |

Either `upstream_asset_key` or `source_database_url_env_var` must be set.

### LLM Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `llm_provider` | str | `openai` | Provider name: `openai`, `anthropic`, `azure_openai`, `google`, `ollama` |
| `model` | str | `gpt-4o-mini` | Model identifier (provider-specific, e.g. `claude-3-5-sonnet-20241022`) |
| `api_key_env_var` | str | None | Env var holding the provider API key |
| `api_base_env_var` | str | None | Env var holding a custom API base URL (useful for Azure endpoints or Ollama) |
| `temperature` | float | `0.0` | Sampling temperature |
| `max_tokens` | int | `1024` | Maximum tokens per completion |

### Chain Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `prompt_template` | str | required | LangChain prompt template. Use `{column_name}` to reference DataFrame columns |
| `system_message` | str | None | System message prepended for chat models |
| `response_column` | str | `chain_output` | Column name added to the DataFrame for the LLM response |
| `parse_json` | bool | `false` | When `true`, attempt to parse the LLM response as JSON and expand keys into separate columns |

### Processing

| Field | Type | Default | Description |
|---|---|---|---|
| `batch_size` | int | `10` | How often (in rows) to emit a progress log |
| `max_rows` | int | None | Cap the number of rows processed — useful for smoke-testing |
| `max_concurrency` | int | `1` | Concurrent LLM calls. Increase carefully to avoid rate limits |

### Destination

| Field | Type | Default | Description |
|---|---|---|---|
| `database_url_env_var` | str | required | Env var holding the destination SQLAlchemy connection URL |
| `table_name` | str | required | Destination table name |
| `schema_name` | str | None | Destination schema (optional) |
| `if_exists` | str | `replace` | Pandas `to_sql` behavior: `fail`, `replace`, or `append` |

### Asset Metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | str | `ai` | Dagster asset group name |
| `description` | str | None | Asset description (auto-generated from provider/model/prompt if omitted) |
| `deps` | list[str] | None | Additional upstream asset keys for lineage (beyond `upstream_asset_key`) |

## Example YAML

```yaml
type: dagster_component_templates.LangChainChainAssetComponent
attributes:
  asset_name: summarized_articles
  upstream_asset_key: raw_articles
  llm_provider: openai
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  prompt_template: |
    Summarize the following article in 2-3 sentences:
    {content}
  system_message: "You are a concise summarizer. Respond with plain text only."
  response_column: summary
  database_url_env_var: DATABASE_URL
  table_name: summarized_articles
```

## Structured Output with `parse_json`

Set `parse_json: true` when the prompt instructs the LLM to return a JSON object. The component will attempt `json.loads()` on each response and expand the keys into separate DataFrame columns before writing. If parsing fails for a row, that row falls back to storing the raw string in `response_column`.

Example prompt for structured output:

```yaml
prompt_template: |
  Extract the following fields from this support ticket as JSON:
  {{"sentiment": "<positive|neutral|negative>", "category": "<billing|technical|other>", "urgency": <1-5>}}

  Ticket:
  {body}
parse_json: true
```

This would produce `sentiment`, `category`, and `urgency` columns in the output table.

## Asset Lineage

Use `upstream_asset_key` to both load data from and declare a lineage dependency on a single upstream asset. Use `deps` to declare additional lineage-only dependencies (assets that must be materialized first but whose values are not loaded directly). Both fields can be combined:

```yaml
upstream_asset_key: raw_articles        # loaded as DataFrame + lineage dep
deps:
  - article_ingestion_complete          # lineage dep only (no data load)
```

The Dagster asset graph will show all declared dependencies and enforce their materialization order.

## Anthropic Example

```yaml
type: dagster_component_templates.LangChainChainAssetComponent
attributes:
  asset_name: classified_tickets
  source_database_url_env_var: SOURCE_DB_URL
  source_table: support_tickets
  llm_provider: anthropic
  model: claude-3-5-haiku-20241022
  api_key_env_var: ANTHROPIC_API_KEY
  prompt_template: |
    Classify this support ticket into one of: billing, technical, account, other.
    Respond with the single category word only.

    Ticket: {body}
  response_column: category
  database_url_env_var: DATABASE_URL
  table_name: classified_tickets
  if_exists: append
```

## Ollama (Local) Example

```yaml
type: dagster_component_templates.LangChainChainAssetComponent
attributes:
  asset_name: local_summaries
  upstream_asset_key: raw_documents
  llm_provider: ollama
  model: llama3.2
  api_base_env_var: OLLAMA_BASE_URL   # e.g. http://localhost:11434
  prompt_template: "Summarize: {text}"
  response_column: summary
  database_url_env_var: DATABASE_URL
  table_name: local_summaries
  temperature: 0.2
  max_rows: 50
```
