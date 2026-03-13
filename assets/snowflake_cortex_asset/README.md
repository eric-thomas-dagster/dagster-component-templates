# Snowflake Cortex Asset Component

Runs [Snowflake Cortex](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions) LLM functions directly inside Snowflake and writes enriched results to a target table — no data leaves the warehouse.

---

## What it does

Generates and executes a `CREATE OR REPLACE TABLE ... AS SELECT *,  SNOWFLAKE.CORTEX.<fn>(...) AS <output_col> FROM <source>` statement for the chosen Cortex function, then reports the row count.

### Supported Cortex functions

| Function | Description |
|----------|-------------|
| `COMPLETE` | LLM completion with a configurable model and optional prompt template. |
| `SENTIMENT` | Returns a sentiment score in the range [-1, 1]. |
| `SUMMARIZE` | Produces a text summary. |
| `CLASSIFY_TEXT` | Assigns the most likely category from a provided list. |
| `TRANSLATE` | Translates text to a target language. |
| `EXTRACT_ANSWER` | Extracts an answer to a question from the text. |

---

## Required packages

| Package | Minimum version | Purpose |
|---------|----------------|---------|
| `dagster` | 1.8.0 | Orchestration framework |
| `snowflake-connector-python` | 3.6.0 | Snowflake connectivity |
| `snowflake-sqlalchemy` | 1.5.0 | Optional SQLAlchemy dialect |

Install with:

```bash
pip install snowflake-connector-python>=3.6.0 snowflake-sqlalchemy>=1.5.0
```

---

## Configuration fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | — | Dagster asset key name. |
| `snowflake_account_env_var` | `str` | `SNOWFLAKE_ACCOUNT` | Env var holding the Snowflake account identifier. |
| `snowflake_user_env_var` | `str` | `SNOWFLAKE_USER` | Env var holding the Snowflake username. |
| `snowflake_password_env_var` | `str` | `SNOWFLAKE_PASSWORD` | Env var holding the Snowflake password. |
| `snowflake_database` | `str` | — | Snowflake database name. |
| `snowflake_schema` | `str` | `PUBLIC` | Snowflake schema name. |
| `snowflake_warehouse` | `str \| null` | `null` | Virtual warehouse to use. Uses user default when null. |
| `source_table` | `str` | — | Source table (fully qualified or relative). |
| `target_table` | `str` | — | Target table for enriched output. |
| `cortex_function` | `str` | `COMPLETE` | Cortex function: COMPLETE, CLASSIFY_TEXT, SENTIMENT, SUMMARIZE, TRANSLATE, or EXTRACT_ANSWER. |
| `model` | `str` | `claude-3-5-sonnet` | LLM model for COMPLETE (e.g. `mistral-large`, `llama3-70b`). |
| `text_column` | `str` | — | Column passed as the text argument. |
| `output_column` | `str` | `cortex_result` | Output column name added to the target table. |
| `prompt_template` | `str \| null` | `null` | For COMPLETE: template with `{text}` placeholder. |
| `classify_categories` | `list \| null` | `null` | For CLASSIFY_TEXT: list of category strings. |
| `translate_target_language` | `str \| null` | `null` | For TRANSLATE: BCP-47 language code, e.g. `"en"`, `"fr"`. |
| `extract_answer_question` | `str \| null` | `null` | For EXTRACT_ANSWER: the question to answer. |
| `if_exists` | `str` | `replace` | `"replace"` (CREATE OR REPLACE) or `"append"` (INSERT INTO). |
| `batch_size` | `int` | `1000` | Progress logging batch size (does not affect SQL execution). |
| `group_name` | `str \| null` | `ai_enrichment` | Dagster asset group name shown in the UI. |
| `deps` | `list \| null` | `null` | Upstream asset keys for lineage. |

---

## Example YAML

### Sentiment analysis on support tickets

```yaml
type: dagster_component_templates.SnowflakeCortexAssetComponent
attributes:
  asset_name: support_ticket_sentiment
  snowflake_account_env_var: SNOWFLAKE_ACCOUNT
  snowflake_user_env_var: SNOWFLAKE_USER
  snowflake_password_env_var: SNOWFLAKE_PASSWORD
  snowflake_database: ANALYTICS
  snowflake_schema: PUBLIC
  source_table: RAW_SUPPORT_TICKETS
  target_table: ENRICHED_SUPPORT_TICKETS
  cortex_function: SENTIMENT
  text_column: ticket_body
  output_column: sentiment_score
  group_name: ai_enrichment
  deps:
    - raw/support_tickets
```

### LLM completion with a prompt template

```yaml
type: dagster_component_templates.SnowflakeCortexAssetComponent
attributes:
  asset_name: ticket_summary
  snowflake_account_env_var: SNOWFLAKE_ACCOUNT
  snowflake_user_env_var: SNOWFLAKE_USER
  snowflake_password_env_var: SNOWFLAKE_PASSWORD
  snowflake_database: ANALYTICS
  source_table: RAW_SUPPORT_TICKETS
  target_table: TICKET_SUMMARIES
  cortex_function: COMPLETE
  model: claude-3-5-sonnet
  text_column: ticket_body
  output_column: summary
  prompt_template: "Summarize this support ticket in one sentence: {text}"
  group_name: ai_enrichment
```

### Text classification

```yaml
type: dagster_component_templates.SnowflakeCortexAssetComponent
attributes:
  asset_name: ticket_category
  snowflake_account_env_var: SNOWFLAKE_ACCOUNT
  snowflake_user_env_var: SNOWFLAKE_USER
  snowflake_password_env_var: SNOWFLAKE_PASSWORD
  snowflake_database: ANALYTICS
  source_table: RAW_SUPPORT_TICKETS
  target_table: CATEGORIZED_TICKETS
  cortex_function: CLASSIFY_TEXT
  text_column: ticket_body
  output_column: category
  classify_categories:
    - billing
    - technical
    - account
    - other
  group_name: ai_enrichment
```

---

## Required environment variables

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` (or custom) | Snowflake account identifier (e.g. `xy12345.us-east-1`) |
| `SNOWFLAKE_USER` (or custom) | Snowflake username |
| `SNOWFLAKE_PASSWORD` (or custom) | Snowflake password |

---

## Materialization metadata

| Key | Description |
|-----|-------------|
| `rows_processed` | Number of rows in the target table after the run |
| `cortex_function` | The Cortex function used |
| `model` | LLM model used (COMPLETE only; `"n/a"` for others) |
| `source_table` | Source table name |
| `target_table` | Target table name |
