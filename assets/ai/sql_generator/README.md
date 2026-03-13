# SqlGeneratorComponent

Generate SQL queries from natural language questions using an LLM. Optionally provide database schema context (DDL) and a SQL dialect to improve accuracy.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `question_column` | string | yes | — | Column with natural language questions |
| `output_column` | string | no | `"generated_sql"` | Column to write SQL |
| `schema_context` | string | no | null | DDL or table description for prompt |
| `dialect` | enum | no | `"generic"` | `postgresql`, `mysql`, `sqlite`, `bigquery`, `snowflake`, `mssql` |
| `model` | string | no | `"gpt-4o-mini"` | LLM model |
| `validate_sql` | boolean | no | `false` | Run sqlparse validation |
| `api_key_env_var` | string | no | null | Env var for API key |

## Example

```yaml
component_type: dagster_component_templates.SqlGeneratorComponent
asset_name: generated_sql_queries
upstream_asset_key: nl_questions
question_column: question
schema_context: |
  CREATE TABLE orders (id INT, amount DECIMAL);
dialect: postgresql
validate_sql: true
```
