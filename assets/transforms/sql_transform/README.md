# SQL Transform

Templated SQL → table (CTAS / INSERT pushdown) OR templated SQL → DataFrame (inline read). Vendor-agnostic via SQLAlchemy URLs.

For teams who live in Snowflake / BigQuery / Redshift / Postgres / Oracle / Db2 / DuckDB / Trino and want a lightweight pushdown asset without spinning up a full dbt project. Mirrors the shape of Dagster's `dagster_snowflake.TemplatedSQLComponent` but works against any SQLAlchemy backend.

## Two modes

| Mode | What it does | Asset value |
|---|---|---|
| `return_dataframe: false` (default) | `CREATE TABLE <dest> AS <sql>` (replace) or `INSERT INTO <dest> <sql>` (append) | `MaterializeResult` (table is the side-effect) |
| `return_dataframe: true` | Executes the SQL as a SELECT, returns its result | `pd.DataFrame` (consumable by downstream pandas assets) |

## How it works

Connection is via a single SQLAlchemy URL env var, so any warehouse with a SQLAlchemy dialect works:

| Warehouse | URL form |
|---|---|
| Snowflake | `snowflake://user:pwd@account/db/schema?warehouse=COMPUTE_WH` |
| BigQuery  | `bigquery://my-project/my_dataset` *(requires `sqlalchemy-bigquery`)* |
| Redshift  | `redshift+psycopg2://user:pwd@host:5439/dbname` |
| Postgres  | `postgresql://user:pwd@host:5432/dbname` |
| DuckDB    | `duckdb:////tmp/local.db` |
| Oracle    | `oracle+oracledb://user:pwd@host:port/?service_name=...` |
| Db2       | `db2+ibm_db://user:pwd@host:port/db` |
| Trino     | `trino://user@host:8080/catalog` |

## Templating

SQL is rendered via **Jinja2** (with a `str.replace` fallback when Jinja2 isn't installed). Supports conditionals, loops, filters:

```sql
SELECT *
FROM {{ orders_table }}
{% if partition_key %}
WHERE order_date = '{{ partition_key }}'
{% endif %}
```

### Auto-injected context vars

When the asset runs, the following are available in the template (no need to declare in `template_vars`):

| Variable | Set when |
|---|---|
| `partition_key` | Asset is partitioned and a partition is being materialized |
| `partition_key_range_start` / `partition_key_range_end` | Partition-range backfills |
| `partition_time_window_start` / `partition_time_window_end` | Time-window partitions (ISO 8601) |
| `asset_key` | Always — slash-joined asset key path |
| `run_id` | Always — the current Dagster run id |

`template_vars` (user-supplied) override auto-injected vars on name conflict.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | **required** | Dagster asset name |
| `connection_url_env_var` | `str` | **required** | Env var holding a SQLAlchemy URL |
| `destination_table` | `str` | — | Fully-qualified destination. Required when `return_dataframe=false` |
| `sql` | `str` | **required** | The SELECT body (rendered via Jinja2) |
| `template_vars` | `Dict[str, str]` | `None` | User-supplied template variables |
| `return_dataframe` | `bool` | `false` | If true, return DataFrame instead of CTAS/INSERT |
| `if_exists` | `replace` \| `append` | `replace` | DDL mode (ignored when `return_dataframe=true`) |
| `upstream_asset_keys` | `List[str]` | `None` | Upstream asset keys for lineage |
| `kinds` | `List[str]` | auto-inferred | Catalog kinds |
| `group_name` | `str` | `warehouse_transforms` | Asset group |
| `asset_tags` | `Dict[str, str]` | `None` | Catalog tags |
| `owners` | `List[str]` | `None` | Owner email/team |

## Examples

### Daily-partitioned Snowflake CTAS (uses auto-injected `partition_key`)

```yaml
type: dagster_component_templates.SqlTransformComponent
attributes:
  asset_name: daily_revenue
  connection_url_env_var: SNOWFLAKE_URL
  destination_table: ANALYTICS.PUBLIC.DAILY_REVENUE
  sql: |
    SELECT region, SUM(amount) AS revenue
    FROM {{ source_table }}
    WHERE order_date = '{{ partition_key }}'
    GROUP BY region
  template_vars:
    source_table: ANALYTICS.PUBLIC.ORDERS
  partition_type: daily
  partition_start: '2024-01-01'
```

### Inline read mode (DataFrame for downstream pandas)

```yaml
type: dagster_component_templates.SqlTransformComponent
attributes:
  asset_name: top_customers
  connection_url_env_var: SNOWFLAKE_URL
  return_dataframe: true
  sql: |
    SELECT customer_id, SUM(amount) AS lifetime_value
    FROM {{ orders }}
    GROUP BY customer_id
    ORDER BY lifetime_value DESC
    LIMIT 100
  template_vars:
    orders: ANALYTICS.PUBLIC.ORDERS
```

`top_customers` is now a DataFrame asset — a downstream `filter` / `summarize` / `dataframe_to_csv` can consume it directly.

### Conditional Jinja for one component used in both daily-batch and full-refresh

```yaml
sql: |
  SELECT *
  FROM {{ source_table }}
  {% if partition_key %}
  WHERE event_date = '{{ partition_key }}'
  {% else %}
  WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
  {% endif %}
```

### BigQuery aggregation

```yaml
type: dagster_component_templates.SqlTransformComponent
attributes:
  asset_name: daily_revenue
  connection_url_env_var: BIGQUERY_URL
  destination_table: "`my-project.analytics.daily_revenue`"
  sql: |
    SELECT
      DATE(created_at) AS day,
      SUM(amount) AS total_revenue,
      COUNT(*) AS order_count
    FROM {{ orders }}
    GROUP BY 1
    ORDER BY 1
  template_vars:
    orders: "`my-project.raw.orders`"
  if_exists: replace
```

## Lineage

`upstream_asset_keys` draws lineage edges in the Dagster graph without loading data at runtime. Pair with `external_snowflake_table` / `external_bigquery_table` so upstream warehouse tables show up as proper Dagster assets.

## When to use vs alternatives

| Use this | Use this instead |
|---|---|
| One CTAS asset with templated SQL | A multi-model pipeline → `dagster-dbt` |
| `dagster_snowflake.TemplatedSQLComponent` shape but vendor-agnostic | `dagster_snowflake.TemplatedSQLComponent` (if Snowflake-only) |
| Quick pushdown with partition_key auto-injection | `sql_query_reader` (static query, no template) |

## Requirements

```
sqlalchemy>=1.4
jinja2>=3.0
pandas>=1.5.0  # only when return_dataframe=true
# Plus a warehouse-specific driver:
# snowflake-sqlalchemy
# sqlalchemy-bigquery
# sqlalchemy-redshift + psycopg2-binary
# psycopg / pymysql / oracledb / ibm_db_sa / duckdb-engine / trino
```
