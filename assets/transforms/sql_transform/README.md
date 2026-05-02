# SQL Transform

Warehouse-pushdown SQL transform. Takes a `SELECT` plus a destination table name, runs `CREATE TABLE <dest> AS <SELECT>` directly on the warehouse — **no data round-trips through Python**.

For teams who live in Snowflake / BigQuery / Redshift and want a lightweight pushdown asset without spinning up a full dbt project. For multi-step transforms, use `dagster-dbt` instead.

## How it works

Connection is via a single SQLAlchemy URL env var, so any warehouse with a SQLAlchemy dialect works:

| Warehouse | URL form |
|---|---|
| Snowflake | `snowflake://user:pwd@account/db/schema?warehouse=COMPUTE_WH` |
| BigQuery  | `bigquery://my-project/my_dataset` *(requires `sqlalchemy-bigquery`)* |
| Redshift  | `redshift+psycopg2://user:pwd@host:5439/dbname` |
| Postgres  | `postgresql://user:pwd@host:5432/dbname` |

The component dispatches one of two execution paths:

- `if_exists: replace` → `DROP TABLE IF EXISTS <dest>; CREATE TABLE <dest> AS <sql>`
- `if_exists: append`  → `INSERT INTO <dest> <sql>`

Both are wrapped in a single transaction.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | **required** | Dagster asset name |
| `connection_url_env_var` | `str` | **required** | Env var holding a SQLAlchemy URL |
| `destination_table` | `str` | **required** | Fully-qualified destination |
| `sql` | `str` | **required** | The SELECT body |
| `template_vars` | `Dict[str, str]` | `None` | Simple `{{ name }}` substitution map (no Jinja) |
| `if_exists` | `replace` \| `append` | `replace` | DDL mode |
| `upstream_asset_keys` | `List[str]` | `None` | Upstream asset keys for lineage |
| `kinds` | `List[str]` | auto-inferred | Catalog kinds |
| `group_name` | `str` | `warehouse_transforms` | Asset group |
| `description` | `str` | auto-generated | Catalog description |
| `asset_tags` | `Dict[str, str]` | `None` | Catalog tags |
| `owners` | `List[str]` | `None` | Owner email/team |

## Example — Snowflake order dedup

```yaml
type: dagster_component_templates.SqlTransformComponent
attributes:
  asset_name: orders_dedup
  connection_url_env_var: SNOWFLAKE_URL
  destination_table: ANALYTICS.PUBLIC.ORDERS_DEDUP
  sql: |
    SELECT *
    FROM {{ upstream }}
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY order_id ORDER BY updated_at DESC
    ) = 1
  template_vars:
    upstream: ANALYTICS.PUBLIC.ORDERS_RAW
  if_exists: replace
  upstream_asset_keys:
    - orders_raw
```

## Example — BigQuery aggregation

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

## Example — Redshift incremental insert

```yaml
type: dagster_component_templates.SqlTransformComponent
attributes:
  asset_name: events_today
  connection_url_env_var: REDSHIFT_URL
  destination_table: analytics.events_history
  sql: |
    SELECT *
    FROM raw.events
    WHERE event_date = CURRENT_DATE
  if_exists: append
```

## Lineage

`upstream_asset_keys` draws lineage edges in the Dagster graph without loading data at runtime. Pair this component with `external_snowflake_table` / `external_bigquery_table` etc. so upstream warehouse tables show up as proper Dagster assets.

## When to use vs dbt

| Use this | Use dagster-dbt |
|---|---|
| One CTAS asset | A pipeline of N models with refs and macros |
| Quick prototype on a single transform | Production warehouse pipeline |
| Don't want a dbt project structure | Already have or want dbt |

## Requirements

```
sqlalchemy>=1.4
# Plus a warehouse-specific driver:
# snowflake-sqlalchemy
# sqlalchemy-bigquery
# sqlalchemy-redshift + psycopg2-binary
```
