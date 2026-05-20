# DLT DataFrame Writer Component

Write pandas DataFrames to any database using [dlt (data load tool)](https://dlthub.com/) with automatic schema evolution and credential management.

## Overview

This component takes a DataFrame from an upstream asset and writes it to any dlt-supported destination. It's perfect for persisting DataFrames to production databases with minimal configuration.

## Key Features

- **14 Database Destinations**: Snowflake, BigQuery, Postgres, Redshift, DuckDB, MotherDuck, Databricks, ClickHouse, MSSQL, AWS Athena, MySQL, Filesystem (Parquet/CSV), Azure Synapse
- **Automatic Schema Evolution**: dlt handles table creation and schema changes
- **Environment-Aware Routing**: Different destinations for local, branch, and production
- **Three Write Modes**:
  - `replace`: Drop and recreate table
  - `append`: Add new rows
  - `merge`: Upsert based on primary key
- **Structured Configuration**: Conditional fields based on destination selection

## Configuration

### Required Fields

- **asset_name**: Name of the asset to create
- **table_name**: Name of the table in the destination
- **destination**: Target database (or leave empty for in-memory DuckDB)

### Write Modes

- **replace** (default): Drops the table and recreates it with new data
- **append**: Adds new rows to existing table
- **merge**: Updates existing rows and inserts new ones based on primary key
  - Requires `primary_key` field (comma-separated for composite keys)

### Destination Configuration

Each destination has specific fields that appear when selected:

#### Snowflake
- Account identifier
- Database, schema, warehouse
- Username, password, role (optional)

#### BigQuery
- Project ID, dataset
- Service account JSON path (optional)
- Location (default: US)

#### PostgreSQL
- Host, port, database
- Username, password, schema

#### Redshift
- Cluster endpoint, port, database
- Username, password, schema

#### DuckDB
- Database file path (defaults to :memory:)

#### MotherDuck
- Database name, authentication token

#### Databricks
- Server hostname, HTTP path
- Access token
- Catalog (optional), schema

#### ClickHouse
- Host, port, database
- Username, password

#### Microsoft SQL Server
- Host, port, database
- Username, password

#### AWS Athena
- S3 query result bucket
- Database, AWS credentials
- Region

#### MySQL
- Host, port, database
- Username, password

#### Filesystem
- Bucket path (local or S3)
- Format (parquet, jsonl, csv)

#### Azure Synapse
- Workspace hostname, database
- Username, password

### Environment-Aware Routing

Enable `use_environment_routing` to automatically select destinations based on where your code runs:

- **destination_local**: Used when not in Dagster Cloud (local development)
- **destination_branch**: Used for branch deployments
- **destination_prod**: Used for production deployments

The component detects environment using Dagster Cloud variables:
- `DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT`
- `DAGSTER_CLOUD_DEPLOYMENT_NAME`

## Usage Example

### Basic Usage (DuckDB)

```yaml
asset_name: customer_data_table
table_name: customers
write_disposition: replace
```

### Snowflake with Environment Routing

```yaml
asset_name: orders_table
table_name: orders
write_disposition: merge
primary_key: order_id

# Environment routing
use_environment_routing: true
destination_local: duckdb
destination_branch: postgres
destination_prod: snowflake

# Snowflake credentials (used in prod)
snowflake_account: myaccount.us-east-1
snowflake_database: analytics
snowflake_schema: raw_data
snowflake_warehouse: transforming
snowflake_username: ${SNOWFLAKE_USERNAME}
snowflake_password: ${SNOWFLAKE_PASSWORD}
```

### BigQuery Append Mode

```yaml
asset_name: events_table
table_name: events
write_disposition: append
destination: bigquery

bigquery_project_id: my-project
bigquery_dataset: analytics
bigquery_credentials_path: /path/to/service-account.json
bigquery_location: US
```

## How It Works

1. **Load DataFrame**: Loads DataFrame from connected upstream asset
2. **Determine Destination**: Uses environment routing if enabled
3. **Build Credentials**: Constructs dlt credentials from structured fields
4. **Create Pipeline**: Initializes dlt pipeline with destination config
5. **Write Data**: Executes pipeline.run() with DataFrame and write mode
6. **Schema Evolution**: dlt automatically creates/updates table schema

## Use Cases

- **Persist Synthetic Data**: Write generated test data to databases
- **API Response Storage**: Store API responses in production databases
- **Transformation Results**: Persist transformed DataFrames
- **Multi-Environment Pipelines**: Same config, different destinations per environment
- **Data Lake Writes**: Write to Parquet/CSV on S3 or local filesystem

## Integration

This component works with any DataFrame-producing asset:

- **Ingestion Components**: Write ingested data to databases (shopify_ingestion → dlt_dataframe_writer)
- **Transformation Components**: Persist transformation results (dataframe_transformer → dlt_dataframe_writer)
- **Custom Assets**: Write custom DataFrame assets to databases

## dlt vs Native Writers

**Use dlt_dataframe_writer when:**
- You need automatic schema evolution
- You want the same component for multiple destinations
- You're using environment-based routing
- You need merge/upsert functionality

**Use native writers (like duckdb_table_writer) when:**
- You only need DuckDB
- You want minimal dependencies
- You need simple, direct database operations

## Credentials and Security

- Use environment variables for sensitive credentials: `${VAR_NAME}`
- Mark fields as secrets in Dagster Designer
- Credentials are passed to dlt, not stored in configs
- Environment routing prevents accidental production writes during development

## Requirements

```bash
pip install dlt[<destination>]
```

Example for Snowflake:
```bash
pip install dlt[snowflake]
```

For multiple destinations:
```bash
pip install dlt[snowflake,bigquery,postgres]
```

## Error Handling

- **No upstream asset**: Error if no DataFrame input connected
- **Wrong input type**: Error if upstream asset doesn't produce DataFrame
- **Missing credentials**: Error if destination selected but credentials incomplete
- **Merge without primary key**: Error if merge mode used without primary key
- **Schema conflicts**: dlt handles schema evolution automatically

## Performance Tips

- Use `append` mode for incremental data
- Use `merge` mode for deduplication
- Use `replace` for full refreshes
- Consider partitioning for large datasets
- Use MotherDuck for cloud-based analytics on DuckDB

## Related Components

- **Ingestion Components**: 21 dlt-powered data sources
- **dataframe_transformer**: Transform data before writing
- **duckdb_table_writer**: Simple DuckDB writer
- **database_query**: Query written data

## Learn More

- [dlt Documentation](https://dlthub.com/docs/)
- [dlt Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/)
- [Dagster Components](https://docs.dagster.io/concepts/components)
- [Environment Variables](https://docs.dagster.io/deployment/dagster-plus/management/environment-variables/built-in)

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset to create |
| `table_name` | `str` | Name of the table to write data to in the destination |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Description of the asset |
| `group_name` | `str` | — | Asset group name for organization |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}. |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am). |

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
| `write_disposition` | `Literal['replace', 'append', 'merge']` | `"replace"` | Write mode: 'replace' (drop and recreate), 'append' (add rows), 'merge' (upsert based on primary key) |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `primary_key` | `str` | — | Primary key column(s) for merge mode (comma-separated if multiple) |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `upstream_asset_keys` | `str` | — | Comma-separated list of upstream asset keys to load DataFrames from (automatically set by custom lineage) |
| `destination` | `str` | — | Database destination for persisting data (leave empty for in-memory DuckDB) |
| `snowflake_account` | `str` | — | Snowflake account identifier |
| `snowflake_database` | `str` | — | Snowflake database name |
| `snowflake_schema` | `str` | `"public"` | Snowflake schema name |
| `snowflake_warehouse` | `str` | — | Snowflake warehouse name |
| `snowflake_username` | `str` | — | Snowflake username |
| `snowflake_password` | `str` | — | Snowflake password |
| `snowflake_role` | `str` | — | Snowflake role (optional) |
| `bigquery_project_id` | `str` | — | Google Cloud project ID |
| `bigquery_dataset` | `str` | — | BigQuery dataset name |
| `bigquery_credentials_path` | `str` | — | Path to service account JSON |
| `bigquery_location` | `str` | `"US"` | BigQuery dataset location |
| `postgres_host` | `str` | `"localhost"` | PostgreSQL host |
| `postgres_port` | `int` | `5432` | PostgreSQL port |
| `postgres_database` | `str` | — | PostgreSQL database name |
| `postgres_username` | `str` | — | PostgreSQL username |
| `postgres_password` | `str` | — | PostgreSQL password |
| `postgres_schema` | `str` | `"public"` | PostgreSQL schema |
| `redshift_host` | `str` | — | Redshift cluster endpoint |
| `redshift_port` | `int` | `5439` | Redshift port |
| `redshift_database` | `str` | — | Redshift database name |
| `redshift_username` | `str` | — | Redshift username |
| `redshift_password` | `str` | — | Redshift password |
| `redshift_schema` | `str` | `"public"` | Redshift schema |
| `duckdb_database_path` | `str` | — | Path to DuckDB file |
| `motherduck_database` | `str` | — | MotherDuck database name |
| `motherduck_token` | `str` | — | MotherDuck authentication token |
| `databricks_server_hostname` | `str` | — | Databricks workspace hostname |
| `databricks_http_path` | `str` | — | SQL warehouse HTTP path |
| `databricks_access_token` | `str` | — | Databricks personal access token |
| `databricks_catalog` | `str` | — | Unity Catalog name |
| `databricks_schema` | `str` | `"default"` | Databricks schema name |
| `clickhouse_host` | `str` | `"localhost"` | ClickHouse server host |
| `clickhouse_port` | `int` | `9000` | ClickHouse server port |
| `clickhouse_database` | `str` | `"default"` | ClickHouse database name |
| `clickhouse_username` | `str` | — | ClickHouse username |
| `clickhouse_password` | `str` | — | ClickHouse password |
| `mssql_host` | `str` | `"localhost"` | SQL Server host |
| `mssql_port` | `int` | `1433` | SQL Server port |
| `mssql_database` | `str` | — | SQL Server database name |
| `mssql_username` | `str` | — | SQL Server username |
| `mssql_password` | `str` | — | SQL Server password |
| `athena_query_result_bucket` | `str` | — | S3 bucket for query results |
| `athena_database` | `str` | `"default"` | Athena database name |
| `athena_aws_access_key_id` | `str` | — | AWS access key ID |
| `athena_aws_secret_access_key` | `str` | — | AWS secret access key |
| `athena_region` | `str` | `"us-east-1"` | AWS region |
| `mysql_host` | `str` | `"localhost"` | MySQL server host |
| `mysql_port` | `int` | `3306` | MySQL server port |
| `mysql_database` | `str` | — | MySQL database name |
| `mysql_username` | `str` | — | MySQL username |
| `mysql_password` | `str` | — | MySQL password |
| `filesystem_bucket_path` | `str` | — | Local or S3 path |
| `filesystem_format` | `str` | `"parquet"` | Output file format |
| `synapse_host` | `str` | — | Azure Synapse workspace hostname |
| `synapse_database` | `str` | — | Synapse database name |
| `synapse_username` | `str` | — | Synapse username |
| `synapse_password` | `str` | — | Synapse password |
| `use_environment_routing` | `bool` | `false` | Automatically select destination based on Dagster deployment environment |
| `destination_local` | `str` | — | Destination for local development |
| `destination_branch` | `str` | — | Destination for branch deployments |
| `destination_prod` | `str` | — | Destination for production |

<!-- FIELDS:END -->
