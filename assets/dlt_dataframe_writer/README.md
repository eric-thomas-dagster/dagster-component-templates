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
