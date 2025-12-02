# Stripe Ingestion Component

Ingest payment and subscription data from Stripe (customers, charges, subscriptions, invoices) using dlt's verified Stripe source.

## Overview

This component uses [dlt (data load tool)](https://dlthub.com) to extract data from Stripe.

dlt handles:
- ✅ API authentication and rate limiting
- ✅ Automatic pagination and incremental loading
- ✅ Schema evolution and data type inference
- ✅ Retry logic and error handling

## Use Cases

- **Payment Analytics**: Track charges, refunds, payment methods
- **Subscription Metrics**: Calculate MRR, churn, LTV
- **Revenue Recognition**: Financial reporting and forecasting
- **Customer Billing**: Analyze payment patterns and failures
- **Fraud Detection**: Monitor suspicious transaction patterns

## Data Types

**Available Resources:**
Customers, Charges, Subscriptions, Invoices, Payment Intents, Balance Transactions

## Output Modes

This component supports two output modes:

### 1. DataFrame Mode (Default)
Returns data as a pandas DataFrame for downstream processing in Dagster Designer.

```yaml
type: dagster_component_templates.StripeIngestionComponent
attributes:
  asset_name: stripe_data
  # ... authentication parameters ...
```

### 2. Database Persistence Mode
Persist data directly to a database (Snowflake, BigQuery, Postgres, DuckDB, etc.) using dlt's destination capabilities.

**Option A: Persist Only**
```yaml
type: dagster_component_templates.StripeIngestionComponent
attributes:
  asset_name: stripe_data
  destination: "snowflake"  # or bigquery, postgres, duckdb, etc.
  destination_config: "snowflake://user:pass@account/database/schema"
  persist_and_return: false  # Only persist, don't return DataFrame
  # ... authentication parameters ...
```

**Option B: Persist AND Return DataFrame**
```yaml
type: dagster_component_templates.StripeIngestionComponent
attributes:
  asset_name: stripe_data
  destination: "snowflake"
  destination_config: "snowflake://user:pass@account/database/schema"
  persist_and_return: true  # Persist to DB AND return DataFrame
  # ... authentication parameters ...
```

## Configuration Parameters

### Database Destination Parameters (Optional)

- **`destination`** (string, optional): dlt destination name
  - Supported: `snowflake`, `bigquery`, `postgres`, `redshift`, `duckdb`, `motherduck`, `databricks`, `synapse`, `clickhouse`, and [more](https://dlthub.com/docs/dlt-ecosystem/destinations)
  - Default: Uses in-memory DuckDB and returns DataFrame

- **`destination_config`** (string, optional): Destination configuration
  - Format depends on destination (connection string, JSON config, or credentials file path)
  - Required if `destination` is set
  - Examples:
    - Postgres: `postgresql://user:pass@host:5432/database`
    - Snowflake: `snowflake://user:pass@account/database/schema`
    - BigQuery: Path to service account JSON or credentials dict

- **`persist_and_return`** (boolean, optional): Persistence behavior
  - `false` (default): Only persist to database, return metadata DataFrame
  - `true`: Persist to database AND return full DataFrame
  - Only applies when `destination` is set

### Standard Parameters

- **`asset_name`** (string, required): Name for the output asset
- **`description`** (string, optional): Asset description
- **`group_name`** (string, optional): Asset group for organization
- **`include_sample_metadata`** (boolean, optional): Include data preview in metadata (default: true)

### Source-Specific Parameters

See `schema.json` for complete list of authentication and configuration parameters specific to Stripe.

## Destination Examples

### Snowflake

```yaml
attributes:
  destination: "snowflake"
  destination_config: |
    {
      "credentials": {
        "database": "analytics",
        "password": "${SNOWFLAKE_PASSWORD}",
        "username": "dlt_user",
        "host": "account.snowflakecomputing.com",
        "warehouse": "transforming",
        "role": "dlt_role"
      }
    }
  persist_and_return: false
```

### BigQuery

```yaml
attributes:
  destination: "bigquery"
  destination_config: "/path/to/service-account.json"
  persist_and_return: false
```

### Postgres

```yaml
attributes:
  destination: "postgres"
  destination_config: "postgresql://user:password@localhost:5432/analytics"
  persist_and_return: false
```

### DuckDB (Local File)

```yaml
attributes:
  destination: "duckdb"
  destination_config: "/path/to/analytics.duckdb"
  persist_and_return: true  # Can return DataFrame from local DB
```

## When to Use Each Mode

### Use DataFrame Mode When:
- Building data pipelines in Dagster Designer
- Chaining transformations (standardizers, analytics)
- Need to process data before storing
- Want visual workflow composition

### Use Database Persistence When:
- Direct data warehouse loading
- High-volume data (millions+ rows)
- Long-term storage and querying
- BI tool integration (Tableau, Looker, etc.)
- Production data pipelines

### Use Persist + Return When:
- Need both warehouse copy AND downstream processing
- Debugging/monitoring workflows
- Hybrid architectures (some data to warehouse, some to next step)

## Performance Considerations

- **DataFrame Mode**: Holds data in memory, suitable for up to ~10M rows
- **Persist Only**: Streams directly to destination, handles billions of rows
- **Persist + Return**: Loads twice (destination + memory), use selectively

## Authentication

Refer to `schema.json` and the [dlt documentation](https://dlthub.com/docs) for authentication requirements specific to Stripe.

Common patterns:
- API keys via environment variables
- OAuth tokens
- Service account credentials
- Connection strings

## Dependencies

- `dlt[stripe]` - Installs dlt with Stripe source
- `pandas>=1.5.0` - For DataFrame operations

## Notes

- **Incremental Loading**: dlt automatically tracks state for incremental loads
- **Schema Evolution**: dlt handles schema changes automatically
- **Rate Limiting**: dlt respects API rate limits automatically
- **Retries**: Built-in retry logic for transient failures
- **Destinations**: See [dlt destinations](https://dlthub.com/docs/dlt-ecosystem/destinations) for full list
- **Credentials**: Use environment variables for sensitive values (e.g., `${VAR_NAME}`)

## Learn More

- [dlt Documentation](https://dlthub.com/docs)
- [dlt Stripe Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/stripe)
- [dlt Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations)
