# Configuring destinations for ingestion components

Every dlt-based ingestion component in this directory exposes the same five
destination-related fields. This document is the single source of truth for
how to use them. Per-component READMEs only document source-specific config
(API tokens, resource selection, etc.) and link here for destination details.

> dlt's destination ecosystem is the authoritative reference:
> <https://dlthub.com/docs/dlt-ecosystem/destinations>. We don't restate every
> credential field here — just the patterns and the most common destinations.

## The 5 fields

| Field | Type | Default | Purpose |
|---|---|---|---|
| `destination` | `str?` | `null` | dlt destination identifier (e.g. `snowflake`, `bigquery`, `filesystem`). When unset, the component uses an in-memory DuckDB pipeline and returns a DataFrame. |
| `dataset_name` | `str?` | `null` | Target dataset / schema in the destination. Defaults to the asset name. |
| `persist_only` | `bool` | `false` | When `true` and `destination` is set, the asset emits a `MaterializeResult` and skips DataFrame return. When `false`, the asset queries the destination back into a DataFrame (only meaningful for SQL destinations). |
| `destination_credentials_url` | `str?` | `null` | Inline connection string passed to dlt's destination factory (`dlt.destinations.<name>(credentials=...)`). Useful for projects that ingest into multiple accounts of the same destination type. |
| `destination_credentials_env_var` | `str?` | `null` | Alternative to `destination_credentials_url`: name of an env var holding the connection string. Resolved at run-time. |

## Four operating modes

### Mode A — DuckDB → DataFrame *(default)*

Leave `destination` unset. The component runs an in-memory DuckDB pipeline,
queries the result back, and returns a pandas DataFrame. Best for chaining
into transformations and sinks elsewhere in your asset graph.

```yaml
attributes:
  asset_name: github_repos
  # ... source config ...
```

### Mode B — Persist to a destination, single account

Set `destination`. dlt resolves credentials from environment variables using
the convention `DESTINATION__<NAME>__CREDENTIALS__<FIELD>`.

```yaml
attributes:
  asset_name: github_repos
  destination: snowflake
  dataset_name: github_raw
  persist_only: true
```

```bash
DESTINATION__SNOWFLAKE__CREDENTIALS__HOST=account.snowflakecomputing.com
DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME=loader
DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD=...
DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE=ANALYTICS
DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE=LOADING
```

### Mode C — Multiple accounts of the same destination type

dlt's env-var lookup checks **pipeline-scoped** vars before falling back to
the global form. Each ingestion component builds a unique pipeline name
(`<asset_name>_pipeline`), so you can configure independent credentials per
asset by prefixing the env var with that pipeline name:

```bash
# Asset 1 — production Snowflake account
prod_orders_pipeline__DESTINATION__SNOWFLAKE__CREDENTIALS__HOST=prod.snowflakecomputing.com
prod_orders_pipeline__DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD=prod_password
prod_orders_pipeline__DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE=PROD

# Asset 2 — staging Snowflake account
staging_users_pipeline__DESTINATION__SNOWFLAKE__CREDENTIALS__HOST=staging.snowflakecomputing.com
staging_users_pipeline__DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD=staging_password
staging_users_pipeline__DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE=STAGING
```

No code change to the component — pick a different asset name and you get
a different credential namespace.

### Mode D — Inline credentials per asset

For projects that prefer to thread credentials through the YAML directly
(or through Dagster's resource system), set `destination_credentials_url`
or `destination_credentials_env_var`. The component will use dlt's
destination factory to inject them.

```yaml
attributes:
  asset_name: github_repos
  destination: postgres
  dataset_name: github_raw
  destination_credentials_url: "postgresql://loader:pw@db.internal:5432/warehouse"
```

```yaml
attributes:
  asset_name: github_repos
  destination: snowflake
  destination_credentials_env_var: PROD_SNOWFLAKE_DSN  # holds e.g. snowflake://user:pw@account/db?warehouse=wh
```

Inline credentials take precedence over env-var-resolved credentials.
Pipeline-scoped env vars (Mode C) still apply for any fields the inline
URL leaves blank — dlt will fill them in from the environment.

## Supported destinations

dlt supports ~24 destinations grouped by category. We document credential
quick references for the most common ones below; for the rest, follow the
link to dlt's destination page.

### Data warehouses & analytics

| `destination` | Service | dlt docs |
|---|---|---|
| `snowflake` | Snowflake | <https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake> |
| `bigquery` | Google BigQuery | <https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery> |
| `redshift` | Amazon Redshift | <https://dlthub.com/docs/dlt-ecosystem/destinations/redshift> |
| `databricks` | Databricks | <https://dlthub.com/docs/dlt-ecosystem/destinations/databricks> |
| `synapse` | Azure Synapse | <https://dlthub.com/docs/dlt-ecosystem/destinations/synapse> |
| `athena` | AWS Athena / Glue Catalog | <https://dlthub.com/docs/dlt-ecosystem/destinations/athena> |
| `fabric` | Microsoft Fabric Warehouse | <https://dlthub.com/docs/dlt-ecosystem/destinations/fabric> |

### OLTP & SQL databases

| `destination` | Service | dlt docs |
|---|---|---|
| `postgres` | PostgreSQL | <https://dlthub.com/docs/dlt-ecosystem/destinations/postgres> |
| `mssql` | Microsoft SQL Server | <https://dlthub.com/docs/dlt-ecosystem/destinations/mssql> |
| `duckdb` | DuckDB (local file or in-memory) | <https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb> |
| `motherduck` | MotherDuck | <https://dlthub.com/docs/dlt-ecosystem/destinations/motherduck> |
| `clickhouse` | ClickHouse | <https://dlthub.com/docs/dlt-ecosystem/destinations/clickhouse> |
| `dremio` | Dremio (experimental) | <https://dlthub.com/docs/dlt-ecosystem/destinations/dremio> |
| `sqlalchemy` | 30+ SQL databases via SQLAlchemy | <https://dlthub.com/docs/dlt-ecosystem/destinations/sqlalchemy> |

### Lake formats

| `destination` | Service | dlt docs |
|---|---|---|
| `delta` | Delta Lake | <https://dlthub.com/docs/dlt-ecosystem/destinations/delta> |
| `iceberg` | Apache Iceberg | <https://dlthub.com/docs/dlt-ecosystem/destinations/iceberg> |
| `ducklake` | DuckLake | <https://dlthub.com/docs/dlt-ecosystem/destinations/ducklake> |

### Cloud storage / object stores

| `destination` | Service | dlt docs |
|---|---|---|
| `filesystem` | S3 / GCS / Azure Blob / local FS | <https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem> |

### Vector stores & ML

| `destination` | Service | dlt docs |
|---|---|---|
| `weaviate` | Weaviate | <https://dlthub.com/docs/dlt-ecosystem/destinations/weaviate> |
| `qdrant` | Qdrant | <https://dlthub.com/docs/dlt-ecosystem/destinations/qdrant> |
| `lancedb` | LanceDB | <https://dlthub.com/docs/dlt-ecosystem/destinations/lancedb> |
| `lance` | Lance columnar format | <https://dlthub.com/docs/dlt-ecosystem/destinations/lancedb> |
| `huggingface` | Hugging Face Datasets | <https://dlthub.com/docs/dlt-ecosystem/destinations/huggingface> |

For destinations not listed above, see
<https://dlthub.com/docs/dlt-ecosystem/destinations>.

## Env-var quick reference (common destinations)

These are the env vars dlt expects in **Mode B** (single account) or **Mode C**
(prefixed with `<pipeline_name>__` for multi-account setups). Field names follow
dlt's docs — check the linked dlt page above for the authoritative list.

### snowflake

| Env var | Required |
|---|---|
| `DESTINATION__SNOWFLAKE__CREDENTIALS__HOST` | yes — `<account>.<region>.snowflakecomputing.com` |
| `DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME` | yes |
| `DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD` | yes (or use `PRIVATE_KEY` for key-pair auth) |
| `DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE` | yes |
| `DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE` | yes |
| `DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE` | optional |

### bigquery

| Env var | Required |
|---|---|
| `DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID` | yes |
| `DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY` | yes (service-account JSON's `private_key`) |
| `DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL` | yes (service-account JSON's `client_email`) |
| `DESTINATION__BIGQUERY__LOCATION` | optional — region for new datasets |

When running on GCP with workload-identity, you can omit credentials entirely
and dlt will use Application Default Credentials.

### postgres / redshift / mssql / mysql / clickhouse

These all share the same shape:

| Env var | Required |
|---|---|
| `DESTINATION__<NAME>__CREDENTIALS__HOST` | yes |
| `DESTINATION__<NAME>__CREDENTIALS__PORT` | optional (defaults to driver default) |
| `DESTINATION__<NAME>__CREDENTIALS__USERNAME` | yes |
| `DESTINATION__<NAME>__CREDENTIALS__PASSWORD` | yes |
| `DESTINATION__<NAME>__CREDENTIALS__DATABASE` | yes |

`<NAME>` is one of `POSTGRES`, `REDSHIFT`, `MSSQL`, `MYSQL`, `CLICKHOUSE`.

### databricks

| Env var | Required |
|---|---|
| `DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME` | yes — workspace URL host |
| `DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH` | yes — SQL warehouse path |
| `DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN` | yes |
| `DESTINATION__DATABRICKS__CREDENTIALS__CATALOG` | optional |
| `DESTINATION__DATABRICKS__CREDENTIALS__SCHEMA` | optional |

### filesystem (S3 / GCS / Azure / local)

The bucket URL drives which backend is used. Backend-specific credentials
follow.

| Env var | Required |
|---|---|
| `DESTINATION__FILESYSTEM__BUCKET_URL` | yes — `s3://...`, `gs://...`, `az://...`, or a local path |

For **S3**:
- `DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID`
- `DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY`
- `DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME`

For **GCS**:
- `DESTINATION__FILESYSTEM__CREDENTIALS__PROJECT_ID`
- `DESTINATION__FILESYSTEM__CREDENTIALS__PRIVATE_KEY`
- `DESTINATION__FILESYSTEM__CREDENTIALS__CLIENT_EMAIL`

For **Azure**:
- `DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME`
- `DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY` *(or SAS token)*

### athena

| Env var | Required |
|---|---|
| `DESTINATION__ATHENA__QUERY_RESULT_BUCKET` | yes — `s3://...` for query results staging |
| `DESTINATION__ATHENA__CREDENTIALS__AWS_ACCESS_KEY_ID` | yes |
| `DESTINATION__ATHENA__CREDENTIALS__AWS_SECRET_ACCESS_KEY` | yes |
| `DESTINATION__ATHENA__CREDENTIALS__REGION_NAME` | yes |

### duckdb / motherduck

DuckDB is local — no credentials.

| Env var | Required |
|---|---|
| `DESTINATION__DUCKDB__CREDENTIALS` | optional — file path; `:memory:` if unset |
| `DESTINATION__MOTHERDUCK__CREDENTIALS__DATABASE` | yes |
| `DESTINATION__MOTHERDUCK__CREDENTIALS__TOKEN` | yes |

## Notes

- **DataFrame return for non-SQL destinations**: when `persist_only=false` and
  the destination isn't SQL-backed (e.g. `filesystem`, `weaviate`, `qdrant`),
  the asset cannot query data back into a DataFrame. The component logs a
  warning and emits a `MaterializeResult` instead. Set `persist_only=true`
  explicitly for these destinations to avoid the warning.
- **Inline credentials precedence**: `destination_credentials_url` (or
  `_env_var`) is passed to dlt's destination factory and takes precedence
  over `DESTINATION__*` env vars for that pipeline run.
- **Long-tail destinations**: if dlt's destination identifier doesn't match
  any factory exposed under `dlt.destinations.<name>`, the component falls
  back to passing the raw destination string and lets dlt resolve via env vars.
