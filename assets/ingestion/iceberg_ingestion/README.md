# Iceberg Ingestion Component

Read from an **existing external** Apache Iceberg table — one written by Snowflake, Trino, Spark, Flink, Databricks, or any other engine — into a Dagster asset as a pandas DataFrame.

## When this is the right component

| | Use this | Use [`iceberg_io_manager`](https://dagster-community-components-cli.vercel.app/c/iceberg_io_manager) |
|---|---|---|
| Who owns the table | Some other engine (Snowflake, Trino, Spark, ...) | Dagster |
| What you do | Read existing snapshots | Materialize from scratch each run |
| Pattern | "Cross-engine lake — Dagster is one reader among many" | "Dagster pipeline writes Iceberg as output" |

The official `dagster_iceberg` package ships only the IO manager side (`IcebergPyarrowIOManager`). This component fills the **read-from-existing** gap with PyIceberg directly.

## Catalogs supported

PyIceberg's `load_catalog(...)` handles all the standard catalog implementations:

| Catalog type | What it is | `catalog_type:` |
|---|---|---|
| **REST** | Generic Iceberg REST spec — Nessie, Polaris (Apache), Lakekeeper, Tabular, S3 Tables, Snowflake-managed catalogs | `rest` |
| **AWS Glue** | Glue Data Catalog | `glue` |
| **Hive Metastore** | HMS Thrift API | `hive` |
| **Hadoop** | Filesystem-only (metadata next to data) | `hadoop` |
| **SQL** | Metadata in Postgres / MySQL / SQLite | `sql` |

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_name` | `str` | yes | |
| `catalog_type` | `str` | no | Default `rest` |
| `catalog_name` | `str` | no | Default `default` — PyIceberg local handle |
| `catalog_properties` | `dict` | yes | Catalog-specific config. `${ENV_VAR}` expansion supported |
| `namespace` | `str` | yes | Iceberg namespace (e.g. `sales`) |
| `table_name` | `str` | yes | Iceberg table |
| `select_columns` | `list` | no | Column projection |
| `row_filter` | `str` | no | Iceberg expression for predicate pushdown. `{partition_key}` substitution supported |
| `snapshot_id` | `int` | no | Time travel: exact snapshot |
| `as_of_timestamp_ms` | `int` | no | Time travel: snapshot active at this timestamp (mutually exclusive with `snapshot_id`) |
| `branch` | `str` | no | Iceberg branching (v2 tables) |
| `limit` | `int` | no | Row limit |
| Standard fields | | | partition / freshness / owners / tags / deps / retry |

## Catalog configs

### REST — Nessie / Polaris / Lakekeeper / Tabular

```yaml
catalog_type: rest
catalog_properties:
  uri: https://catalog.example.com
  credential: "${CATALOG_CREDENTIAL}"   # OAuth: 'client_id:client_secret'
  # token: "${BEARER_TOKEN}"            # alt: bearer token
  warehouse: my_warehouse
```

### REST — Snowflake-managed Iceberg catalog

```yaml
catalog_type: rest
catalog_properties:
  uri: https://abc-xy12345.snowflakecomputing.com/api/v2/catalogs/MY_CAT/iceberg
  credential: "${SNOWFLAKE_PAT}"
  warehouse: MY_WAREHOUSE
  scope: PRINCIPAL_ROLE:MY_ROLE
```

### REST — S3 Tables (AWS)

```yaml
catalog_type: rest
catalog_properties:
  uri: https://s3tables.us-east-1.amazonaws.com/iceberg
  warehouse: arn:aws:s3tables:us-east-1:123456789012:bucket/my-tables-bucket
  # IAM via env / instance profile / IRSA
```

### AWS Glue

```yaml
catalog_type: glue
catalog_properties:
  warehouse: s3://my-bucket/iceberg/
  # AWS_PROFILE / instance profile / IRSA for auth
```

### Hadoop (filesystem)

```yaml
catalog_type: hadoop
catalog_properties:
  warehouse: s3://my-bucket/iceberg-warehouse/
```

## Time travel

Three mutually-exclusive ways to point at a non-current snapshot:

```yaml
# Specific snapshot (deterministic — best for re-runs)
snapshot_id: 12345

# Snapshot active at a Unix-millis timestamp
as_of_timestamp_ms: 1716200000000

# Iceberg branch (v2 tables, e.g. 'dev' / 'staging')
branch: dev
```

When backfilling, prefer `snapshot_id` — it's idempotent across Iceberg compactions that rewrite the data layout but preserve snapshot history.

## Partitioning with `{partition_key}`

```yaml
row_filter: order_date >= '{partition_key}T00:00:00' and order_date < '{partition_key_next}T00:00:00'
partition_type: daily
partition_start: '2024-01-01'
```

`row_filter` becomes Iceberg's predicate pushdown — only the matching data files are scanned. Massive perf win vs reading the whole table.

## Storage credentials

Iceberg data files live on S3 / ADLS / GCS. PyIceberg uses fsspec under the hood — credentials come from your environment:

| Cloud | Where credentials come from |
|---|---|
| **S3** | `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`, or instance profile / IRSA / `aws sso login` cache |
| **ADLS** | `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_KEY`, or `DefaultAzureCredential` / workload identity |
| **GCS** | `GOOGLE_APPLICATION_CREDENTIALS` env var pointing at a SA key, or workload identity |

You can also pass these as `catalog_properties` — PyIceberg merges them with the table's own properties.

## Dependencies

```
pyiceberg>=0.6.0
pyarrow>=10.0.0
pandas>=1.5.0
```

Plus catalog/storage extras as needed:

```bash
pip install 'pyiceberg[s3fs,glue]'    # S3 storage + Glue catalog
pip install 'pyiceberg[adlfs]'        # ADLS
pip install 'pyiceberg[gcs]'          # GCS
pip install 'pyiceberg[hive]'         # Hive Metastore
pip install 'pyiceberg[sql-postgres]' # SQL catalog (Postgres-backed)
```

## See also

- [`iceberg_io_manager`](https://dagster-community-components-cli.vercel.app/c/iceberg_io_manager) — when Dagster OWNS the table (output side)
- [`dataframe_to_iceberg_table`](../../sinks/dataframe_to_iceberg_table/) — write to an existing Iceberg table (append / overwrite)
- [`external_iceberg_table`](../../../external_assets/external_iceberg_table/) — declare an Iceberg table as an observable asset
- [`delta_ingestion`](../delta_ingestion/) — sister component for Delta Lake
