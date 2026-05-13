# External Iceberg Table

Declare an Apache Iceberg table that exists OUTSIDE Dagster (written by Snowflake, Trino, Spark, Flink, Databricks, …) as an observable external Dagster asset.

This is the **declaration** — the table shows up in Dagster's asset graph with proper kinds + metadata. Pair with [`iceberg_ingestion`](../../assets/ingestion/iceberg_ingestion/) if you need to also read it.

## When to use

Use this when you want to:
- Show the lineage cleanly: "Snowflake writes this Iceberg table → Dagster reads downstream"
- Tag the asset with `owner_engine` (snowflake / trino / spark / …) so it's visible who owns it
- Establish a dependency edge from Dagster assets onto the external table without materializing it

## Engine-agnostic vs `external_databricks_table`

| | This component | [`external_databricks_table`](https://dagster-community-components-cli.vercel.app/c/external_databricks_table) |
|---|---|---|
| Scope | Any Iceberg writer (Snowflake / Trino / Spark / …) | Databricks Delta-via-UC specifically |
| Format | Iceberg | Delta |
| Metadata model | PyIceberg catalog / namespace / table | Databricks workspace / catalog / schema / table |

Use both side-by-side if you mix engines.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_key` | `str` | yes | Slash-delimited |
| `catalog_name` | `str` | yes | Iceberg catalog handle |
| `namespace` / `table_name` | `str` | yes | |
| `warehouse` | `str` | no | Metadata only |
| `catalog_type` | `str` | no | `rest` / `glue` / `hive` / `hadoop` / `sql` |
| `owner_engine` | `str` | no | Adds to asset kinds + metadata |
| Standard fields | | | group / description / owners / tags / partition |

## See also

- [`iceberg_ingestion`](../../assets/ingestion/iceberg_ingestion/) — read this table
- [`dataframe_to_iceberg_table`](../../assets/sinks/dataframe_to_iceberg_table/) — write to it
- [`external_delta_table`](../external_delta_table/) — sister component for Delta
- [`external_databricks_table`](https://dagster-community-components-cli.vercel.app/c/external_databricks_table) — Databricks-specific equivalent
