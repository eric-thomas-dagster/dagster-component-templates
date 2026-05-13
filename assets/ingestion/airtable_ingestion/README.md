# Airtable Ingestion Component


Ingest [Airtable](https://airtable.com) base data using [dlt](https://dlthub.com)'s verified `airtable` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `api_key` | `str` | yes | Airtable API key (use env var: `"{{ env('AIRTABLE_API_KEY') }}"`) |
| `base_id` | `str` | yes | Airtable base ID (starts with `app...`) |
| `table_names` | `List[str]` | yes | Tables to extract from the base |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.AirtableIngestionComponent
attributes:
  asset_name: airtable_data
  # ... source config ...
  destination: snowflake          # or bigquery, postgres, filesystem, ...
  dataset_name: airtable_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.AirtableIngestionComponent
attributes:
  asset_name: airtable_data
  api_key: "{{ env('AIRTABLE_API_KEY') }}"
  base_id: "appXXXXXXXXXXXXXX"
  table_names: [Customers, Orders, Products]
  group_name: airtable
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Snowflake

```yaml
type: dagster_component_templates.AirtableIngestionComponent
attributes:
  asset_name: airtable_data
  api_key: "{{ env('AIRTABLE_API_KEY') }}"
  base_id: "appXXXXXXXXXXXXXX"
  table_names: [Customers, Orders]
  destination: snowflake
  dataset_name: airtable_raw
  persist_only: true
```

Set `DESTINATION__SNOWFLAKE__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Incremental loading**: dlt tracks state automatically across runs.
- **Schema evolution**: dlt accommodates new fields without manual migrations.
- **Table names**: spaces and dashes in Airtable table names are normalized to underscores when querying back from SQL destinations.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Multiple Airtable accounts (multi-tenant)

To pull from multiple Airtable bases / customers, create one `defs/<name>/defs.yaml` per instance, each referencing its own env var:

```yaml
# defs/airtable_customer_a/defs.yaml
attributes:
  asset_name: customer_a_airtable
  api_key: "{{ env('AIRTABLE_API_KEY_CUSTOMER_A') }}"
  base_id: appAAAAAAAAAAAAAA
  ...
```

```yaml
# defs/airtable_customer_b/defs.yaml
attributes:
  asset_name: customer_b_airtable
  api_key: "{{ env('AIRTABLE_API_KEY_CUSTOMER_B') }}"
  base_id: appBBBBBBBBBBBBBB
  ...
```

See [`../../MULTI_INSTANCE.md`](../../MULTI_INSTANCE.md) for the general pattern (works for any component with credentials), a Python scaffold script for many tenants, and notes on when partitioning a single asset by customer is the better choice.

## Learn more

- [dlt Airtable source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/airtable)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — multi-destination writes
- [`../../MULTI_INSTANCE.md`](../../MULTI_INSTANCE.md) — multi-tenant pattern
