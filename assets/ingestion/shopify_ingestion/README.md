# Shopify Ingestion Component


Ingest [Shopify](https://www.shopify.com) e-commerce data using [dlt](https://dlthub.com)'s verified `shopify_dlt` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `shop_url` | `str` | yes | Shopify store URL (e.g. `https://my-shop.myshopify.com`) |
| `private_app_password` | `str` | yes | Admin API access token from a Shopify Custom App |
| `resources` | `List[str]` | no | Defaults to `["customers", "orders", "products"]` |
| `start_date` | `Optional[str]` | no | Incremental cursor start (default `"2000-01-01"`) |
| `order_status` | `Optional[str]` | no | Order status filter — `open`, `closed`, `cancelled`, `any` (default) |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_sample_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.ShopifyIngestionComponent
attributes:
  asset_name: shopify_data
  # ... source config ...
  destination: snowflake          # or bigquery, postgres, filesystem, ...
  dataset_name: shopify_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.ShopifyIngestionComponent
attributes:
  asset_name: shopify_data
  shop_url: "https://my-shop.myshopify.com"
  private_app_password: "{{ env('SHOPIFY_PASSWORD') }}"
  resources: [customers, orders, products]
  start_date: "2024-01-01"
  group_name: shopify
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Snowflake

```yaml
type: dagster_component_templates.ShopifyIngestionComponent
attributes:
  asset_name: shopify_data
  shop_url: "https://my-shop.myshopify.com"
  private_app_password: "{{ env('SHOPIFY_PASSWORD') }}"
  resources: [customers, orders, products]
  start_date: "2024-01-01"
  destination: snowflake
  dataset_name: shopify_raw
  persist_only: true
```

Set `DESTINATION__SNOWFLAKE__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Incremental loading**: dlt tracks state from `start_date` forward across runs.
- **Custom App**: create one in your Shopify admin to obtain `private_app_password`.
- **Order status**: defaults to `any` to include cancelled orders. Set `closed` for completed-only.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Learn more

- [dlt Shopify source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/shopify)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
