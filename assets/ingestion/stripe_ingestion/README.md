# Stripe Ingestion Component


Ingest [Stripe](https://stripe.com) payment and billing data using [dlt](https://dlthub.com)'s verified `stripe_analytics` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `api_key` | `str` | yes | Stripe API Secret Key (`sk_live_...` or `sk_test_...`) |
| `resources` | `str` | no | Comma-separated endpoints — defaults to `"customers,subscriptions,charges"`. Available: `customers`, `subscriptions`, `charges`, `invoices`, `products`, `prices`, `payment_intents`, `balance_transactions`, `events` |
| `start_date` | `Optional[str]` | no | YYYY-MM-DD start of extraction window (defaults to all historical data) |
| `end_date` | `Optional[str]` | no | YYYY-MM-DD end of extraction window (defaults to today) |
| `incremental` | `bool` | no | Use append-mode incremental loading (default `false`, requires `start_date`) |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.StripeIngestionComponent
attributes:
  asset_name: stripe_data
  # ... source config ...
  destination: snowflake          # or bigquery, postgres, filesystem, ...
  dataset_name: stripe_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.StripeIngestionComponent
attributes:
  asset_name: stripe_data
  api_key: "{{ env('STRIPE_API_KEY') }}"
  resources: "customers,subscriptions,charges"
  start_date: "2024-01-01"
  group_name: stripe
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Snowflake

```yaml
type: dagster_component_templates.StripeIngestionComponent
attributes:
  asset_name: stripe_data
  api_key: "{{ env('STRIPE_API_KEY') }}"
  resources: "customers,subscriptions,charges,invoices"
  start_date: "2024-01-01"
  incremental: true
  destination: snowflake
  dataset_name: stripe_raw
  persist_only: true
```

Set `DESTINATION__SNOWFLAKE__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Incremental vs full refresh**: set `incremental: true` AND `start_date` to use append-mode loading. Otherwise the component does a full refresh.
- **Test vs live**: same component handles both — just swap the API key.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Learn more

- [dlt Stripe source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/stripe)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
