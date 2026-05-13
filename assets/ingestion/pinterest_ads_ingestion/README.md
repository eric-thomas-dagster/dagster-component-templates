# Pinterest Ads Ingestion Component

Ingest Pinterest Ads data (campaigns, ad groups, ads, pins, keywords, analytics) using [dlt](https://dlthub.com)'s REST API source against the Pinterest Marketing API.

## Overview

dlt handles authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.

## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `access_token` | `str` | yes | Pinterest OAuth 2.0 Access Token with `ads:read` scope (use env var: `"${PINTEREST_ACCESS_TOKEN}"`) |
| `ad_account_ids` | `str` | yes | Comma-separated Pinterest Ad Account IDs |
| `app_id` | `str` | no | Pinterest App ID (for token refresh) |
| `app_secret` | `str` | no | Pinterest App Secret (for token refresh) |
| `resources` | `str` | no | Comma-separated resource list. Default `"campaigns,analytics"`. Available: `ad_accounts`, `campaigns`, `ad_groups`, `ads`, `pins`, `keywords`, `analytics` |
| `start_date` | `str` | no | Analytics start date (YYYY-MM-DD). Defaults to 30 days ago |
| `end_date` | `str` | no | Analytics end date (YYYY-MM-DD). Defaults to today |
| `granularity` | `str` | no | Analytics granularity: `DAY` (default), `HOUR`, `WEEK`, `MONTH` |
| `level` | `str` | no | Aggregation level: `CAMPAIGN` (default), `AD_ACCOUNT`, `AD_GROUP`, `AD`, `KEYWORD`, `PIN_PROMOTION` |
| `columns` | `str` | no | Comma-separated metric columns. Empty uses default metrics |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.

## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:

```yaml
type: dagster_component_templates.PinterestAdsIngestionComponent
attributes:
  asset_name: pinterest_ads
  access_token: "${PINTEREST_ACCESS_TOKEN}"
  ad_account_ids: "123456789,987654321"
  destination: bigquery            # or snowflake, postgres, filesystem, ...
  dataset_name: pinterest_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__BIGQUERY__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.

**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**

## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.PinterestAdsIngestionComponent
attributes:
  asset_name: pinterest_ads_data
  access_token: "${PINTEREST_ACCESS_TOKEN}"
  ad_account_ids: "123456789,987654321"
  resources: "campaigns,ad_groups,ads,analytics"
  start_date: "2024-10-01"
  end_date: "2024-12-31"
  granularity: "DAY"
  level: "AD_GROUP"
  group_name: pinterest_ads
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.

## Example — persist to BigQuery

```yaml
type: dagster_component_templates.PinterestAdsIngestionComponent
attributes:
  asset_name: pinterest_ads
  access_token: "${PINTEREST_ACCESS_TOKEN}"
  ad_account_ids: "123456789"
  resources: "campaigns,ad_groups,ads,pins,analytics"
  destination: bigquery
  dataset_name: pinterest_raw
  persist_only: true
```

Set `DESTINATION__BIGQUERY__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.

## Notes

- **Incremental loading**: dlt tracks state across runs; configure incremental cursor on date fields if needed.
- **Schema evolution**: dlt accommodates new fields without manual migrations.
- **Token expiration**: Pinterest OAuth tokens expire after 1 year — implement a refresh flow for production.
- **Data latency**: Pinterest performance metrics typically lag 24–48 hours.
- **Pin-level analytics**: use `level: PIN_PROMOTION` for the most granular creative insights.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.


## Multiple accounts (multi-tenant)

To pull from multiple accounts / customers, create one `defs/<name>/defs.yaml` per instance — each referencing its own env var for credentials:

```yaml
# defs/customer_a/defs.yaml
type: ...
attributes:
  asset_name: customer_a_data
  # Replace `<credential_field>` with this component's auth field name (see Configuration above):
  <credential_field>: "{{ env('<VENDOR>_<CREDENTIAL>_CUSTOMER_A') }}"
  # ... other source config (account-specific IDs, etc.) ...
```

```yaml
# defs/customer_b/defs.yaml
type: ...
attributes:
  asset_name: customer_b_data
  <credential_field>: "{{ env('<VENDOR>_<CREDENTIAL>_CUSTOMER_B') }}"
  # ...
```

Both assets show up independently in the Dagster catalog with their own credentials, schedules, freshness policies, etc. See [`../../MULTI_INSTANCE.md`](../../MULTI_INSTANCE.md) for the full pattern (works for any component with credentials), a Python scaffold script for many tenants, multi-destination combinations, and when partitioning a single asset by customer is the better choice.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.

## Learn more

- [Pinterest Marketing API v5](https://developers.pinterest.com/docs/api/v5/)
- [dlt REST API source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
