# Google Ads Ingestion Component


Ingest [Google Ads](https://ads.google.com) entity and reporting data using [dlt](https://dlthub.com)'s verified `google_ads` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `customer_id` | `str` | yes | Google Ads Customer ID (10-digit, with or without dashes) |
| `developer_token` | `str` | yes | Google Ads API Developer Token |
| `client_id` | `str` | yes | Google OAuth Client ID |
| `client_secret` | `str` | yes | Google OAuth Client Secret |
| `refresh_token` | `str` | yes | Google OAuth Refresh Token |
| `use_service_account` | `bool` | no | Use service account credentials instead of OAuth (default `false`) |
| `project_id` | `Optional[str]` | no | Google Cloud Project ID (service account auth) |
| `service_account_email` | `Optional[str]` | no | Service account email |
| `private_key` | `Optional[str]` | no | Service account private key |
| `impersonated_email` | `Optional[str]` | no | Email to impersonate via DWD |
| `resources` | `str` | no | Comma-separated resources — `customers,campaigns,ad_groups,ads,keywords,change_events` (default: `"customers,campaigns"`) |
| `start_date` | `Optional[str]` | no | YYYY-MM-DD start of metrics window |
| `end_date` | `Optional[str]` | no | YYYY-MM-DD end of metrics window |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.GoogleAdsIngestionComponent
attributes:
  asset_name: google_ads_data
  # ... source config ...
  destination: bigquery          # or bigquery, postgres, filesystem, ...
  dataset_name: google_ads_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.GoogleAdsIngestionComponent
attributes:
  asset_name: google_ads_data
  customer_id: "1234567890"
  developer_token: "{{ env('GOOGLE_ADS_DEV_TOKEN') }}"
  client_id: "{{ env('GOOGLE_ADS_CLIENT_ID') }}"
  client_secret: "{{ env('GOOGLE_ADS_CLIENT_SECRET') }}"
  refresh_token: "{{ env('GOOGLE_ADS_REFRESH_TOKEN') }}"
  resources: "customers,campaigns,ads"
  group_name: google_ads
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Bigquery

```yaml
type: dagster_component_templates.GoogleAdsIngestionComponent
attributes:
  asset_name: google_ads_data
  customer_id: "1234567890"
  developer_token: "{{ env('GOOGLE_ADS_DEV_TOKEN') }}"
  client_id: "{{ env('GOOGLE_ADS_CLIENT_ID') }}"
  client_secret: "{{ env('GOOGLE_ADS_CLIENT_SECRET') }}"
  refresh_token: "{{ env('GOOGLE_ADS_REFRESH_TOKEN') }}"
  resources: "customers,campaigns,ads"
  destination: bigquery
  dataset_name: google_ads_raw
  persist_only: true
```

Set `DESTINATION__BIGQUERY__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Auth modes**: OAuth (default) or service account (`use_service_account: true`). Service-account mode requires Domain-Wide Delegation if you set `impersonated_email`.
- **Date range**: omit `start_date`/`end_date` for default last-30-days window.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Learn more

- [dlt Google Ads source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_ads)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
