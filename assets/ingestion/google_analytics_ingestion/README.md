# Google Analytics 4 Ingestion Component


Ingest [Google Analytics 4](https://analytics.google.com) reporting data using [dlt](https://dlthub.com)'s verified `google_analytics` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `property_id` | `str` | yes | GA4 Property ID (numeric, e.g. `123456789`) |
| `credentials_json` | `Optional[str]` | no | Service account JSON as a string (preferred — overrides individual fields) |
| `project_id` | `Optional[str]` | no | Google Cloud Project ID |
| `client_email` | `Optional[str]` | no | Service account email |
| `private_key` | `Optional[str]` | no | Service account private key |
| `use_oauth` | `bool` | no | Use OAuth instead of service account (default `false`) |
| `client_id` | `Optional[str]` | no | OAuth Client ID (when `use_oauth: true`) |
| `client_secret` | `Optional[str]` | no | OAuth Client Secret |
| `refresh_token` | `Optional[str]` | no | OAuth Refresh Token |
| `dimensions` | `str` | no | Comma-separated GA4 dimension names (default `"date,sessionSource,sessionMedium,deviceCategory"`) |
| `metrics` | `str` | no | Comma-separated GA4 metric names (default `"sessions,totalUsers,screenPageViews,conversions"`) |
| `start_date` | `str` | no | YYYY-MM-DD start (default `"2024-01-01"`) |
| `end_date` | `Optional[str]` | no | YYYY-MM-DD end (default today) |
| `rows_per_page` | `int` | no | API page size (default 10000, max 100000) |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.GoogleAnalyticsIngestionComponent
attributes:
  asset_name: google_analytics_data
  # ... source config ...
  destination: bigquery          # or bigquery, postgres, filesystem, ...
  dataset_name: google_analytics_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.GoogleAnalyticsIngestionComponent
attributes:
  asset_name: ga4_data
  property_id: "123456789"
  credentials_json: "{{ env('GOOGLE_ANALYTICS_CREDENTIALS') }}"
  dimensions: "date,sessionSource,deviceCategory"
  metrics: "sessions,totalUsers,conversions"
  start_date: "2024-01-01"
  group_name: google_analytics
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Bigquery

```yaml
type: dagster_component_templates.GoogleAnalyticsIngestionComponent
attributes:
  asset_name: ga4_data
  property_id: "123456789"
  credentials_json: "{{ env('GOOGLE_ANALYTICS_CREDENTIALS') }}"
  dimensions: "date,city,sessionSource"
  metrics: "sessions,totalUsers,conversions"
  start_date: "2024-01-01"
  destination: bigquery
  dataset_name: ga4_raw
  persist_only: true
```

Set `DESTINATION__BIGQUERY__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Single result table**: extracted data lands in a `ga4_data` table; multiple queries can be supported by extending `queries` in the component.
- **Dimensions/metrics**: see the [GA4 Data API field list](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema).
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Learn more

- [dlt Google Analytics 4 source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_analytics)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
