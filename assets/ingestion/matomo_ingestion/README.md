# Matomo Ingestion Component


Ingest [Matomo](https://matomo.org) analytics report data using [dlt](https://dlthub.com)'s verified `matomo` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `api_url` | `str` | yes | Matomo instance URL (e.g. `https://analytics.example.com`) |
| `api_token` | `str` | yes | Matomo API token |
| `site_id` | `str` | yes | Matomo site ID |
| `reports` | `List[str]` | no | Report types — defaults to `["VisitsSummary", "Actions"]`. Examples: `VisitsSummary`, `Actions`, `Events`, `Goals` |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.MatomoIngestionComponent
attributes:
  asset_name: matomo_data
  # ... source config ...
  destination: postgres          # or bigquery, postgres, filesystem, ...
  dataset_name: matomo_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.MatomoIngestionComponent
attributes:
  asset_name: matomo_data
  api_url: "https://analytics.example.com"
  api_token: "{{ env('MATOMO_API_TOKEN') }}"
  site_id: "1"
  reports: [VisitsSummary, Actions]
  group_name: matomo
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Postgres

```yaml
type: dagster_component_templates.MatomoIngestionComponent
attributes:
  asset_name: matomo_data
  api_url: "https://analytics.example.com"
  api_token: "{{ env('MATOMO_API_TOKEN') }}"
  site_id: "1"
  reports: [VisitsSummary, Actions, Events]
  destination: postgres
  dataset_name: matomo_raw
  persist_only: true
```

Set `DESTINATION__POSTGRES__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Self-hosted vs cloud**: works with both — provide your own `api_url`.
- **Reports**: see [Matomo Reporting API reference](https://developer.matomo.org/api-reference/reporting-api) for the full list.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Learn more

- [dlt Matomo source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/matomo)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
