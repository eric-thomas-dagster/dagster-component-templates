# LinkedIn Ads Ingestion Component

Ingest LinkedIn Ads data (campaigns, creatives, campaign groups, analytics, conversions) using [dlt](https://dlthub.com)'s REST API source against the LinkedIn Marketing API.

## Overview

dlt handles authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.

## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `access_token` | `str` | yes | LinkedIn OAuth 2.0 Access Token with `r_ads` and `r_ads_reporting` scopes (use env var: `"${LINKEDIN_ACCESS_TOKEN}"`) |
| `account_ids` | `str` | yes | Comma-separated LinkedIn Ad Account IDs |
| `resources` | `str` | no | Comma-separated resource list. Default `"campaigns,analytics"`. Available: `accounts`, `campaigns`, `creatives`, `campaign_groups`, `analytics`, `conversions` |
| `start_date` | `str` | no | Analytics start date (YYYY-MM-DD). Defaults to 30 days ago |
| `end_date` | `str` | no | Analytics end date (YYYY-MM-DD). Defaults to today |
| `time_granularity` | `str` | no | `DAILY` (default), `MONTHLY`, or `ALL` (cumulative) |
| `pivot_by` | `str` | no | `CAMPAIGN`, `CREATIVE`, `ACCOUNT`, or `COMPANY` |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_sample_metadata`, `deps` — same convention as every other component in this library.

## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:

```yaml
type: dagster_component_templates.LinkedInAdsIngestionComponent
attributes:
  asset_name: linkedin_ads
  access_token: "${LINKEDIN_ACCESS_TOKEN}"
  account_ids: "123456789"
  destination: bigquery            # or snowflake, postgres, filesystem, ...
  dataset_name: linkedin_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__BIGQUERY__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.

**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**

## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.LinkedInAdsIngestionComponent
attributes:
  asset_name: linkedin_ads_data
  access_token: "${LINKEDIN_ACCESS_TOKEN}"
  account_ids: "123456789,987654321"
  resources: "campaigns,creatives,campaign_groups,analytics"
  start_date: "2024-10-01"
  end_date: "2024-12-31"
  time_granularity: "DAILY"
  pivot_by: "CAMPAIGN"
  group_name: linkedin_ads
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.

## Example — persist to BigQuery

```yaml
type: dagster_component_templates.LinkedInAdsIngestionComponent
attributes:
  asset_name: linkedin_ads
  access_token: "${LINKEDIN_ACCESS_TOKEN}"
  account_ids: "123456789"
  resources: "campaigns,creatives,analytics,conversions"
  destination: bigquery
  dataset_name: linkedin_raw
  persist_only: true
```

Set `DESTINATION__BIGQUERY__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.

## Notes

- **Incremental loading**: dlt tracks state across runs.
- **Schema evolution**: dlt accommodates new fields without manual migrations.
- **API versioning**: this component pins the `LinkedIn-Version` header to `202401`. Bump this if LinkedIn announces a breaking change.
- **OAuth scopes**: tokens require `r_ads` and `r_ads_reporting` scopes — verify your app has those scopes approved.
- **Date format**: LinkedIn's analytics endpoint requires day/month/year as separate fields; this component constructs them from your YYYY-MM-DD inputs.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.

## Learn more

- [LinkedIn Marketing API](https://learn.microsoft.com/en-us/linkedin/marketing/)
- [dlt REST API source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
