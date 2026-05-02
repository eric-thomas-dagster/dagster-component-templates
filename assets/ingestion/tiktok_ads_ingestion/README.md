# TikTok Ads Ingestion Component

Ingest TikTok Ads data (advertisers, campaigns, ad groups, ads, videos, images, reports) using [dlt](https://dlthub.com)'s REST API source against the TikTok Marketing API.

## Overview

dlt handles authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.

## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `access_token` | `str` | yes | TikTok Marketing API Access Token (use env var: `"${TIKTOK_ACCESS_TOKEN}"`) |
| `advertiser_ids` | `str` | yes | Comma-separated TikTok Advertiser IDs |
| `app_id` | `str` | no | TikTok App ID |
| `secret` | `str` | no | TikTok App Secret |
| `resources` | `str` | no | Comma-separated resource list. Default `"campaigns,reports"`. Available: `advertisers`, `campaigns`, `ad_groups`, `ads`, `videos`, `images`, `reports` |
| `start_date` | `str` | no | Report start date (YYYY-MM-DD). Defaults to 30 days ago |
| `end_date` | `str` | no | Report end date (YYYY-MM-DD). Defaults to today |
| `report_type` | `str` | no | `BASIC` (default), `AUDIENCE`, or `PLAYABLE_MATERIAL` |
| `data_level` | `str` | no | `AUCTION_CAMPAIGN` (default), `AUCTION_ADVERTISER`, `AUCTION_ADGROUP`, `AUCTION_AD` |
| `metrics` | `str` | no | Comma-separated metrics. Empty uses default metrics |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.

## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:

```yaml
type: dagster_component_templates.TikTokAdsIngestionComponent
attributes:
  asset_name: tiktok_ads
  access_token: "${TIKTOK_ACCESS_TOKEN}"
  advertiser_ids: "1234567890"
  destination: bigquery            # or snowflake, postgres, filesystem, ...
  dataset_name: tiktok_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__BIGQUERY__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.

**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**

## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.TikTokAdsIngestionComponent
attributes:
  asset_name: tiktok_ads_data
  access_token: "${TIKTOK_ACCESS_TOKEN}"
  advertiser_ids: "1234567890,9876543210"
  resources: "campaigns,ad_groups,ads,reports"
  start_date: "2024-10-01"
  end_date: "2024-12-31"
  data_level: "AUCTION_AD"
  group_name: tiktok_ads
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.

## Example — persist to BigQuery

```yaml
type: dagster_component_templates.TikTokAdsIngestionComponent
attributes:
  asset_name: tiktok_ads
  access_token: "${TIKTOK_ACCESS_TOKEN}"
  advertiser_ids: "1234567890"
  resources: "campaigns,ad_groups,ads,videos,reports"
  destination: bigquery
  dataset_name: tiktok_raw
  persist_only: true
```

Set `DESTINATION__BIGQUERY__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.

## Notes

- **Incremental loading**: dlt tracks state across runs.
- **Schema evolution**: dlt accommodates new fields without manual migrations.
- **Token expiration**: TikTok long-term tokens are valid for 365 days — set up renewal reminders.
- **Data latency**: TikTok performance metrics typically lag 24–48 hours.
- **Video metrics**: only populated for video ad creatives — verify ads have `video_id` set.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.

## Learn more

- [TikTok Marketing API v1.3](https://business-api.tiktok.com/portal/docs)
- [dlt REST API source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
