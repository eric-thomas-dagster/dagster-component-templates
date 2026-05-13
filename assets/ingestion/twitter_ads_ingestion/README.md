# Twitter/X Ads Ingestion Component

Ingest Twitter/X Ads data (campaigns, line items, promoted tweets, analytics) using [dlt](https://dlthub.com)'s REST API source against the Twitter Ads API.

## Overview

dlt handles authentication (OAuth 1.0a), pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.

## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `consumer_key` | `str` | yes | Twitter API Consumer Key (use env var: `"${TWITTER_CONSUMER_KEY}"`) |
| `consumer_secret` | `str` | yes | Twitter API Consumer Secret |
| `access_token` | `str` | yes | Twitter Access Token |
| `access_token_secret` | `str` | yes | Twitter Access Token Secret |
| `account_ids` | `str` | yes | Comma-separated Twitter Ads Account IDs |
| `resources` | `str` | no | Comma-separated resource list. Default `"campaigns,analytics"`. Available: `accounts`, `campaigns`, `line_items`, `promoted_tweets`, `tweets`, `media`, `analytics` |
| `start_date` | `str` | no | Analytics start date (YYYY-MM-DD). Defaults to 30 days ago |
| `end_date` | `str` | no | Analytics end date (YYYY-MM-DD). Defaults to today |
| `entity_type` | `str` | no | `CAMPAIGN` (default), `ACCOUNT`, `FUNDING_INSTRUMENT`, `LINE_ITEM`, `PROMOTED_TWEET` |
| `granularity` | `str` | no | `HOUR`, `DAY` (default), or `TOTAL` |
| `metric_groups` | `str` | no | Comma-separated metric groups: `ENGAGEMENT`, `BILLING`, `VIDEO`, `MOBILE_CONVERSION`, `WEB_CONVERSION`, `MEDIA`, `LIFE_TIME_VALUE_MOBILE_CONVERSION` |
| `placement` | `str` | no | `ALL_ON_TWITTER` (default) or `PUBLISHER_NETWORK` |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.

## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:

```yaml
type: dagster_component_templates.TwitterAdsIngestionComponent
attributes:
  asset_name: twitter_ads
  consumer_key: "${TWITTER_CONSUMER_KEY}"
  consumer_secret: "${TWITTER_CONSUMER_SECRET}"
  access_token: "${TWITTER_ACCESS_TOKEN}"
  access_token_secret: "${TWITTER_ACCESS_TOKEN_SECRET}"
  account_ids: "abc123"
  destination: bigquery            # or snowflake, postgres, filesystem, ...
  dataset_name: twitter_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__BIGQUERY__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.

**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**

## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.TwitterAdsIngestionComponent
attributes:
  asset_name: twitter_ads_data
  consumer_key: "${TWITTER_CONSUMER_KEY}"
  consumer_secret: "${TWITTER_CONSUMER_SECRET}"
  access_token: "${TWITTER_ACCESS_TOKEN}"
  access_token_secret: "${TWITTER_ACCESS_TOKEN_SECRET}"
  account_ids: "abc123,xyz789"
  resources: "campaigns,line_items,promoted_tweets,analytics"
  start_date: "2024-10-01"
  end_date: "2024-12-31"
  metric_groups: "ENGAGEMENT,BILLING,VIDEO"
  group_name: twitter_ads
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.

## Example — persist to BigQuery

```yaml
type: dagster_component_templates.TwitterAdsIngestionComponent
attributes:
  asset_name: twitter_ads
  consumer_key: "${TWITTER_CONSUMER_KEY}"
  consumer_secret: "${TWITTER_CONSUMER_SECRET}"
  access_token: "${TWITTER_ACCESS_TOKEN}"
  access_token_secret: "${TWITTER_ACCESS_TOKEN_SECRET}"
  account_ids: "abc123"
  resources: "campaigns,line_items,promoted_tweets,analytics"
  destination: bigquery
  dataset_name: twitter_raw
  persist_only: true
```

Set `DESTINATION__BIGQUERY__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.

## Notes

- **Incremental loading**: dlt tracks state across runs.
- **Schema evolution**: dlt accommodates new fields without manual migrations.
- **OAuth 1.0a**: Twitter Ads API requires OAuth 1.0a — all four credential fields are required.
- **API access**: Twitter Ads API access requires application approval — apply through the developer portal.
- **Data latency**: Twitter performance metrics typically lag a few hours.
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

- [Twitter Ads API](https://developer.twitter.com/en/docs/twitter-ads-api)
- [dlt REST API source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
