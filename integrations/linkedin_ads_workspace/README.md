# LinkedInAdsWorkspaceComponent

Auto-emit **one Dagster asset per LinkedIn Ads campaign** across the ad accounts you specify. `StateBackedComponent` — campaign discovery is cached to disk, so `dg dev` startup is instant after the first refresh. Refresh with `dg utils refresh-defs-state`.

## What each asset produces

Each materialization runs an **`adAnalytics` finder query** against the LinkedIn Marketing API for one campaign and returns a **pandas DataFrame** keyed on `(campaign_id, <time bucket>)` with the configured `analytics_fields` (default: `impressions`, `clicks`, `costInUsd`, `conversions`, `videoViews`). Time bucket granularity is controlled by `time_granularity:` (`DAILY` / `MONTHLY` / `YEARLY` / `ALL`).

The workspace-shape peer of `linkedin_ads_ingestion` — that one is single-campaign with built-in dlt destinations (Snowflake / BigQuery / etc.). This component covers the auto-discovery pattern where every campaign becomes its own Dagster asset. To land the DataFrames in a warehouse, **chain a downstream sink component** (e.g. `dataframe_to_snowflake`, `dataframe_to_bigquery`, `dataframe_to_duckdb`) — one per output.

## Example

```yaml
type: dagster_community_components.LinkedInAdsWorkspaceComponent
attributes:
  access_token_env_var: LINKEDIN_ACCESS_TOKEN
  account_ids: ["1234567"]
  campaign_selector:
    by_pattern: ["Product*"]
  time_granularity: DAILY
  date_range_days: 30
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Auth

Requires an OAuth access token with the `r_ads_reporting` scope. LinkedIn's Marketing API tokens are short-lived — pair with `oauth_token_resource` for refresh-token rotation.

## Follow-up (roadmap)

This component is in the "Bucket B" reshape backlog — planned to eventually become `linkedin_ads_resource` + optional `_sync` component + optional `_sensor` component, matching the pattern already shipped for `notion` / `github` / `jira` / `pagerduty` / `stripe` / `airtable`. Blocked on LinkedIn's OAuth-only auth surface (harder to demo than username+password vendors). Track this via `blog/not-every-saas-is-a-workspace.md`.

## Related

- `linkedin_ads_resource` — thin API wrapper you can use directly from a sync component.
- `linkedin_ads_ingestion` — single-campaign counterpart with built-in dlt destinations (Snowflake / BigQuery / Postgres / …).
- `dataframe_to_snowflake` / `dataframe_to_bigquery` / `dataframe_to_duckdb` — downstream sinks to persist per-campaign DataFrames.
