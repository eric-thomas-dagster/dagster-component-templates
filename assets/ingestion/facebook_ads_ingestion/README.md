# Facebook Ads Ingestion Component


Ingest [Facebook Ads](https://www.facebook.com/business/ads) campaign data and insights using [dlt](https://dlthub.com)'s verified `facebook_ads` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `account_id` | `str` | yes | Ad account ID (format `act_123456789`) |
| `access_token` | `str` | yes | Facebook access token with `ads_read` permissions |
| `app_id` | `Optional[str]` | no | Facebook App ID (optional, for long-lived tokens) |
| `app_secret` | `Optional[str]` | no | Facebook App Secret (optional, for long-lived tokens) |
| `resources` | `str` | no | Comma-separated resources — `campaigns,ad_sets,ads,creatives,ad_leads,insights` (default: `"insights"`) |
| `initial_load_past_days` | `int` | no | Days of historical data on first load (default 30) |
| `ad_states` | `str` | no | Comma-separated ad states to extract (default `"ACTIVE,PAUSED"`) |
| `insights_fields` | `Optional[str]` | no | Comma-separated insights fields |
| `insights_breakdown` | `Optional[str]` | no | Insights breakdown dimension (`age`, `gender`, `country`, `region`, `platform`, `device_platform`) |
| `time_increment_days` | `int` | no | Insights time increment in days (default 1 = daily) |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.FacebookAdsIngestionComponent
attributes:
  asset_name: facebook_ads_data
  # ... source config ...
  destination: bigquery          # or bigquery, postgres, filesystem, ...
  dataset_name: facebook_ads_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.FacebookAdsIngestionComponent
attributes:
  asset_name: facebook_ads_data
  account_id: act_123456789
  access_token: "{{ env('FACEBOOK_ACCESS_TOKEN') }}"
  resources: "campaigns,ads,insights"
  initial_load_past_days: 90
  group_name: facebook_ads
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Bigquery

```yaml
type: dagster_component_templates.FacebookAdsIngestionComponent
attributes:
  asset_name: facebook_ads_data
  account_id: act_123456789
  access_token: "{{ env('FACEBOOK_ACCESS_TOKEN') }}"
  resources: "campaigns,ads,insights"
  destination: bigquery
  dataset_name: facebook_ads_raw
  persist_only: true
```

Set `DESTINATION__BIGQUERY__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Insights vs entities**: insights data goes through a separate `facebook_insights_source` and is loaded after the entity resources.
- **Long-lived tokens**: provide `app_id` and `app_secret` to enable token refresh.
- **Rate limits**: Facebook applies aggressive rate limits — use `initial_load_past_days` to bound first-load size.
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

- [dlt Facebook Ads source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/facebook_ads)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
