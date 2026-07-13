# FacebookAdsWorkspaceComponent

Auto-emit one Dagster asset per **Facebook Ads campaign** across specified ad accounts. `StateBackedComponent` — discovery cached to disk. Materializing an asset runs an insights query and emits a DataFrame with campaign performance metrics.

The workspace-shape peer of `facebook_ads_ingestion`.

## Example

```yaml
type: dagster_community_components.FacebookAdsWorkspaceComponent
attributes:
  access_token_env_var: FB_ACCESS_TOKEN
  account_ids: ["act_1234567890"]
  campaign_selector:
    by_pattern: ["Q4*", "*_Retention"]
  date_preset: last_30d
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Insights fields

Default: `campaign_name, impressions, clicks, spend, reach, ctr, cpc`. Add breakdowns like `actions`, `action_values`, `frequency`, `cost_per_action_type` for deeper analytics.

## Related

- `facebook_ads_resource`
- `facebook_ads_ingestion` — single-campaign counterpart
