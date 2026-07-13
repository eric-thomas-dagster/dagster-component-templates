# LinkedInAdsWorkspaceComponent

Auto-emit one Dagster asset per **LinkedIn Ads campaign** across specified ad accounts. `StateBackedComponent` — discovery cached to disk. Materializing an asset runs an `adAnalytics` finder query for the campaign.

The workspace-shape peer of `linkedin_ads_ingestion`.

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

## Related

- `linkedin_ads_resource`
- `linkedin_ads_ingestion` — single-campaign counterpart
