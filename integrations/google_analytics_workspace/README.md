# GoogleAnalyticsWorkspaceComponent

Auto-emit one Dagster asset per **GA4 property**. `StateBackedComponent` — discovery cached to disk. Materializing an asset runs a `runReport` against the GA Data API with the configured metrics/dimensions.

The workspace-shape peer of `google_analytics_ingestion`.

## Example

```yaml
type: dagster_community_components.GoogleAnalyticsWorkspaceComponent
attributes:
  credentials_json_env_var: GA_SERVICE_ACCOUNT_JSON
  property_selector:
    by_pattern: ["Marketing*", "Product*"]
  metrics: [activeUsers, sessions, eventCount, screenPageViews]
  dimensions: [date, country]
  date_range_days: 30
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Setup

1. In Google Cloud Console, create a service account with the **GA Data API** and **GA Admin API** enabled.
2. Grant the service-account email `viewer` access on every GA property you want to enumerate.
3. Download the JSON key and set the full blob as an env var: `export GA_SERVICE_ACCOUNT_JSON=$(cat sa.json)`.

## Metrics + dimensions

See [Google's GA4 API docs](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema) for the full list. Common:
- **Metrics**: `activeUsers`, `sessions`, `eventCount`, `screenPageViews`, `bounceRate`, `averageSessionDuration`, `newUsers`
- **Dimensions**: `date`, `country`, `city`, `deviceCategory`, `sessionSource`, `eventName`, `pagePath`

## Related

- `google_analytics_resource`
- `google_analytics_ingestion` — single-property counterpart
