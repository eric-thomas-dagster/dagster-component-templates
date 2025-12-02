# Product Analytics Standardizer Component

Standardize product analytics data across platforms (GA4, Matomo, Mixpanel, Amplitude) into a unified schema.

## Overview

Different product analytics platforms track and report engagement metrics differently. This component normalizes web and mobile analytics data into a consistent format for cross-platform analysis.

**Supported Platforms:**
- Google Analytics 4 (GA4)
- Matomo
- Mixpanel
- Amplitude

## Use Cases

- **Multi-Platform Analytics**: Combine data from web (GA4) and mobile (Amplitude) analytics
- **Platform Migration**: Maintain historical metrics when switching analytics tools
- **Unified Dashboards**: Single source of truth for product metrics
- **A/B Testing**: Cross-platform experiment analysis
- **User Behavior**: Consistent user journey tracking across platforms

## Input Requirements

Raw product analytics data with platform-specific schemas:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `date` | datetime | ✓ | event_date, timestamp, day | Event date |
| `user_id` | string | | visitor_id, client_id, userId | User identifier |
| `sessions` | number | | session_count, visits | Number of sessions |
| `page_views` | number | | screenviews, pageviews, screen_page_views | Page/screen views |

**Compatible Upstream Components:**
- `google_analytics_4_ingestion`
- `matomo_ingestion`

## Output Schema

Standardized product analytics metrics:

| Column | Type | Description |
|--------|------|-------------|
| `date` | datetime | Event/report date |
| `platform` | string | Source platform (google_analytics_4, matomo, etc.) |
| `user_id` | string | User identifier (if available) |
| `sessions` | number | Total sessions |
| `users` | number | Total users |
| `new_users` | number | New users |
| `page_views` | number | Total page/screen views |
| `events` | number | Total events tracked |
| `bounce_rate` | number | Bounce rate percentage |
| `avg_session_duration` | number | Average session duration (seconds) |
| `engagement_rate` | number | Engagement rate percentage |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for standardized output asset
- **`platform`** (string): Source analytics platform
  - `google_analytics_4`
  - `matomo`
  - `mixpanel`
  - `amplitude`

### Optional Parameters

- **`source_asset`** (string): Upstream asset with raw data (auto-set via lineage)
- **`date_field`** (string): Custom field for date (auto-detected)
- **`event_name_field`** (string): Custom field for event name (auto-detected)
- **`user_id_field`** (string): Custom field for user ID (auto-detected)
- **`filter_date_from`** (string): Start date filter (YYYY-MM-DD)
- **`filter_date_to`** (string): End date filter (YYYY-MM-DD)
- **`filter_event_name`** (string): Filter by event names (comma-separated)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `product_analytics`)
- **`include_sample_metadata`** (boolean): Include data preview (default: true)

## Example Configuration

### GA4 Standardization

```yaml
type: dagster_component_templates.ProductAnalyticsStandardizerComponent
attributes:
  asset_name: standardized_ga4_metrics
  platform: google_analytics_4
  description: Standardized Google Analytics 4 metrics
  group_name: product_analytics
```

### Matomo Standardization

```yaml
type: dagster_component_templates.ProductAnalyticsStandardizerComponent
attributes:
  asset_name: standardized_matomo_metrics
  platform: matomo
  filter_date_from: "2024-01-01"
  description: Standardized Matomo analytics data
```

### Multi-Platform Setup

```yaml
# ga4.yaml
type: dagster_component_templates.ProductAnalyticsStandardizerComponent
attributes:
  asset_name: std_ga4_metrics
  platform: google_analytics_4

---
# matomo.yaml
type: dagster_component_templates.ProductAnalyticsStandardizerComponent
attributes:
  asset_name: std_matomo_metrics
  platform: matomo

---
# Combine downstream
type: dagster_component_templates.DataFrameCombinerComponent
attributes:
  asset_name: all_analytics_metrics
  source_assets: ["std_ga4_metrics", "std_matomo_metrics"]
```

## Platform-Specific Mappings

### Google Analytics 4
- `event_date` → `date`
- `user_pseudo_id` → `user_id`
- `sessions` → `sessions`
- `total_users` → `users`
- `new_users` → `new_users`
- `screen_page_views` → `page_views`
- `event_count` → `events`
- `bounce_rate` → `bounce_rate`
- `average_session_duration` → `avg_session_duration`
- `engagement_rate` → `engagement_rate`

### Matomo
- `date` → `date`
- `visitor_id` → `user_id`
- `visits` → `sessions`
- `visitors` → `users`
- `pageviews` → `page_views`
- `bounce_rate` → `bounce_rate`
- `avg_time_on_site` → `avg_session_duration`

### Mixpanel
- `date` → `date`
- `distinct_id` → `user_id`
- `sessions` → `sessions`
- `unique_users` → `users`
- `events` → `events`

### Amplitude
- `event_date` → `date`
- `user_id` → `user_id`
- `sessions` → `sessions`
- `active_users` → `users`
- `events` → `events`

## How It Works

1. **Field Detection**: Auto-identifies platform-specific field names
2. **Metric Calculation**: Calculates derived metrics (bounce rate, engagement rate)
3. **Time Normalization**: Converts session durations to consistent units (seconds)
4. **Percentage Standardization**: Ensures rates are in 0-100 format
5. **Platform Tagging**: Adds source platform for tracking
6. **Filtering**: Applies optional date and event filters

## Key Metrics Explained

### Bounce Rate
Percentage of single-page sessions (user left without interaction):
```
Bounce Rate = (Single-page sessions / Total sessions) × 100
```

### Engagement Rate
Percentage of engaged sessions (lasted >10s, had conversion, or 2+ page views):
```
Engagement Rate = (Engaged sessions / Total sessions) × 100
```

### Pages per Session
Average pages viewed per session:
```
Pages per Session = Total page views / Total sessions
```

### Average Session Duration
Average time users spend per session (in seconds):
```
Avg Duration = Total session duration / Total sessions
```

## Common Use Cases

### Unified Product Dashboard
```
GA4 (Web) → Standardizer →
Amplitude (Mobile) → Standardizer → Unified Metrics → Product Dashboard
Matomo (EU) → Standardizer →
```

### User Retention Analysis
```
Standardized Analytics → Cohort Analysis → Retention Dashboard
```

### Feature Adoption
```
Standardized Events → Filter by Feature → Adoption Metrics
```

## Best Practices

### Data Granularity
- **Daily aggregates**: Best for most dashboards
- **Hourly data**: For real-time monitoring
- **User-level**: For detailed behavior analysis

### Session Definition
- Standardize session timeout (typically 30 minutes)
- Document cross-platform session boundaries
- Handle multi-device sessions consistently

### User Identity
- Use consistent user IDs across platforms
- Handle anonymous vs. authenticated users
- Document identity resolution logic

## Platform Comparison Metrics

### Typical Bounce Rates by Industry
- E-commerce: 20-45%
- Content/Media: 40-60%
- SaaS: 10-30%
- Lead Gen: 30-50%

### Typical Engagement Rates
- High Engagement: >60%
- Medium Engagement: 40-60%
- Low Engagement: <40%

### Session Duration Benchmarks
- Quick Actions: 1-2 minutes
- Content Consumption: 3-5 minutes
- Complex Tasks: 5-10+ minutes

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **Sampling**: GA4 may sample data for large datasets; ensure consistent sampling settings
- **Time Zones**: Normalize all dates to consistent timezone (usually UTC or business timezone)
- **Event Limits**: Different platforms have different event volume limits
- **Custom Events**: Map platform-specific custom events to standard taxonomy
- **Performance**: Handles millions of events per day efficiently
- **Real-time**: Most platforms have 24-48 hour data delay; consider this in reporting
- **Privacy**: Ensure compliance with GDPR/CCPA when combining cross-platform user data
