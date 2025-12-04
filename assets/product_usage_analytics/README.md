# Product Usage Analytics Component

Analyze product feature usage to measure adoption, engagement, and identify power users for product-led growth strategies.

## Overview

The Product Usage Analytics Component helps you understand how users interact with your product by analyzing usage events and calculating engagement metrics. It identifies power users, tracks feature adoption, and calculates key product metrics like DAU/MAU ratio (stickiness).

## Features

- **User Engagement Metrics**: Total actions, days active, actions per day
- **Power User Identification**: Automatically identify highly engaged users
- **DAU/MAU Calculation**: Measure product stickiness
- **Feature Adoption Tracking**: Monitor which features are being used
- **Flexible Analysis Period**: Analyze any time window (7-365 days)
- **Custom Thresholds**: Configure what defines a "power user" for your product

## Use Cases

1. **Product-Led Growth**: Identify users ready for upgrade/expansion
2. **User Segmentation**: Group users by engagement level
3. **Feature Prioritization**: Understand which features drive engagement
4. **Churn Prevention**: Identify inactive users before they churn
5. **Onboarding Optimization**: Track new user activation and engagement
6. **Product Metrics Dashboard**: Calculate key product KPIs

## Prerequisites

**Python Packages**:
- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Configuration

### Basic Usage Analytics

Track overall product engagement:

```yaml
type: dagster_component_templates.ProductUsageAnalyticsComponent
attributes:
  asset_name: user_engagement_metrics

  # Input data
  event_data_asset: product_events

  # Analysis configuration
  analysis_period_days: 30

  # Power user identification
  identify_power_users: true
  power_user_threshold: 20  # 20 actions in period

  # Metrics to calculate
  calculate_dau_mau: true
  calculate_feature_adoption: true
```

### Feature Adoption Analysis

Track specific feature usage:

```yaml
type: dagster_component_templates.ProductUsageAnalyticsComponent
attributes:
  asset_name: feature_adoption_metrics

  event_data_asset: product_events

  # Define core features to track
  core_feature_events: "export_data,share_report,create_dashboard,invite_team_member"

  analysis_period_days: 30

  calculate_feature_adoption: true
```

### Power User Identification

Identify your most engaged users:

```yaml
type: dagster_component_templates.ProductUsageAnalyticsComponent
attributes:
  asset_name: power_users

  event_data_asset: product_events
  user_data_asset: user_profiles  # Optional: enrich with user data

  analysis_period_days: 30

  identify_power_users: true
  power_user_threshold: 50  # Higher threshold for "power user"
```

### Monthly Product Metrics

Calculate monthly product health metrics:

```yaml
type: dagster_component_templates.ProductUsageAnalyticsComponent
attributes:
  asset_name: monthly_product_metrics

  event_data_asset: product_events

  analysis_period_days: 30

  calculate_dau_mau: true
  calculate_feature_adoption: true
  identify_power_users: true
```

## Input Schema

The component expects event data with the following structure:

### Required Columns:
- `user_id` (or `customer_id`, `id`, `userid`): User identifier
- `timestamp` (or `event_timestamp`, `created_at`, `event_time`): Event timestamp

### Optional Columns:
- `event_name`: Name of the event/action (required for feature adoption tracking)
- `event_properties`: Additional event metadata

### Example Input:

```
| user_id | event_name        | timestamp           |
|---------|-------------------|---------------------|
| u_123   | page_view         | 2024-01-15 10:30:00 |
| u_123   | export_data       | 2024-01-15 10:35:00 |
| u_456   | create_dashboard  | 2024-01-15 11:00:00 |
| u_123   | share_report      | 2024-01-15 14:20:00 |
| u_789   | invite_user       | 2024-01-16 09:15:00 |
```

## Output Schema

The component generates a DataFrame with the following columns:

### Always Included:
- `user_id`: User identifier
- `total_actions`: Total number of actions in the analysis period
- `first_seen`: First event timestamp for this user
- `last_seen`: Most recent event timestamp
- `days_active`: Number of days between first and last event
- `actions_per_day`: Average actions per day

### Optional (based on configuration):
- `is_power_user`: Boolean flag (if identify_power_users=true)
- `dau_mau_ratio`: Daily/Monthly active user ratio (if calculate_dau_mau=true)
- `feature_adoption_*`: Adoption metrics for each feature (if calculate_feature_adoption=true)

### Example Output:

```
| user_id | total_actions | first_seen          | last_seen           | days_active | actions_per_day | is_power_user |
|---------|---------------|---------------------|---------------------|-------------|-----------------|---------------|
| u_123   | 45            | 2024-01-15 10:30:00 | 2024-01-30 18:45:00 | 16          | 2.8             | true          |
| u_456   | 12            | 2024-01-15 11:00:00 | 2024-01-25 14:30:00 | 11          | 1.1             | false         |
| u_789   | 8             | 2024-01-16 09:15:00 | 2024-01-20 10:00:00 | 5           | 1.6             | false         |
```

## Key Metrics Explained

### DAU/MAU Ratio (Stickiness)
- **Range**: 0.0 to 1.0
- **Interpretation**:
  - 0.2 (20%): Users active 6 days per month (good for monthly tools)
  - 0.5 (50%): Users active 15 days per month (very good)
  - 0.8 (80%): Users active 24 days per month (exceptional, like Slack/email)

### Power Users
- Users exceeding the `power_user_threshold` for actions
- Typically top 10-20% of your user base
- Focus on understanding what makes them engaged

### Actions Per Day
- Average activity level
- Helps segment users: high (>5), medium (2-5), low (<2)

### Days Active
- Retention indicator
- Users with high days_active but low actions_per_day may need onboarding help

## Advanced Patterns

### User Segmentation Pipeline

Combine with other components to create user segments:

```yaml
# Step 1: Calculate engagement metrics
- type: dagster_component_templates.ProductUsageAnalyticsComponent
  attributes:
    asset_name: engagement_metrics
    event_data_asset: product_events
    identify_power_users: true

# Step 2: Segment users (custom logic or clustering component)
# Step 3: Store segments for activation campaigns
```

### Feature Adoption Funnel

Track multi-step feature adoption:

```yaml
type: dagster_component_templates.ProductUsageAnalyticsComponent
attributes:
  asset_name: feature_funnel
  event_data_asset: product_events

  # Track onboarding funnel
  core_feature_events: "signup,profile_complete,first_action,invite_sent,premium_feature_used"

  calculate_feature_adoption: true
```

### Churn Risk Identification

Identify users becoming less engaged:

```yaml
type: dagster_component_templates.ProductUsageAnalyticsComponent
attributes:
  asset_name: churn_risk_users
  event_data_asset: product_events

  analysis_period_days: 7  # Recent activity only

  # Low threshold to catch declining users
  power_user_threshold: 5
  identify_power_users: true
```

## Integration Examples

### SaaS Product Analytics

```yaml
type: dagster_component_templates.ProductUsageAnalyticsComponent
attributes:
  asset_name: saas_product_metrics
  event_data_asset: application_events

  analysis_period_days: 30

  core_feature_events: "export,share,api_call,integration_setup"

  calculate_dau_mau: true
  calculate_feature_adoption: true
  identify_power_users: true
  power_user_threshold: 25
```

### Mobile App Analytics

```yaml
type: dagster_component_templates.ProductUsageAnalyticsComponent
attributes:
  asset_name: mobile_engagement
  event_data_asset: mobile_events

  analysis_period_days: 14  # Shorter for mobile

  calculate_dau_mau: true
  identify_power_users: true
  power_user_threshold: 10
```

### B2B Platform Usage

```yaml
type: dagster_component_templates.ProductUsageAnalyticsComponent
attributes:
  asset_name: platform_usage
  event_data_asset: platform_events
  user_data_asset: account_data

  analysis_period_days: 90  # Longer for B2B

  # B2B-relevant actions
  core_feature_events: "create_project,add_team_member,export_report,api_integration"

  calculate_feature_adoption: true
  identify_power_users: true
  power_user_threshold: 30
```

## Benchmarks by Product Type

### Consumer Apps (Social, Gaming)
- **Target DAU/MAU**: 0.3-0.5 (30-50%)
- **Power User Definition**: Daily active for 20+ days/month

### SaaS/B2B Tools
- **Target DAU/MAU**: 0.15-0.25 (15-25%)
- **Power User Definition**: 3-4 times per week

### Productivity Tools
- **Target DAU/MAU**: 0.5-0.8 (50-80%)
- **Power User Definition**: Multiple times per day

### E-commerce/Marketplace
- **Target DAU/MAU**: 0.1-0.2 (10-20%)
- **Power User Definition**: Weekly purchases

## Monitoring & Metadata

Metadata provided:

```python
{
  "analysis_period_days": 30,
  "total_users": 1500,
  "total_events": 45000,
  "average_actions_per_user": 30.0,
  "median_actions_per_user": 15.0,
  "power_user_count": 225,
  "power_user_percentage": 15.0,
  "power_user_threshold": 20,
  "average_days_active": 12.5,
  "dau_mau_ratio": 0.35
}
```

## Troubleshooting

### Low Engagement Numbers

**Problem**: Most users show very low engagement

**Solutions**:
1. Lower `power_user_threshold` to match your product's natural usage pattern
2. Check if events are being tracked correctly
3. Consider whether your analysis period matches your product's usage cycle
4. Segment by user cohort (new vs. established users)

### Missing Event Data

**Problem**: No events for some users

**Solutions**:
1. Verify event tracking implementation
2. Check for gaps in data pipeline
3. Ensure `event_data_asset` is correctly configured
4. Filter analysis to users with at least 1 event

### DAU/MAU Seems Off

**Problem**: DAU/MAU ratio doesn't match expectations

**Solutions**:
1. Ensure analysis_period_days is 30 for accurate MAU
2. Check for test/bot traffic in events
3. Verify timestamp parsing is correct
4. Consider your product's natural usage frequency

### Power User Definition Unclear

**Problem**: Unclear what threshold to use

**Solutions**:
1. Start with median user activity as baseline
2. Set threshold at 75th percentile of user activity
3. A/B test different thresholds for business outcomes
4. Align with product-specific engagement goals

## Best Practices

1. **Regular Monitoring**: Run daily/weekly to track trends over time
2. **Cohort Analysis**: Segment by signup date to track cohort retention
3. **Feature Flags**: Track adoption of new features separately
4. **User Feedback**: Combine with qualitative data from power users
5. **Alert on Changes**: Set up alerts for significant DAU/MAU drops
6. **Account-Level (B2B)**: For B2B, aggregate to account level
7. **Exclude Internal**: Filter out internal team/test users
8. **Time Zones**: Normalize timestamps to consistent timezone
9. **Mobile vs Desktop**: Segment by platform for better insights
10. **Compare Cohorts**: Compare new vs. established user engagement

## Product Metrics Glossary

- **DAU**: Daily Active Users (unique users per day)
- **MAU**: Monthly Active Users (unique users per 30 days)
- **Stickiness**: DAU/MAU ratio (how often users return)
- **L7/L28**: Users active 7 days / users active 28 days
- **Power User**: Top 10-20% most engaged users
- **Feature Adoption**: % of users who've used a feature
- **Activation Rate**: % of users reaching key product milestone

## Related Components

- **customer_segmentation**: Segment users based on engagement metrics
- **ltv_prediction**: Predict lifetime value using engagement patterns
- **churn_prediction**: Identify users at risk of churning
- **cohort_analysis**: Analyze retention by user cohort
- **funnel_analysis**: Track multi-step conversion funnels

## Support & Resources

- [Product Metrics That Matter](https://www.productplan.com/glossary/product-metrics/)
- [DAU/MAU Benchmarks by Industry](https://www.mixpanel.com/blog/dau-mau-benchmarks/)
- [Defining Power Users](https://www.reforge.com/blog/power-users)

## License

This component is part of the Dagster Components Templates library.
