# Customer Journey Mapping Component

Map and analyze customer journeys from first touch to conversion. Identify common paths, drop-off points, and optimize conversion funnels.

## Overview

Customer journey mapping traces the path customers take from awareness to conversion, revealing:
- Most common journey paths
- Average journey length and duration
- Conversion rates by journey stage
- Drop-off points and bottlenecks
- Journey patterns by customer segment

## Use Cases

- **Funnel Optimization**: Identify and fix conversion bottlenecks
- **Path Analysis**: Understand most effective routes to conversion
- **Drop-off Analysis**: Find where customers abandon
- **Journey Personalization**: Tailor experiences based on common paths
- **Content Strategy**: Optimize content for each journey stage
- **Marketing Attribution**: See full customer journey context

## Input Requirements

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `customer_id` | string | ✓ | user_id, customerId, userId | Customer identifier |
| `event` | string | ✓ | event_name, event_type, action | Event/interaction type |
| `timestamp` | datetime | ✓ | date, created_at, event_time | Event timestamp |

**Compatible Upstream Components:**
- `event_data_standardizer`
- `google_analytics_ingestion`
- Any component outputting customer event streams

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | string | Customer identifier |
| `journey_path` | string | Event sequence (e.g., "view → add_cart → purchase") |
| `journey_length` | number | Number of events in journey |
| `journey_duration_hours` | number | Time from first to last event (hours) |
| `first_event` | string | Starting event |
| `last_event` | string | Ending event |
| `unique_events` | number | Count of distinct event types |
| `converted` | boolean | Whether journey resulted in conversion |
| `first_timestamp` | datetime | Journey start time |
| `last_timestamp` | datetime | Journey end time |

## Configuration

```yaml
type: dagster_component_templates.CustomerJourneyMappingComponent
attributes:
  asset_name: customer_journeys
  conversion_event: "purchase"
  max_journey_length: 20
  time_window_hours: 720  # 30 days
  include_path_analysis: true
  description: Customer journey analysis
```

## Key Insights

### Top Journey Paths
Reveals most common sequences:
```
1. (450 customers, 15.2%): homepage_view → product_view → purchase
2. (320 customers, 10.8%): search → product_view → add_cart → purchase
3. (280 customers, 9.4%): email_click → product_view → purchase
```

### Conversion Rate by Journey Length
```
1 events: 5.2% (single-step conversions)
2 events: 18.4% (direct path)
3 events: 32.1% (optimal length)
4-5 events: 28.5%
6+ events: 15.8% (too complex?)
```

## Best Practices

- **Time Window**: 30 days for e-commerce, 90-180 days for B2B
- **Max Journey Length**: 15-20 events (prevents noise)
- **Conversion Event**: Define clear conversion criteria
- **Regular Updates**: Refresh weekly to track changes

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`
