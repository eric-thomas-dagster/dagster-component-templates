# Event Data Standardizer Component

Standardize event tracking data across platforms (Segment, Rudderstack, Snowplow, Custom) into a unified schema.

## Overview

Different event tracking platforms use different schemas and field names. This component normalizes event data from various sources into a consistent format, making cross-platform analysis possible.

**Supported Platforms:**
- Segment
- Rudderstack
- Snowplow
- Custom implementations

## Use Cases

- **Unified Analytics**: Analyze events from multiple tracking platforms together
- **Platform Migration**: Maintain consistent schema when switching platforms
- **Data Warehouse**: Create standardized event tables for BI tools
- **Customer Journey**: Track user paths across different event sources
- **Attribution**: Connect events from various platforms for multi-touch attribution

## Input Requirements

Raw event data with platform-specific schemas:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `event` | string | ✓ | event_name, event_type, type | Event name or type |
| `timestamp` | datetime | ✓ | sent_at, received_at, event_time | Event timestamp |
| `user_id` | string | | userId, customer_id, anonymous_id | User identifier (optional but recommended) |

**Compatible Upstream Components:**
- `google_analytics_4_ingestion`
- `matomo_ingestion`

## Output Schema

Standardized event data with unified schema:

| Column | Type | Description |
|--------|------|-------------|
| `platform` | string | Source platform (segment, rudderstack, etc.) |
| `event_id` | string | Unique event identifier |
| `event_name` | string | Standardized event name |
| `event_type` | string | Event type (track, page, identify, etc.) |
| `user_id` | string | User identifier |
| `anonymous_id` | string | Anonymous session identifier |
| `session_id` | string | Session identifier |
| `timestamp` | datetime | Event timestamp |
| `page_url` | string | Page URL where event occurred |
| `referrer` | string | Referring URL |
| `device_type` | string | Device type (desktop, mobile, tablet) |
| `browser` | string | Browser name |
| `os` | string | Operating system |
| `country` | string | User country |
| `city` | string | User city |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for standardized output asset
- **`platform`** (string): Source platform to standardize
  - `segment`
  - `rudderstack`
  - `snowplow`
  - `custom`

### Optional Parameters

- **`source_asset`** (string): Upstream asset with raw data (auto-set via lineage)
- **`event_id_field`** (string): Custom field for event ID (auto-detected)
- **`event_name_field`** (string): Custom field for event name (auto-detected)
- **`user_id_field`** (string): Custom field for user ID (auto-detected)
- **`timestamp_field`** (string): Custom field for timestamp (auto-detected)
- **`filter_event_name`** (string): Filter by event names (comma-separated)
- **`filter_event_type`** (string): Filter by event types (comma-separated)
- **`filter_date_from`** (string): Start date filter (YYYY-MM-DD)
- **`filter_date_to`** (string): End date filter (YYYY-MM-DD)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `events`)
- **`include_sample_metadata`** (boolean): Include data preview (default: true)

## Example Configuration

### Segment Events Standardization

```yaml
type: dagster_component_templates.EventDataStandardizerComponent
attributes:
  asset_name: standardized_events
  platform: segment
  description: Standardized event tracking data from Segment
  group_name: events
```

### With Filtering

```yaml
type: dagster_component_templates.EventDataStandardizerComponent
attributes:
  asset_name: filtered_events
  platform: rudderstack
  filter_event_name: "page_view,purchase,signup"
  filter_event_type: "track,page"
  filter_date_from: "2024-01-01"
  description: Filtered and standardized events
```

### Custom Platform

```yaml
type: dagster_component_templates.EventDataStandardizerComponent
attributes:
  asset_name: custom_events
  platform: custom
  event_id_field: "custom_event_id"
  event_name_field: "action"
  user_id_field: "uid"
  timestamp_field: "created_at"
  description: Standardized custom event data
```

## Platform-Specific Mappings

### Segment
- `event` → `event_name`
- `messageId` → `event_id`
- `userId` → `user_id`
- `anonymousId` → `anonymous_id`
- `context.page.url` → `page_url`

### Rudderstack
- `event` → `event_name`
- `messageId` → `event_id`
- `userId` → `user_id`
- `anonymousId` → `anonymous_id`
- Similar structure to Segment

### Snowplow
- `event_name` → `event_name`
- `event_id` → `event_id`
- `user_id` → `user_id`
- `domain_sessionid` → `session_id`

### Custom
- Uses field mappings you specify
- Auto-detection for common patterns

## How It Works

1. **Field Detection**: Automatically identifies platform-specific field names using alternatives
2. **Schema Mapping**: Maps source fields to standardized output schema
3. **Type Conversion**: Ensures consistent data types (datetime, string, etc.)
4. **Enrichment**: Adds platform identifier to track source
5. **Filtering**: Optionally filters by event name, type, or date range

## Best Practices

### Data Quality
- Ensure raw events have consistent event_name values
- Validate timestamp formats are parseable
- Handle missing user_id gracefully

### Filtering
- Use event name filtering to reduce data volume
- Filter date ranges for incremental processing
- Keep commonly-used events in standardized form

### Naming Conventions
- Use consistent event names across platforms (if possible)
- Document custom field mappings
- Maintain event taxonomy documentation

## Common Use Cases

### Multi-Platform Analytics
```
Segment Events → Standardizer → Unified Events Table → BI Tool
Snowplow Events → Standardizer → ↗
```

### Platform Migration
```
Old Platform → Standardizer → Combined Historical Data
New Platform → Standardizer → ↗
```

### Event Warehouse
```
Raw Events → Standardizer → Standardized Events → Funnel Analysis
                                                 → RFM Segmentation
                                                 → Attribution Modeling
```

## Dependencies

- `pandas>=1.5.0`

## Notes

- **Column Auto-Detection**: Component tries common field name variations automatically
- **Missing Fields**: Optional fields are filled with null if not present
- **Performance**: Handles millions of events efficiently
- **Incremental**: Works well with partitioned/incremental data loading
- **Platform Support**: Easy to extend for new platforms by adding field mappings
