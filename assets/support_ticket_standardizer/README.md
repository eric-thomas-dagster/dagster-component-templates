# Support Ticket Standardizer Component

Standardize support ticket data across platforms (Zendesk, Freshdesk, Intercom) into a unified schema.

## Overview

Different support platforms organize ticket data differently. This component normalizes ticket data into a consistent format for unified reporting and analysis.

**Supported Platforms:**
- Zendesk
- Freshdesk
- Intercom

## Use Cases

- **Multi-Platform Support**: Analyze tickets from multiple support tools
- **Platform Migration**: Maintain historical data when switching platforms
- **Executive Dashboards**: Unified metrics across all support channels
- **SLA Tracking**: Consistent response/resolution time metrics
- **Customer Satisfaction**: Cross-platform CSAT analysis

## Input Requirements

Raw support ticket data with platform-specific schemas:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `id` | string | ✓ | ticket_id, ticketId | Ticket identifier |
| `status` | string | ✓ | ticket_status, state | Ticket status |
| `created_at` | datetime | ✓ | created, submitted_at | Ticket creation date |
| `subject` | string | | title, summary | Ticket subject (optional) |

**Compatible Upstream Components:**
- `zendesk_ingestion`
- `freshdesk_ingestion`

## Output Schema

Standardized support ticket data:

| Column | Type | Description |
|--------|------|-------------|
| `platform` | string | Source platform (zendesk, freshdesk, intercom) |
| `ticket_id` | string | Unique ticket identifier |
| `subject` | string | Ticket subject line |
| `status` | string | Standardized status (open, pending, solved, closed) |
| `priority` | string | Priority level (low, medium, high, urgent) |
| `type` | string | Ticket type (question, incident, problem, task) |
| `requester_email` | string | Customer email address |
| `assignee_email` | string | Support agent email |
| `created_at` | datetime | Ticket creation timestamp |
| `updated_at` | datetime | Last update timestamp |
| `resolved_at` | datetime | Resolution timestamp |
| `satisfaction_score` | string | CSAT rating (good, bad, offered, unoffered) |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for standardized output asset
- **`platform`** (string): Source platform
  - `zendesk`
  - `freshdesk`
  - `intercom`

### Optional Parameters

- **`source_asset`** (string): Upstream asset with raw data (auto-set via lineage)
- **`ticket_id_field`** (string): Custom field for ticket ID (auto-detected)
- **`status_field`** (string): Custom field for status (auto-detected)
- **`filter_status`** (string): Filter by status (comma-separated)
- **`filter_priority`** (string): Filter by priority (comma-separated)
- **`filter_date_from`** (string): Start date filter (YYYY-MM-DD)
- **`filter_date_to`** (string): End date filter (YYYY-MM-DD)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `support`)
- **`include_sample_metadata`** (boolean): Include data preview (default: true)

## Example Configuration

### Zendesk Tickets Standardization

```yaml
type: dagster_component_templates.SupportTicketStandardizerComponent
attributes:
  asset_name: standardized_support_tickets
  platform: zendesk
  description: Standardized support ticket data
  group_name: support
```

### With Filtering

```yaml
type: dagster_component_templates.SupportTicketStandardizerComponent
attributes:
  asset_name: active_tickets
  platform: freshdesk
  filter_status: "open,pending"
  filter_priority: "high,urgent"
  filter_date_from: "2024-01-01"
  description: Active high-priority tickets
```

## Platform-Specific Mappings

### Zendesk
- `id` → `ticket_id`
- `status` → `status` (new, open, pending, hold, solved, closed)
- `priority` → `priority` (low, normal, high, urgent)
- `subject` → `subject`
- `requester.email` → `requester_email`
- `assignee.email` → `assignee_email`
- `created_at` → `created_at`
- `updated_at` → `updated_at`
- `satisfaction_rating.score` → `satisfaction_score`

### Freshdesk
- `id` → `ticket_id`
- `status` → `status` (Open, Pending, Resolved, Closed)
- `priority` → `priority` (Low, Medium, High, Urgent)
- `subject` → `subject`
- `requester_id` → resolved to email
- `responder_id` → resolved to email

### Intercom
- `id` → `ticket_id`
- `state` → `status` (submitted, in_progress, resolved)
- `priority` → `priority` (low, high)
- `ticket_parts[0].body` → subject (first message)

## How It Works

1. **Field Detection**: Auto-identifies platform-specific field names
2. **Status Normalization**: Maps platform statuses to standard values (open, pending, solved, closed)
3. **Priority Normalization**: Standardizes priority levels
4. **Date Parsing**: Converts various timestamp formats to datetime
5. **Platform Tagging**: Adds source platform for tracking
6. **Filtering**: Applies optional filters for status, priority, dates

## Metrics You Can Calculate

### Response Time
```sql
AVG(first_response_at - created_at) AS avg_first_response_time
```

### Resolution Time
```sql
AVG(resolved_at - created_at) AS avg_resolution_time
```

### Ticket Volume
```sql
COUNT(*) GROUP BY DATE(created_at)
```

### Satisfaction Rate
```sql
COUNT(*) FILTER (satisfaction_score = 'good') / COUNT(*) AS satisfaction_rate
```

### Agent Performance
```sql
COUNT(*), AVG(resolved_at - created_at)
GROUP BY assignee_email
```

## Best Practices

### Data Quality
- Ensure ticket IDs are unique per platform
- Validate timestamp formats
- Handle null assignees (unassigned tickets)
- Clean email addresses for consistency

### Filtering
- Use status filtering to focus on active tickets
- Filter by priority for escalation tracking
- Date range filters for period-specific analysis

### Reporting
- Segment by platform for comparison
- Track SLA compliance by priority level
- Monitor satisfaction trends over time

## Common Use Cases

### Unified Support Dashboard
```
Zendesk → Standardizer →
Freshdesk → Standardizer → Unified Tickets → Metrics Dashboard
Intercom → Standardizer →
```

### SLA Monitoring
```
Standardized Tickets → Calculate Resolution Time → SLA Compliance Report
```

### Agent Performance
```
Standardized Tickets → Group by Assignee → Performance Metrics
```

### Customer Satisfaction
```
Standardized Tickets → Join with CSAT → Satisfaction Analysis
```

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **Status Mapping**: Component maps platform-specific statuses to standard values
- **Priority Levels**: Normalizes priority scales across platforms
- **Email Resolution**: Looks up email addresses from user IDs when needed
- **Time Zones**: Ensures all timestamps are in consistent timezone
- **Performance**: Handles millions of tickets efficiently
- **Custom Fields**: Can be extended to include platform-specific custom fields
