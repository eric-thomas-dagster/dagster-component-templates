# Support Ticket Ingestion

Ingest support tickets from Zendesk and other support platforms using dlt.

## Overview

The Support Ticket Ingestion component extracts support ticket data from platforms like Zendesk using dlt. It reads credentials from environment variables and returns ticket data as a pandas DataFrame.

## Features

- **Platform Support**: Zendesk via dlt, with sample data fallback for other platforms
- **Environment Variables**: Reads credentials securely from environment
- **DataFrame Output**: Returns structured ticket data for downstream processing
- **Sample Data Fallback**: Provides realistic sample tickets when platform is unavailable
- **Automatic Schema**: dlt handles schema detection and field mapping

## Configuration

### Required Parameters

- `asset_name`: Name of the asset to create
- `platform`: Support platform to extract from (default: "zendesk")

### Optional Parameters

- `description`: Asset description
- `group_name`: Asset group for organization (default: "support")
- `include_sample_metadata`: Include data preview in metadata (default: true)

## Usage

```yaml
type: dagster_component_templates.SupportTicketIngestionComponent
attributes:
  asset_name: support_tickets
  platform: zendesk
  description: "Support tickets for customer service analysis"
```

## Supported Platforms

### Zendesk
Uses dlt's Zendesk source with full API support. Requires environment variables:
- `ZENDESK_SUBDOMAIN`: Your Zendesk subdomain (e.g., "mycompany")
- `ZENDESK_EMAIL`: Admin email address
- `ZENDESK_API_TOKEN`: API token for authentication

### Other Platforms
Falls back to sample data. To add support for additional platforms, extend the component with dlt sources.

## Environment Variables

For Zendesk:

```bash
export ZENDESK_SUBDOMAIN="mycompany"
export ZENDESK_EMAIL="admin@mycompany.com"
export ZENDESK_API_TOKEN="your-api-token"
```

## How It Works

1. **Platform Detection**: Checks which support platform to use
2. **DLT Pipeline**: Creates dlt pipeline for the platform
3. **Credential Loading**: Reads credentials from environment variables
4. **Data Extraction**: Extracts tickets using dlt
5. **DataFrame Conversion**: Returns structured DataFrame
6. **Fallback**: Provides sample data if credentials are missing

## Output Schema

Typical ticket data includes:

| Field | Description |
|-------|-------------|
| ticket_id | Unique ticket identifier |
| subject | Ticket subject/title |
| description | Full ticket description |
| status | Current status (open, pending, solved, closed) |
| priority | Priority level (low, normal, high, urgent) |
| created_at | Ticket creation timestamp |
| updated_at | Last update timestamp |
| requester_id | Customer/requester ID |
| assignee_id | Agent assigned to ticket |
| tags | Ticket tags/labels |

## Use Cases

- Support ticket analysis
- Agent performance metrics
- Response time tracking
- Customer satisfaction analysis
- Ticket volume forecasting
- SLA compliance monitoring

## Dependencies

- dlt[zendesk]>=0.4.0
- pandas>=1.5.0
