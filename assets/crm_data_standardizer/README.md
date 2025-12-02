# CRM Data Standardizer Component

Standardize CRM data across platforms (HubSpot, Salesforce, Pipedrive) into a unified schema.

## Overview

Different CRM platforms organize customer data differently. This component normalizes contacts, companies, deals, and activities into a consistent format for unified analysis.

**Supported Platforms:**
- HubSpot
- Salesforce
- Pipedrive

**Supported Resource Types:**
- Contacts
- Companies
- Deals
- Activities

## Use Cases

- **Unified Customer View**: Combine CRM data from multiple platforms
- **Platform Migration**: Maintain consistent schema when switching CRMs
- **Sales Analytics**: Cross-platform sales funnel analysis
- **Data Warehouse**: Standardized CRM tables for BI tools
- **Customer 360**: Build complete customer profiles from multiple sources

## Input Requirements

Raw CRM data with platform-specific schemas:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `_resource_type` | string | ✓ | resource_type, object_type | Resource type (contacts, companies, deals) |
| `id` | string | ✓ | contact_id, company_id, deal_id | Record identifier |
| `email` | string | | contact_email, email_address | Contact email |
| `created_at` | datetime | | createdate, created_date | Record creation date |

**Compatible Upstream Components:**
- `hubspot_ingestion`
- `salesforce_ingestion`
- `pipedrive_ingestion`

## Output Schema

Standardized CRM data with unified fields:

| Column | Type | Description |
|--------|------|-------------|
| `resource_type` | string | contacts, companies, deals, activities |
| `platform` | string | Source platform (hubspot, salesforce, pipedrive) |
| `id` | string | Unique record identifier |
| `email` | string | Email address (contacts) |
| `name` | string | Name (contact/company/deal name) |
| `company` | string | Company name (for contacts) |
| `owner` | string | Account/deal owner |
| `status` | string | Status (active, inactive, open, closed) |
| `stage` | string | Deal stage (qualification, proposal, closed-won) |
| `value` | number | Deal value/revenue |
| `created_at` | datetime | Record creation timestamp |
| `updated_at` | datetime | Last update timestamp |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for standardized output asset
- **`platform`** (string): Source CRM platform
  - `hubspot`
  - `salesforce`
  - `pipedrive`
- **`resource_type`** (string): Type of CRM resource
  - `contacts`
  - `companies`
  - `deals`
  - `activities`

### Optional Parameters

- **`source_asset`** (string): Upstream asset with raw data (auto-set via lineage)
- **`record_id_field`** (string): Custom field for record ID (auto-detected)
- **`email_field`** (string): Custom field for email (auto-detected)
- **`name_field`** (string): Custom field for name (auto-detected)
- **`filter_status`** (string): Filter by status (comma-separated)
- **`filter_owner`** (string): Filter by owner (comma-separated)
- **`filter_date_from`** (string): Start date filter (YYYY-MM-DD)
- **`filter_date_to`** (string): End date filter (YYYY-MM-DD)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `crm`)
- **`include_sample_metadata`** (boolean): Include data preview (default: true)

## Example Configuration

### HubSpot Contacts Standardization

```yaml
type: dagster_component_templates.CRMDataStandardizerComponent
attributes:
  asset_name: standardized_crm_contacts
  platform: hubspot
  resource_type: contacts
  description: Standardized HubSpot contact data
  group_name: crm
```

### Salesforce Deals Standardization

```yaml
type: dagster_component_templates.CRMDataStandardizerComponent
attributes:
  asset_name: standardized_deals
  platform: salesforce
  resource_type: deals
  filter_status: "open"
  description: Open deals from Salesforce
```

### Multi-Platform Setup

```yaml
# hubspot_contacts.yaml
type: dagster_component_templates.CRMDataStandardizerComponent
attributes:
  asset_name: std_hubspot_contacts
  platform: hubspot
  resource_type: contacts

---
# salesforce_contacts.yaml
type: dagster_component_templates.CRMDataStandardizerComponent
attributes:
  asset_name: std_salesforce_contacts
  platform: salesforce
  resource_type: contacts

---
# Combine downstream
type: dagster_component_templates.DataFrameCombinerComponent
attributes:
  asset_name: all_contacts
  source_assets: ["std_hubspot_contacts", "std_salesforce_contacts"]
```

## Platform-Specific Mappings

### HubSpot Contacts
- `id` → `id`
- `properties.email` → `email`
- `properties.firstname` + `properties.lastname` → `name`
- `properties.company` → `company`
- `properties.hubspot_owner_id` → `owner`
- `properties.hs_lead_status` → `status`
- `properties.createdate` → `created_at`
- `properties.lastmodifieddate` → `updated_at`

### Salesforce Contacts
- `Id` → `id`
- `Email` → `email`
- `Name` → `name`
- `Account.Name` → `company`
- `OwnerId` → `owner`
- `Status` → `status`
- `CreatedDate` → `created_at`
- `LastModifiedDate` → `updated_at`

### Pipedrive Contacts
- `id` → `id`
- `primary_email` → `email`
- `name` → `name`
- `org_name` → `company`
- `owner_id` → `owner`
- `add_time` → `created_at`
- `update_time` → `updated_at`

## How It Works

1. **Resource Type Detection**: Identifies whether data is contacts, companies, deals, or activities
2. **Field Mapping**: Maps platform-specific fields to standardized schema
3. **Data Type Conversion**: Converts dates, numbers to consistent formats
4. **Status Normalization**: Standardizes status values across platforms
5. **Platform Tagging**: Adds source platform for tracking
6. **Filtering**: Applies optional filters for status, owner, date range

## Common Use Cases

### Customer 360 View

```
HubSpot Contacts → Standardizer →
Salesforce Contacts → Standardizer → Unified Contacts → Customer 360 Dashboard
Pipedrive Contacts → Standardizer →
```

### Sales Pipeline Analysis

```
Multi-Platform Deals → Standardizer → Deal Analysis → Pipeline Reports
```

### Revenue Attribution

```
Standardized Deals + Marketing Data → Attribution Model → Channel ROI
```

## Best Practices

### Data Quality
- Deduplicate contacts by email across platforms
- Validate email formats
- Handle null owners gracefully
- Standardize company names (casing, abbreviations)

### Owner Management
- Map owner IDs to email addresses
- Create owner lookup tables
- Handle reassignments and churned employees

### Status Values
- Document status mapping per platform
- Create standard lifecycle stages
- Handle custom status values

## Key Metrics

### Contact Metrics
- Total contacts by platform
- Active vs. inactive contacts
- Contact growth rate
- Contacts per owner

### Deal Metrics
- Pipeline value by stage
- Win rate by source
- Average deal size
- Sales velocity

### Activity Metrics
- Activities per contact
- Response times
- Engagement scores

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **Deduplication**: Consider post-processing to deduplicate across platforms
- **Relationships**: Maintains contact-to-company relationships when available
- **Custom Fields**: Can be extended to include platform-specific custom fields
- **Performance**: Handles millions of CRM records efficiently
- **Incremental**: Works well with incremental data loading
