# HubSpot Ingestion

Ingest HubSpot CRM and marketing data (contacts, companies, deals, tickets) using dlt (data load tool).

## Overview

The HubSpot Ingestion component extracts data from HubSpot's CRM using dlt and returns it as a pandas DataFrame for downstream transformation. Supports multiple resources including contacts, companies, deals, and tickets.

## Features

- **dlt Integration**: Uses dlt's HubSpot source for reliable data extraction
- **Multiple Resources**: Extract contacts, companies, deals, tickets, and more
- **Automatic Schema Detection**: dlt handles schema inference and evolution
- **DataFrame Output**: Returns data as pandas DataFrame for easy downstream processing
- **Sample Data Fallback**: Provides sample HubSpot data when API is unavailable

## Configuration

### Required Parameters

- `asset_name`: Name of the asset to create
- `api_key`: HubSpot Private App API key or access token (use environment variables recommended)

### Optional Parameters

- `resources`: List of HubSpot resources to extract (default: ["contacts", "companies", "deals"])
- `description`: Asset description
- `group_name`: Asset group for organization (default: "hubspot")
- `include_sample_metadata`: Include data preview in metadata (default: true)

## Usage

```yaml
type: dagster_component_templates.HubSpotIngestionComponent
attributes:
  asset_name: hubspot_crm_data
  api_key: "{{ env('HUBSPOT_API_KEY') }}"
  resources:
    - contacts
    - companies
    - deals
  description: "HubSpot CRM data for customer analytics"
```

## Available Resources

- `contacts`: Contact records with properties
- `companies`: Company records with properties
- `deals`: Deal pipeline and stage information
- `tickets`: Support ticket records
- `products`: Product catalog
- `quotes`: Sales quotes
- `hubspot_events_for_objects`: Event history

## Environment Variables

Set your HubSpot API key as an environment variable:

```bash
export HUBSPOT_API_KEY="your-api-key-here"
```

## How It Works

1. **DLT Pipeline Setup**: Creates a dlt pipeline with DuckDB destination
2. **Resource Selection**: Extracts specified HubSpot resources
3. **Data Extraction**: dlt handles API pagination, rate limiting, and retries
4. **DataFrame Conversion**: Converts extracted data to pandas DataFrame
5. **Metadata Output**: Provides row counts and preview

## Output Schema

The output DataFrame includes all fields from the selected HubSpot resources, typically:

**Contacts:**
- id, email, firstname, lastname
- createdate, lastmodifieddate
- Custom properties

**Companies:**
- id, name, domain, industry
- createdate, lastmodifieddate
- Custom properties

**Deals:**
- id, dealname, dealstage, amount
- closedate, createdate
- Custom properties

## Use Cases

- CRM data analytics
- Customer segmentation
- Sales pipeline analysis
- Marketing attribution
- Customer 360 views

## Dependencies

- dlt[hubspot]>=0.4.0
- pandas>=1.5.0
