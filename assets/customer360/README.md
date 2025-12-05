# Customer 360

Create unified customer profiles from multiple data sources, aggregating metrics and attributes into a single view.

## Overview

The Customer 360 component automatically unifies customer data from any upstream sources (CRM, support tickets, transactions, analytics, etc.) into comprehensive customer profiles. It intelligently detects customer identifiers, aggregates numeric metrics, and creates a unified view.

## Features

- **Automatic Data Integration**: Accepts any number of upstream data sources
- **Smart ID Detection**: Auto-detects customer identifiers (customer_id, user_id, email, etc.)
- **Metric Aggregation**: Sums numeric columns, counts records, tracks data sources
- **Derived Metrics**: Adds computed fields like activity status
- **Sample Data Fallback**: Returns sample profiles when no upstream data is available

## Configuration

### Required Parameters

- `asset_name`: Name of the unified customer profile asset

### Optional Parameters

- `description`: Asset description (default: "Unified customer 360 profiles")

## Usage

```yaml
type: dagster_component_templates.Customer360Component
attributes:
  asset_name: unified_customers
  description: "Complete customer profiles from all data sources"
```

## How It Works

1. **Data Collection**: Gathers DataFrames from all upstream assets
2. **ID Detection**: Scans for customer identifier columns (customer_id, user_id, email, id)
3. **Aggregation**: Groups data by customer ID and aggregates:
   - Sums all numeric columns
   - Counts total records per customer
   - Tracks number of data sources
4. **Profile Creation**: Generates unified customer profiles with:
   - customer_id
   - Aggregated metrics from all sources
   - data_sources count
   - is_active flag
5. **Metadata Output**: Provides preview and statistics

## Output Schema

The output includes:
- `customer_id`: Unified customer identifier
- Aggregated numeric columns from upstream sources
- `data_sources`: Number of upstream data sources
- `is_active`: Activity status flag
- Additional columns based on upstream data

## Example Output

| customer_id | total_spend | total_interactions | support_tickets | data_sources | is_active |
|-------------|-------------|-------------------|-----------------|--------------|-----------|
| cust_001    | 1250.00     | 45                | 2               | 3            | true      |
| cust_002    | 890.50      | 32                | 0               | 3            | true      |

## Use Cases

- Unified customer dashboards
- Customer segmentation analysis
- Churn prediction models
- Personalization engines
- Customer health scoring

## Dependencies

- pandas>=1.5.0
