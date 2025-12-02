# Customer 360 Component

Create unified customer profiles by combining data from multiple sources (CRM, payments, marketing, analytics) into a single comprehensive view.

## Overview

The Customer 360 component is the foundation of any Customer Data Platform (CDP). It unifies customer data from disparate sources into a single, comprehensive customer profile, providing a complete view of each customer's interactions, transactions, and behavior across all touchpoints.

## Key Features

- **Multi-Source Unification**: Combine data from Stripe, CRM, marketing platforms, and Google Analytics
- **Flexible Join Logic**: Configure primary and secondary join keys (email, user_id, phone, etc.)
- **Automatic Enrichment**: Calculate lifetime value, order metrics, engagement scores
- **Deduplication**: Intelligent merging of duplicate customer records
- **Activity Tracking**: Identify active vs. inactive customers
- **Attribution Fields**: First-touch source, medium, and campaign tracking

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| customer_id | string | Unified customer identifier |
| email | string | Primary email address |
| first_name | string | Customer first name |
| last_name | string | Customer last name |
| created_at | timestamp | First seen date |
| total_revenue | float | Lifetime revenue |
| total_orders | int | Number of transactions |
| avg_order_value | float | Average transaction size |
| total_sessions | int | Website sessions |
| total_page_views | int | Page views |
| acquisition_source | string | First touch source |
| acquisition_medium | string | First touch medium |
| acquisition_campaign | string | First touch campaign |
| last_interaction_date | timestamp | Most recent activity |
| customer_lifetime_days | int | Days since first interaction |
| is_active | bool | Active in last 30 days |

## Configuration

### Basic Configuration

```yaml
asset_name: customer_360
join_key: email
include_sample_data: true
```

### Input Sources (Connected via Visual Lineage)

- **Stripe Customers**: Payment and subscription data
- **Marketing Data**: Campaign interactions, conversions
- **GA4 Data**: Website behavior, sessions
- **CRM Data**: Contact information, interactions

Connect these sources by drawing edges in the Dagster Designer UI.

### Advanced Configuration

```yaml
asset_name: customer_360
join_key: email
secondary_join_keys: "user_id,phone"
deduplication_strategy: most_recent
active_customer_days: 30
include_sample_data: false
```

## Join Configuration

### Primary Join Key

The `join_key` field determines how customer records are matched across sources:
- `email` (recommended): Most common identifier
- `user_id`: If you have a unified ID system
- `customer_id`: For Stripe-centric CDPs
- `phone`: For SMS/phone-based systems

### Secondary Join Keys

Use `secondary_join_keys` for fuzzy matching when primary key is missing:
```yaml
join_key: email
secondary_join_keys: "user_id,phone"
```

This will first try to join on email, then fall back to user_id, then phone.

## Deduplication Strategies

When multiple records match the same customer:

- **most_recent** (default): Keep the most recently updated record
- **merge**: Combine all non-null fields from matching records
- **highest_value**: Keep the record with highest total_revenue

```yaml
deduplication_strategy: merge
```

## Use Cases

### 1. Customer Segmentation

Use unified profiles to segment customers by behavior, value, or engagement:

```python
# High-value active customers
df[
    (df['total_revenue'] > 1000) &
    (df['is_active'] == True)
]

# At-risk churned customers
df[
    (df['total_orders'] > 5) &
    (df['last_interaction_date'] < '2024-01-01')
]
```

### 2. Personalization

Feed customer profiles into marketing automation:
- Email campaigns based on purchase history
- Product recommendations using behavior data
- Retargeting campaigns for inactive users

### 3. Analytics & Reporting

Calculate business metrics:
- Customer Lifetime Value (CLV) distribution
- Acquisition channel effectiveness
- Customer engagement trends over time

### 4. Data Science & ML

Use as feature source for:
- Churn prediction models
- Next best action recommendations
- Customer scoring algorithms

## Input Requirements

### Stripe Data

Expected fields:
- `customer_id` or `id`: Customer identifier
- `email`: Email address
- `name` or `first_name`/`last_name`: Customer name
- `created`: Account creation date
- Revenue fields from charges/subscriptions

### Marketing Data

Expected fields (from standardized marketing components):
- `user_id` or `email`: User identifier
- `source`: Traffic source (google, facebook, etc.)
- `medium`: Traffic medium (cpc, organic, email)
- `campaign`: Campaign name
- `timestamp`: Event timestamp

### GA4 Data

Expected fields:
- `user_id` or `email`: User identifier
- `session_id`: Session identifier
- `page_views`: Number of page views
- `event_timestamp`: Event time

### CRM Data

Expected fields:
- `contact_id` or `email`: Contact identifier
- `first_name`, `last_name`: Contact name
- `phone`: Phone number (optional)
- `company`: Company name (optional)

## Output Formats

### DataFrame (Default)

Returns pandas DataFrame for downstream transformations:
```python
df = context.load_asset_value("customer_360")
print(df.head())
```

### Database Persistence

Connect to `dlt_dataframe_writer` to persist to any database:
```
customer_360 → dlt_dataframe_writer (Snowflake)
```

## Example Pipeline

```
┌─────────────┐
│   Stripe    │
│   Orders    │
└──────┬──────┘
       │
       ├────┐
       │    │     ┌─────────────┐
       │    └────▶│  Customer   │
       │          │     360     │────▶ Segmentation
┌──────▼──────┐   └─────────────┘
│  Marketing  │          │
│    Data     │──────────┘
└─────────────┘          │
                         │
┌─────────────┐          │
│     GA4     │──────────┘
│   Events    │
└─────────────┘
```

## Performance Considerations

- **Data Volume**: Component handles millions of customer records efficiently
- **Join Performance**: Email joins are fast; phone/user_id may be slower
- **Memory Usage**: Loads all sources into memory; consider filtering large datasets first
- **Incremental Updates**: Run daily/weekly to keep profiles fresh

## Best Practices

1. **Clean Input Data**: Normalize emails (lowercase), validate formats
2. **Consistent Identifiers**: Ensure same ID scheme across sources
3. **Regular Updates**: Schedule daily/weekly runs to keep profiles current
4. **Monitor Quality**: Check for NULL join keys, duplicate records
5. **Enrich Over Time**: Start simple, add sources as needed

## Troubleshooting

### No Matches Found

**Problem**: Customer 360 output is empty or has very few records

**Solutions**:
- Verify join_key exists in all input sources
- Check for email format mismatches (uppercase vs lowercase)
- Try secondary_join_keys for fallback matching
- Enable sample data to test with known records

### Duplicate Customers

**Problem**: Same customer appears multiple times

**Solutions**:
- Set deduplication_strategy to "merge" or "most_recent"
- Check for multiple emails per customer
- Investigate NULL values in join key

### Missing Fields

**Problem**: Expected fields are NULL in output

**Solutions**:
- Verify input sources have required fields
- Check field name mappings
- Some sources may not have all data (expected)

## Related Components

- **CRM Standardizer**: Normalize CRM data before feeding into Customer 360
- **Event Standardizer**: Standardize analytics events for GA4 input
- **Product Standardizer**: Add product data to customer purchases
- **Revenue Attribution**: Calculate ROI using Customer 360 profiles
- **Cohort Analysis**: Analyze customer cohorts from unified profiles
- **Churn Prediction**: Predict churn risk using profile data

## Learn More

- [CDP Architecture Best Practices](https://www.datacouncil.ai/blog/cdp-architecture)
- [Customer 360 Implementation Guide](https://segment.com/blog/customer-360/)
- [Data Quality for Customer Data](https://www.talend.com/resources/customer-data-quality/)
