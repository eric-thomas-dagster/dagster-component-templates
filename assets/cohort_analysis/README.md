# Cohort Analysis Component

Track customer retention by acquisition cohort over time. Understand how well you're retaining customers and identify trends in customer lifecycle behavior.

## Overview

Cohort analysis groups customers by their acquisition date and tracks their behavior over time. This helps answer questions like:

- What percentage of customers from January are still active in June?
- Are newer cohorts retaining better than older ones?
- How does retention change over the customer lifecycle?
- What's the typical customer lifetime?

## Use Cases

- **Retention Measurement**: Track how well you're keeping customers over time
- **Product Improvements**: See if product changes improve retention
- **Marketing Effectiveness**: Compare retention across acquisition channels
- **Revenue Forecasting**: Project future revenue based on cohort behavior
- **Churn Analysis**: Identify when customers are most likely to churn
- **Subscription Metrics**: Essential for SaaS and subscription businesses

## Input Requirements

The component expects customer activity data with:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `customer_id` | string | ✓ | user_id, customerId, userId, id | Unique customer identifier |
| `first_order_date` | datetime | ✓ | first_activity_date, signup_date, created_at | Customer acquisition date |
| `activity_date` | datetime | ✓ | order_date, date, transaction_date | Date of activity/engagement |
| `revenue` | number | | amount, total, price, value | Transaction amount (optional) |

**Compatible Upstream Components:**
- `customer_360`
- `shopify_ingestion`
- `stripe_ingestion`
- `ecommerce_standardizer`

## Output Schema

Returns a cohort retention matrix with one row per cohort:

| Column | Type | Description |
|--------|------|-------------|
| `cohort_period` | string | Cohort identifier (e.g., "2024-01") |
| `cohort_size` | number | Number of customers in cohort |
| `period_0` | number | Retention % in period 0 (100%) |
| `period_1` | number | Retention % in period 1 |
| `period_2` | number | Retention % in period 2 |
| `period_N` | number | Additional periods as configured |

**Example Output:**

```
cohort_period  cohort_size  period_0  period_1  period_2  period_3
2024-01        500          100.0     68.2      54.6      48.4
2024-02        620          100.0     72.1      58.3      52.0
2024-03        580          100.0     74.5      61.2      -
```

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for the output asset (e.g., `customer_cohort_retention`)

### Optional Parameters

- **`source_asset`** (string): Upstream asset name (auto-set via lineage)
- **`cohort_period`** (string): Cohort grouping period
  - `monthly` (default): Group by month
  - `weekly`: Group by week
  - `daily`: Group by day
- **`retention_periods`** (number): Number of periods to track (default: 12, max: 36)
- **`include_revenue`** (boolean): Include revenue metrics (default: false)
- **`customer_id_field`** (string): Custom column name (auto-detected)
- **`first_date_field`** (string): Custom column name (auto-detected)
- **`activity_date_field`** (string): Custom column name (auto-detected)
- **`revenue_field`** (string): Custom column name (required if include_revenue=true)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `customer_analytics`)
- **`include_sample_metadata`** (boolean): Include data preview (default: true)

## Example Configuration

### Basic Retention Analysis

```yaml
type: dagster_component_templates.CohortAnalysisComponent
attributes:
  asset_name: customer_cohort_retention
  cohort_period: monthly
  retention_periods: 12
  description: Monthly customer retention cohorts
  group_name: customer_analytics
```

### With Revenue Tracking

```yaml
type: dagster_component_templates.CohortAnalysisComponent
attributes:
  asset_name: cohort_revenue_retention
  cohort_period: monthly
  retention_periods: 12
  include_revenue: true
  revenue_field: order_amount
  description: Cohort analysis with revenue metrics
```

## How It Works

1. **Cohort Assignment**: Customers are grouped by their first activity date into cohorts (e.g., "January 2024 cohort")

2. **Period Calculation**: For each customer activity, calculate which period it falls into relative to their cohort
   - Period 0 = acquisition period
   - Period 1 = next period after acquisition
   - Period N = N periods after acquisition

3. **Retention Calculation**: For each cohort and period:
   - Count unique active customers
   - Calculate: `(active_customers / cohort_size) × 100`

4. **Matrix Generation**: Create a retention matrix showing retention % for each cohort over time

## Reading the Results

### Interpreting the Matrix

- **Diagonal Pattern**: Retention naturally decreases over time
- **Horizontal Comparison**: Compare cohorts at the same lifecycle stage
- **Vertical Comparison**: See how one cohort evolves over time

### Key Metrics to Watch

- **Period 1 Retention**: First retention checkpoint (e.g., month 1)
- **Period 3 Retention**: Medium-term retention indicator
- **Period 6+ Retention**: Long-term loyalty indicator
- **Retention Curve**: Shape indicates customer lifecycle patterns

### Healthy vs. Unhealthy Patterns

**Healthy:**
- Gradual decline (e.g., 100% → 70% → 60% → 55%)
- Newer cohorts retain better than older ones
- Retention stabilizes after initial drop

**Unhealthy:**
- Steep drop-off (e.g., 100% → 30% → 15%)
- Newer cohorts retain worse than older ones
- Continuous decline without stabilization

## Use Case Examples

### E-commerce
- Track repeat purchase behavior
- Compare seasonal cohorts
- Measure loyalty program impact

### SaaS/Subscription
- Monitor subscription retention
- Calculate customer lifetime value
- Identify optimal trial-to-paid conversion period

### Mobile Apps
- Track daily/weekly active users
- Measure feature impact on retention
- Identify drop-off points in onboarding

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **Data Preparation**: Ensure data includes first activity date for each customer
- **Period Selection**: Match period to purchase cycle (daily for apps, monthly for SaaS)
- **Minimum Cohort Size**: At least 50 customers per cohort for statistical relevance
- **Incomplete Cohorts**: Recent cohorts won't have data for later periods
- **Performance**: Handles millions of records efficiently using pandas groupby operations
- **Revenue Analysis**: When enabled, also calculates average revenue per user per period
