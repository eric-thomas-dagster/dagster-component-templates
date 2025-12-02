# RFM Segmentation Component

Segment customers by **Recency**, **Frequency**, and **Monetary** value to identify high-value customers, at-risk customers, and opportunities for re-engagement.

## Overview

RFM analysis is a proven marketing technique that segments customers based on three key metrics:

- **Recency (R)**: How recently did the customer make a purchase?
- **Frequency (F)**: How often do they purchase?
- **Monetary (M)**: How much do they spend?

This component analyzes transaction data and assigns customers to segments like:
- **Champions**: Best customers (high R, F, M)
- **Loyal Customers**: Regular buyers (high F, M)
- **At Risk**: Used to buy frequently but haven't recently (low R, high F, M)
- **Lost**: Haven't purchased in a long time (low R, F, M)
- **Promising**: Recent customers with potential (high R, low F, M)
- **Need Attention**: Below average recency, frequency, and monetary

## Use Cases

- **Targeted Marketing**: Send personalized campaigns to different segments
- **Customer Retention**: Identify at-risk customers before they churn
- **Revenue Optimization**: Focus on high-value customer segments
- **Acquisition Strategy**: Analyze characteristics of Champions to find similar prospects
- **Loyalty Programs**: Reward loyal customers appropriately

## Input Requirements

The component expects transaction-level data with the following columns:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `customer_id` | string | ✓ | user_id, customerId, userId, id | Unique customer identifier |
| `order_date` | datetime | ✓ | date, transaction_date, created_at, timestamp | Transaction timestamp |
| `order_id` | string | ✓ | transaction_id, orderId, invoice_id | Unique transaction identifier |
| `revenue` | number | ✓ | amount, total, price, value, spend | Transaction amount |

**Compatible Upstream Components:**
- `shopify_ingestion`
- `stripe_ingestion`
- `ecommerce_standardizer`
- `salesforce_ingestion`
- `hubspot_ingestion`

## Output Schema

Returns a DataFrame with one row per customer and their RFM analysis:

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | string | Unique customer identifier |
| `recency_days` | number | Days since last purchase |
| `frequency` | number | Total number of purchases |
| `monetary` | number | Total revenue from customer |
| `r_score` | number | Recency score (1-5 or 1-4) |
| `f_score` | number | Frequency score (1-5 or 1-4) |
| `m_score` | number | Monetary score (1-5 or 1-4) |
| `rfm_score` | string | Combined score (e.g., "555") |
| `rfm_segment` | string | Segment name (e.g., "Champions") |
| `segment_label` | string | Descriptive segment label |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for the output asset (e.g., `customer_rfm_segments`)

### Optional Parameters

- **`source_asset`** (string): Upstream asset name (auto-set via lineage)
- **`scoring_method`** (string): Scoring approach
  - `quintile` (default): 1-5 scale, more granular
  - `quartile`: 1-4 scale, simpler segmentation
- **`lookback_days`** (number): Analysis window in days (default: 365)
- **`customer_id_field`** (string): Custom column name for customer ID (auto-detected)
- **`order_date_field`** (string): Custom column name for order date (auto-detected)
- **`order_id_field`** (string): Custom column name for order ID (auto-detected)
- **`revenue_field`** (string): Custom column name for revenue (auto-detected)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `customer_analytics`)
- **`include_sample_metadata`** (boolean): Include data preview (default: true)

## Example Configuration

```yaml
type: dagster_component_templates.RFMSegmentationComponent
attributes:
  asset_name: customer_rfm_segments
  scoring_method: quintile
  lookback_days: 365
  description: Customer segmentation by RFM analysis
  group_name: customer_analytics
```

## How It Works

1. **Calculate Metrics**: For each customer:
   - Recency = days since last purchase
   - Frequency = total number of purchases
   - Monetary = total revenue

2. **Score Assignment**: Customers are ranked and divided into quintiles (1-5) or quartiles (1-4):
   - **Recency**: Lower days = higher score (5 = recent, 1 = long ago)
   - **Frequency**: More purchases = higher score
   - **Monetary**: Higher spend = higher score

3. **Segmentation**: Based on RFM score combinations:
   - `555` = Champions (best customers)
   - `455` = Loyal Customers
   - `155` = At Risk (high value but not recent)
   - `111` = Lost customers
   - And more...

## Segment Definitions

| Segment | R | F | M | Description | Action |
|---------|---|---|---|-------------|--------|
| Champions | 4-5 | 4-5 | 4-5 | Best customers | Reward loyalty, ask for reviews |
| Loyal Customers | 3-5 | 3-5 | 3-5 | Regular buyers | Upsell, cross-sell |
| Potential Loyalists | 4-5 | 1-3 | 1-3 | Recent customers | Nurture, educate |
| At Risk | 1-2 | 3-5 | 3-5 | Used to buy often | Win-back campaigns |
| Lost | 1-2 | 1-2 | 1-2 | Gone | Aggressive win-back or let go |
| Promising | 4-5 | 1-2 | 1-2 | New customers | Engagement campaigns |
| Need Attention | 2-3 | 2-3 | 2-3 | Below average | Re-engage |

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **No ML Required**: Uses simple statistical scoring, no model training needed
- **Data Requirements**: Minimum of ~100 customers recommended for meaningful segmentation
- **Lookback Period**: Adjust based on purchase cycle (e.g., 180 days for frequent purchases, 730 for infrequent)
- **Column Detection**: Component automatically detects common column name variations
- **Performance**: Handles millions of transactions efficiently using pandas vectorization
