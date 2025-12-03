# Lifetime Value (LTV) Prediction Component

Predict customer lifetime value using historical purchase patterns. Calculate both historical LTV and projected future value to identify your most valuable customers and optimize acquisition spend.

## Overview

Lifetime Value (LTV) prediction calculates the total revenue a customer is expected to generate over their entire relationship with your business. This component analyzes historical transaction data to predict future value using proven formulas:

**Predicted LTV = Historical LTV + (Average Order Value × Purchase Frequency × Remaining Months)**

This helps you:
- Identify high-value customers worth investing in
- Set appropriate customer acquisition costs (CAC)
- Optimize retention efforts by predicted value
- Segment customers by lifetime value tier
- Calculate LTV:CAC ratio for profitability analysis

## Use Cases

- **Marketing ROI**: Determine acceptable customer acquisition costs
- **Customer Segmentation**: Prioritize high-LTV customers for VIP treatment
- **Retention Strategy**: Focus retention efforts on high-value at-risk customers
- **Product Development**: Understand which customer segments drive the most value
- **Financial Forecasting**: Project future revenue from existing customer base
- **Churn Impact Analysis**: Calculate revenue impact of potential churn

## Input Requirements

Transaction/order data with customer purchase history:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `customer_id` | string | ✓ | user_id, customerId, userId, id | Unique customer identifier |
| `date` | datetime | ✓ | transaction_date, order_date, purchase_date, created_at | Transaction timestamp |
| `amount` | number | ✓ | total, revenue, value, order_total | Transaction amount |

**Compatible Upstream Components:**
- `ecommerce_standardizer`
- `crm_data_standardizer` (deals)
- `stripe_ingestion`
- `shopify_ingestion`
- Any component that outputs transaction data

## Output Schema

Returns one row per customer with LTV predictions:

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | string | Unique customer identifier |
| `first_purchase_date` | datetime | Date of first transaction |
| `last_purchase_date` | datetime | Date of most recent transaction |
| `cohort_month` | string | Month of first purchase (if cohort_analysis=true) |
| `total_transactions` | number | Total number of purchases |
| `avg_order_value` | number | Average transaction amount |
| `purchase_frequency_monthly` | number | Average purchases per month |
| `historical_ltv` | number | Total revenue to date |
| `predicted_total_ltv` | number | Predicted lifetime value |
| `value_segment` | string | Platinum, Gold, Silver, or Bronze tier |
| `ltv_percentile` | number | Percentile rank (0-100) |
| `prediction_confidence` | number | Confidence score (0.3-0.95, optional) |
| `ltv_lower_bound` | number | Lower confidence interval (optional) |
| `ltv_upper_bound` | number | Upper confidence interval (optional) |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for the output asset (e.g., `customer_ltv`)

### Optional Parameters

- **`source_asset`** (string): Upstream asset name (auto-set via lineage)
- **`prediction_period_months`** (number): Months to project forward (default: 24)
- **`cohort_analysis`** (boolean): Include cohort-based metrics (default: true)
- **`include_confidence_intervals`** (boolean): Add confidence bounds (default: true)
- **`min_transactions_required`** (number): Minimum purchases to include (default: 2)
- **`customer_id_field`** (string): Custom column name (auto-detected)
- **`transaction_date_field`** (string): Custom column name (auto-detected)
- **`amount_field`** (string): Custom column name (auto-detected)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `customer_analytics`)
- **`include_sample_metadata`** (boolean): Include data preview (default: true)

## Example Configuration

### Basic LTV Prediction

```yaml
type: dagster_component_templates.LTVPredictionComponent
attributes:
  asset_name: customer_ltv
  prediction_period_months: 24
  description: Customer lifetime value prediction
```

### With Custom Thresholds

```yaml
type: dagster_component_templates.LTVPredictionComponent
attributes:
  asset_name: high_value_customers
  prediction_period_months: 36
  min_transactions_required: 3
  cohort_analysis: true
  include_confidence_intervals: true
  description: High-value customer LTV analysis
```

### Connected to Upstream Asset

```yaml
# In Dagster Designer, draw an edge from:
# shopify_orders → customer_ltv

type: dagster_component_templates.LTVPredictionComponent
attributes:
  asset_name: customer_ltv
  prediction_period_months: 24
```

## How It Works

### 1. Historical Metrics Calculation

For each customer, calculate:
- **Average Order Value (AOV)**: Mean transaction amount
- **Purchase Frequency**: Transactions per month
- **Historical LTV**: Total revenue to date
- **Customer Lifespan**: Days between first and last purchase

### 2. Future Value Prediction

Estimate remaining customer lifetime using:
- Median lifespan across all customers
- Days since last purchase (churn indicator)
- Prediction period setting

**Formula:**
```
Predicted Additional LTV = AOV × Purchase Frequency × Estimated Remaining Months
Predicted Total LTV = Historical LTV + Predicted Additional LTV
```

### 3. Confidence Scoring

Confidence is based on purchase consistency:
- **High Confidence (0.8-0.95)**: Consistent order values
- **Medium Confidence (0.6-0.79)**: Some variation in orders
- **Lower Confidence (0.3-0.59)**: Highly variable order values

Confidence intervals are calculated as ±20% adjusted by confidence score.

### 4. Value Segmentation

Customers are grouped into tiers:
- **Platinum (Top 10%)**: Highest predicted LTV
- **Gold (75-90th percentile)**: High-value customers
- **Silver (50-75th percentile)**: Mid-tier customers
- **Bronze (Bottom 50%)**: Lower-value customers

### 5. Cohort Analysis

When enabled, customers are grouped by first purchase month to analyze:
- Cohort size trends
- Average LTV by cohort
- Purchase patterns over time
- Cohort retention and value

## Reading the Results

### Value Segments

Use value segments to prioritize customer treatment:

| Segment | Use Case | Strategy |
|---------|----------|----------|
| **Platinum** | VIP customers | White-glove service, exclusive perks, dedicated support |
| **Gold** | High-value customers | Priority support, loyalty rewards, early access |
| **Silver** | Mid-tier customers | Standard service, occasional promotions |
| **Bronze** | Lower-value customers | Automated service, upsell opportunities |

### Confidence Intervals

When confidence is < 0.7:
- Customer has inconsistent purchase behavior
- Prediction is less reliable
- Consider as a range rather than point estimate

### Cohort Insights

Cohort analysis reveals:
- Which acquisition periods brought the best customers
- Seasonal patterns in customer value
- Whether recent cohorts are more/less valuable
- Long-term retention trends

## Use Case Examples

### 1. Setting Customer Acquisition Costs

Determine maximum CAC based on LTV:

```python
df = context.load_asset_value("customer_ltv")

avg_ltv = df['predicted_total_ltv'].mean()
target_ltv_cac_ratio = 3  # Industry standard: 3:1

max_cac = avg_ltv / target_ltv_cac_ratio

print(f"Average Predicted LTV: ${avg_ltv:,.2f}")
print(f"Maximum CAC (3:1 ratio): ${max_cac:,.2f}")
```

### 2. VIP Program Identification

Select top customers for VIP treatment:

```python
df = context.load_asset_value("customer_ltv")

# Top 5% of customers by predicted LTV
ltv_threshold = df['predicted_total_ltv'].quantile(0.95)
vip_customers = df[df['predicted_total_ltv'] >= ltv_threshold]

print(f"VIP Threshold: ${ltv_threshold:,.2f}")
print(f"VIP Customers: {len(vip_customers)}")
print(f"Total VIP Predicted Value: ${vip_customers['predicted_total_ltv'].sum():,.2f}")

# Export for CRM integration
vip_customers[['customer_id', 'predicted_total_ltv']].to_csv('vip_customers.csv')
```

### 3. Churn Impact Analysis

Calculate revenue risk from at-risk customers:

```python
# Combine with churn prediction
ltv_df = context.load_asset_value("customer_ltv")
churn_df = context.load_asset_value("customer_churn_risk")

risk_analysis = ltv_df.merge(churn_df[['customer_id', 'churn_risk_level']], on='customer_id')

# Calculate revenue at risk
high_risk = risk_analysis[risk_analysis['churn_risk_level'].isin(['High', 'Critical'])]
revenue_at_risk = high_risk['predicted_total_ltv'].sum()

print(f"Revenue at Risk: ${revenue_at_risk:,.2f}")
print(f"Customers at Risk: {len(high_risk)}")

# Prioritize by LTV
high_value_at_risk = high_risk.sort_values('predicted_total_ltv', ascending=False)
print("\nTop 10 At-Risk Customers by LTV:")
print(high_value_at_risk[['customer_id', 'predicted_total_ltv', 'churn_risk_level']].head(10))
```

### 4. Cohort Value Analysis

Compare customer quality across acquisition periods:

```python
df = context.load_asset_value("customer_ltv")

cohort_analysis = df.groupby('cohort_month').agg({
    'customer_id': 'count',
    'predicted_total_ltv': ['mean', 'sum'],
    'purchase_frequency_monthly': 'mean'
}).round(2)

cohort_analysis.columns = ['customers', 'avg_ltv', 'total_ltv', 'avg_frequency']
cohort_analysis = cohort_analysis.sort_values('avg_ltv', ascending=False)

print("Top 10 Cohorts by Average LTV:")
print(cohort_analysis.head(10))

# Identify what made these cohorts successful
best_cohorts = cohort_analysis.head(3).index
print(f"\nBest performing cohorts: {', '.join(map(str, best_cohorts))}")
```

### 5. Resource Allocation by LTV

Allocate customer success resources proportionally:

```python
df = context.load_asset_value("customer_ltv")

# Calculate total predicted value by segment
segment_value = df.groupby('value_segment').agg({
    'customer_id': 'count',
    'predicted_total_ltv': 'sum'
}).round(2)

segment_value.columns = ['customers', 'total_ltv']
segment_value['ltv_percent'] = (segment_value['total_ltv'] / segment_value['total_ltv'].sum() * 100).round(1)

# Allocate 10 FTEs proportionally
segment_value['allocated_staff'] = (segment_value['ltv_percent'] / 100 * 10).round(1)

print("Resource Allocation by Value Segment:")
print(segment_value)
```

## Best Practices

### Data Requirements

**Minimum Data:**
- 6 months of transaction history
- At least 100 customers with 2+ purchases
- Clean, deduplicated customer IDs

**Optimal Data:**
- 12-24 months of history
- 1,000+ customers
- Consistent tracking of returns/refunds

### Tuning Parameters

**Prediction Period:**
- **E-commerce**: 12-24 months (shorter customer lifecycles)
- **SaaS**: 24-36 months (annual contracts, longer retention)
- **High-ticket B2B**: 36-60 months (long sales cycles)

**Minimum Transactions:**
- **Default (2)**: Filters one-time buyers, focuses on repeat customers
- **Higher (3-5)**: More reliable predictions, smaller dataset
- **Lower (1)**: Includes all customers, less accurate predictions

### Handling Edge Cases

**New Customers (< 30 days):**
- Excluded by default
- Not enough history for reliable prediction
- Use lead scoring or propensity models instead

**One-time Buyers:**
- Filtered when min_transactions >= 2
- Can include but will have low predicted LTV
- Consider separate win-back analysis

**High Variance Customers:**
- Indicated by low prediction_confidence
- May be business vs. personal shoppers
- Consider segmenting separately

### Validation

**Model Validation:**
1. Calculate LTV predictions on historical data
2. Wait 3-6 months
3. Compare predictions to actual revenue
4. Adjust prediction_period_months if needed

**Segment Validation:**
- Do Platinum customers actually generate more revenue?
- Are Gold customers staying longer than Bronze?
- Does segment distribution match expectations?

## Limitations

1. **Not Predictive of Churn**: Assumes customers continue historical patterns
2. **No External Factors**: Doesn't account for seasonality, market changes, competition
3. **Requires Repeat Customers**: Not suitable for one-time purchase businesses
4. **Linear Assumptions**: Assumes constant purchase frequency (may not hold)
5. **Historical Bias**: Predictions based on past behavior, not future intent

For more sophisticated predictions, consider:
- Machine learning-based LTV models
- Survival analysis for churn timing
- Cohort-based retention curves
- Incorporating engagement metrics

## Integration Examples

### With Churn Prediction

```
Transaction Data → LTV Prediction ↘
                                    → Combined Risk/Value Analysis → Retention Prioritization
Event Data → Churn Prediction ↗
```

### With Segmentation

```
Transaction Data → LTV Prediction ↘
                                    → Multi-Dimensional Segmentation → Targeted Campaigns
Transaction Data → RFM Segmentation ↗
```

### Full Customer Analytics Pipeline

```
Transaction Data → LTV Prediction → Customer 360 → Executive Dashboard
Event Data → Churn Prediction ↗          ↑
CRM Data → Lead Scoring ↗                 |
Support Data -------------------------→   |
```

## Key Metrics to Track

### Customer-Level
- Predicted LTV vs. actual revenue (validation)
- LTV by acquisition channel
- LTV by product category
- LTV:CAC ratio

### Cohort-Level
- Average LTV by cohort month
- LTV trends over time
- Retention rates by cohort
- Time to payback by cohort

### Business-Level
- Total predicted future revenue
- Weighted average LTV
- LTV distribution (concentration risk)
- High-value customer percentage

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **Update Frequency**: Run weekly or monthly to reflect new transactions
- **Confidence Intervals**: Use ranges for budgeting and forecasting
- **Segmentation**: Adjust percentile thresholds based on your business
- **Performance**: Handles millions of transactions efficiently
- **Incremental**: Works well with incremental transaction data
- **Cohort Analysis**: Essential for understanding acquisition quality over time
