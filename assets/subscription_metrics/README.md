# Subscription Metrics Component

Calculate essential SaaS metrics from Stripe subscription data including MRR, ARR, churn rate, and customer lifetime value (LTV).

## Overview

The Subscription Metrics component is purpose-built for SaaS businesses using Stripe. It automatically calculates the key performance indicators that every subscription business needs to track, from monthly recurring revenue to churn rates and customer lifetime value.

## Key Features

- **Core SaaS Metrics**: MRR, ARR, churn rate, LTV, ARPU
- **Growth Components**: New MRR, expansion MRR, contraction MRR, reactivation MRR
- **Multiple Calculation Periods**: Daily, weekly, monthly snapshots
- **Flexible LTV Methods**: Historical, cohort-based, or predictive
- **Stripe Native**: Works directly with Stripe subscription data
- **Time Series Output**: Track metrics over time for trend analysis

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| metric_date | date | Date of metric snapshot |
| mrr | float | Monthly recurring revenue |
| arr | float | Annual recurring revenue (MRR × 12) |
| active_subscriptions | int | Number of active subscriptions |
| new_subscriptions | int | New subscriptions this period |
| churned_subscriptions | int | Cancelled subscriptions |
| churn_rate | float | Percentage of customers who cancelled |
| new_mrr | float | MRR from new subscriptions |
| expansion_mrr | float | MRR from upgrades |
| contraction_mrr | float | MRR lost from downgrades |
| reactivation_mrr | float | MRR from reactivated customers |
| net_mrr_growth_rate | float | Overall MRR growth percentage |
| arpu | float | Average revenue per user |
| ltv | float | Customer lifetime value |

## Key Metrics Explained

### MRR (Monthly Recurring Revenue)

The predictable monthly revenue from all active subscriptions.

```
MRR = Sum of all active subscription amounts (normalized to monthly)
```

**Example**:
- 50 customers at $99/month = $4,950 MRR
- 20 customers at $999/year = $1,665 MRR (normalized)
- **Total MRR**: $6,615

### ARR (Annual Recurring Revenue)

Annualized MRR, the standard metric for reporting.

```
ARR = MRR × 12
```

**Example**: $6,615 MRR = $79,380 ARR

### Churn Rate

Percentage of customers who cancelled in the period.

```
Churn Rate = (Churned Subscriptions / Starting Subscriptions) × 100%
```

**Example**: 5 cancellations, 100 starting subs = 5% monthly churn

**Healthy Benchmarks**:
- B2C SaaS: 3-7% monthly
- B2B SaaS: 0.5-1% monthly
- Enterprise SaaS: < 0.5% monthly

### Customer Lifetime Value (LTV)

Expected total revenue from a customer over their lifetime.

```
LTV = ARPU / Churn Rate
```

**Example**:
- ARPU: $100/month
- Churn: 5% monthly
- LTV: $100 / 0.05 = $2,000

### ARPU (Average Revenue Per User)

Average monthly revenue per active subscription.

```
ARPU = MRR / Active Subscriptions
```

**Example**: $6,615 MRR / 70 subscriptions = $94.50 ARPU

### MRR Components

#### New MRR
Revenue from brand new customers who subscribed this period.

#### Expansion MRR
Additional revenue from existing customers who upgraded their plans.

#### Contraction MRR
Lost revenue from customers who downgraded (negative value).

#### Reactivation MRR
Revenue from previously churned customers who re-subscribed.

#### Net MRR Growth Rate
```
Net MRR Growth = (New + Expansion + Reactivation - Contraction - Churned) / Starting MRR
```

## Configuration

### Basic Configuration

```yaml
asset_name: subscription_metrics
calculation_period: monthly
ltv_method: historical
```

### Input Sources (Connected via Visual Lineage)

**Stripe Data Asset** (Required)
- Connect any Stripe ingestion component
- Must include subscription data
- Optionally includes customer and charge data

Connect by drawing an edge in Dagster Designer UI from `stripe_data` → `subscription_metrics`.

### Advanced Configuration

```yaml
asset_name: subscription_metrics
calculation_period: daily
ltv_method: cohort_based
include_trial_subscriptions: false
churn_grace_period_days: 7
exclude_one_time_purchases: true
```

## Calculation Periods

### Monthly (Default)

Calculate metrics at monthly intervals.

```yaml
calculation_period: monthly
```

**Best for**:
- Standard SaaS reporting
- Board presentations
- Comparing to industry benchmarks

**Output**: One row per month

### Weekly

Calculate metrics weekly for more granular tracking.

```yaml
calculation_period: weekly
```

**Best for**:
- Early-stage companies tracking rapid growth
- A/B testing impact analysis
- Real-time monitoring

**Output**: One row per week

### Daily

Calculate metrics every day for maximum granularity.

```yaml
calculation_period: daily
```

**Best for**:
- Real-time dashboards
- Detecting anomalies quickly
- High-growth or high-churn scenarios

**Output**: One row per day

## LTV Calculation Methods

### Historical (Default)

Uses actual historical churn rate.

```yaml
ltv_method: historical
```

**Formula**: `LTV = ARPU / Historical Churn Rate`

**Best for**: Mature businesses with stable churn

### Cohort-Based

Calculates LTV separately for each customer cohort.

```yaml
ltv_method: cohort_based
```

**Formula**: Tracks actual revenue from cohorts over time

**Best for**: Businesses with improving retention, seasonal patterns

### Predictive

Uses predictive modeling for forward-looking LTV.

```yaml
ltv_method: predictive
```

**Formula**: Machine learning model based on customer attributes

**Best for**: Early-stage companies without historical data

## Use Cases

### 1. Growth Monitoring

Track key metrics over time:

```python
df = context.load_asset_value("subscription_metrics")

# Plot MRR growth
import matplotlib.pyplot as plt
plt.plot(df['metric_date'], df['mrr'])
plt.title('MRR Growth Over Time')
```

### 2. Churn Analysis

Identify churn problems:

```python
# Months with high churn
high_churn = df[df['churn_rate'] > 0.05]

# Churn trend
df['churn_rate_ma'] = df['churn_rate'].rolling(3).mean()
```

### 3. Unit Economics

Calculate key ratios:

```python
# LTV/CAC ratio (combine with revenue_attribution)
df['ltv_cac_ratio'] = df['ltv'] / df['cac']

# Months to recover CAC
df['cac_payback_months'] = df['cac'] / df['arpu']
```

### 4. Revenue Forecasting

Project future revenue:

```python
# Simple linear forecast
from sklearn.linear_model import LinearRegression

X = df['months_since_start'].values.reshape(-1, 1)
y = df['mrr'].values

model = LinearRegression().fit(X, y)
future_mrr = model.predict([[36]])  # 36 months out
```

## Configuration Options

### Trial Subscriptions

Include or exclude trial subscriptions:

```yaml
include_trial_subscriptions: false  # Default
```

Set to `true` to count trials in MRR (useful for pipeline metrics).

### Churn Grace Period

Allow for failed payment recovery:

```yaml
churn_grace_period_days: 7  # Default
```

Subscriptions aren't counted as churned until grace period expires.

### One-Time Purchases

Exclude non-recurring revenue:

```yaml
exclude_one_time_purchases: true  # Default
```

Ensures only subscription revenue is counted.

## Example Pipeline

```
┌──────────────┐
│    Stripe    │
│    Data      │
│  (Ingestion) │
└──────┬───────┘
       │
       │
       ▼
┌──────────────┐
│ Subscription │
│   Metrics    │────┐
└──────────────┘    │
                    ├──▶ Growth Dashboard
                    │
                    ├──▶ Board Report
                    │
                    └──▶ Revenue Forecast
```

## Best Practices

1. **Consistent Periods**: Use same calculation_period for trend comparison
2. **Monitor Trends**: Track metrics over time, not point-in-time
3. **Segment Analysis**: Break down by plan, cohort, or customer type
4. **Benchmark**: Compare to industry standards
5. **Action Thresholds**: Set alerts for churn rate spikes
6. **Validate**: Cross-check against Stripe dashboard monthly

## Key Benchmarks

### SaaS Metrics by Stage

**Early Stage (< $1M ARR)**
- MRR Growth: 10-20% monthly
- Churn Rate: 3-5% monthly
- LTV/CAC: > 1.0

**Growth Stage ($1-10M ARR)**
- MRR Growth: 5-10% monthly
- Churn Rate: 1-3% monthly
- LTV/CAC: > 3.0

**Scale Stage ($10M+ ARR)**
- MRR Growth: 3-5% monthly
- Churn Rate: 0.5-2% monthly
- LTV/CAC: > 5.0

## Troubleshooting

### MRR Doesn't Match Stripe

**Problem**: Calculated MRR differs from Stripe dashboard

**Solutions**:
- Check if trial subscriptions are included
- Verify grace period settings
- Ensure subscription dates are correct
- Confirm currency normalization

### Churn Rate Too High

**Problem**: Churn rate seems unrealistic

**Solutions**:
- Check churn_grace_period_days setting
- Verify subscription status transitions
- Look for data quality issues
- Confirm calculation_period matches expectations

### LTV Seems Wrong

**Problem**: LTV values don't make sense

**Solutions**:
- Try different ltv_method
- Check if churn rate is too low/high
- Verify ARPU calculation
- Consider using cohort_based for more accuracy

### Missing Time Periods

**Problem**: Gaps in time series output

**Solutions**:
- Ensure Stripe data covers all periods
- Check for data pipeline failures
- Verify subscription created_at dates
- Look for timezone issues

## Related Components

- **Stripe Ingestion**: Source data for this component
- **Revenue Attribution**: Combine with CAC for LTV/CAC ratio
- **Customer 360**: Enhanced customer analysis
- **Cohort Analysis**: Deep-dive into cohort retention
- **Churn Prediction**: Predict which customers will churn

## Learn More

- [SaaS Metrics Guide](https://www.saastr.com/saastr-saas-metrics-guide/)
- [Churn Rate Benchmarks](https://www.profitwell.com/recur/all/churn-rate-benchmarks)
- [LTV Calculation Methods](https://www.cobloom.com/blog/saas-ltv-calculation)
- [MRR Components](https://chartmogul.com/blog/monthly-recurring-revenue-mrr/)
