# Revenue Attribution Component

Connect marketing spend to actual revenue by attributing conversions and revenue to marketing campaigns. Calculate ROI, ROAS, and CAC with multiple attribution models.

## Overview

The Revenue Attribution component bridges the gap between marketing spend and revenue generation. It answers critical questions like "Which campaigns drive the most revenue?" and "What's my return on ad spend?" by implementing industry-standard attribution models.

## Key Features

- **Multiple Attribution Models**: First-touch, last-touch, linear, time-decay
- **ROI & ROAS Calculation**: Measure campaign profitability
- **CAC Tracking**: Calculate customer acquisition cost per campaign
- **Attribution Windows**: Configurable lookback periods
- **Multi-Touch Support**: Credit multiple touchpoints in customer journey
- **Revenue Types**: Support for one-time purchases and recurring subscriptions

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| campaign_id | string | Campaign identifier |
| campaign_name | string | Campaign display name |
| source | string | Traffic source (google, facebook) |
| medium | string | Traffic medium (cpc, display, email) |
| total_spend | float | Total marketing spend |
| attributed_revenue | float | Revenue attributed to campaign |
| attributed_customers | int | Number of customers acquired |
| conversions | int | Number of conversions |
| roi | float | Return on investment |
| roas | float | Return on ad spend |
| cac | float | Customer acquisition cost |
| attribution_model | string | Model used for attribution |

## Attribution Models

### 1. Last Touch (Default)

Credits the last campaign interaction before conversion.

**Best for**: Understanding immediate conversion drivers

```yaml
attribution_model: last_touch
```

**Example**:
```
User journey: Google Ads → Facebook → Email (purchase)
Result: 100% credit to Email
```

### 2. First Touch

Credits the first campaign that introduced the customer.

**Best for**: Understanding top-of-funnel effectiveness

```yaml
attribution_model: first_touch
```

**Example**:
```
User journey: Google Ads → Facebook → Email (purchase)
Result: 100% credit to Google Ads
```

### 3. Linear

Distributes credit equally across all touchpoints.

**Best for**: Valuing every interaction in the journey

```yaml
attribution_model: linear
```

**Example**:
```
User journey: Google Ads → Facebook → Email (purchase)
Result: 33.3% credit to each
```

### 4. Time Decay

Gives more credit to recent interactions, less to older ones.

**Best for**: Emphasizing conversion proximity

```yaml
attribution_model: time_decay
attribution_window_days: 30
```

**Example**:
```
User journey:
- Google Ads (30 days ago): 10% credit
- Facebook (14 days ago): 30% credit
- Email (1 day ago): 60% credit
```

## Configuration

### Basic Configuration

```yaml
asset_name: revenue_attribution
attribution_model: last_touch
attribution_window_days: 30
```

### Input Sources (Connected via Visual Lineage)

1. **Marketing Data**: Campaign spend and touchpoints
   - Required fields: campaign_id, campaign_name, source, medium, spend, user_id/email, timestamp
2. **Revenue Data**: Stripe charges or subscriptions
   - Required fields: customer_id/email, amount, created_at
3. **Customer 360** (optional): Enhanced attribution with customer profiles

Connect these sources by drawing edges in the Dagster Designer UI.

### Advanced Configuration

```yaml
asset_name: revenue_attribution
attribution_model: time_decay
attribution_window_days: 60
revenue_type: recurring
group_by_fields: "source,medium,campaign"
minimum_spend_threshold: 100.00
```

## Key Metrics Explained

### ROI (Return on Investment)

```
ROI = (Revenue - Spend) / Spend × 100%
```

**Example**: $5,000 revenue, $1,000 spend → ROI = 400%

**Interpretation**:
- ROI > 0%: Campaign is profitable
- ROI < 0%: Campaign is losing money
- ROI = 400%: Every $1 spent returns $5

### ROAS (Return on Ad Spend)

```
ROAS = Revenue / Spend
```

**Example**: $5,000 revenue, $1,000 spend → ROAS = 5.0

**Interpretation**:
- ROAS > 1.0: Campaign is profitable
- ROAS < 1.0: Campaign is unprofitable
- ROAS = 5.0: Every $1 spent generates $5 revenue

### CAC (Customer Acquisition Cost)

```
CAC = Total Spend / Number of Customers Acquired
```

**Example**: $1,000 spend, 10 customers → CAC = $100

**Interpretation**:
- Compare CAC to Customer Lifetime Value (LTV)
- Healthy ratio: LTV/CAC > 3.0
- CAC should be < 30% of first-year revenue

## Use Cases

### 1. Campaign Optimization

Identify top-performing campaigns:

```python
df = context.load_asset_value("revenue_attribution")

# Best ROI campaigns
top_campaigns = df.nlargest(10, 'roi')

# Unprofitable campaigns to pause
bad_campaigns = df[df['roi'] < 0]
```

### 2. Budget Allocation

Reallocate spend based on ROAS:

```python
# High ROAS campaigns deserve more budget
high_roas = df[df['roas'] > 3.0]
print(f"Increase budget for: {high_roas['campaign_name'].tolist()}")

# Low ROAS campaigns need optimization
low_roas = df[df['roas'] < 1.0]
print(f"Optimize or pause: {low_roas['campaign_name'].tolist()}")
```

### 3. Channel Performance

Compare marketing channels:

```python
# Aggregate by source
channel_performance = df.groupby('source').agg({
    'total_spend': 'sum',
    'attributed_revenue': 'sum',
    'attributed_customers': 'sum'
})
channel_performance['roas'] = (
    channel_performance['attributed_revenue'] /
    channel_performance['total_spend']
)
```

### 4. Customer Acquisition Analysis

Track CAC trends over time:

```python
# CAC by month
monthly_cac = df.groupby('month').agg({
    'total_spend': 'sum',
    'attributed_customers': 'sum'
})
monthly_cac['cac'] = (
    monthly_cac['total_spend'] /
    monthly_cac['attributed_customers']
)
```

## Configuration Options

### Attribution Window

How far back to look for attributing touchpoints:

```yaml
attribution_window_days: 30  # Default
# or
attribution_window_days: 60  # Longer consideration cycles
# or
attribution_window_days: 7   # Short sales cycles
```

**Guidelines**:
- E-commerce: 7-30 days
- B2B SaaS: 30-90 days
- High-ticket items: 60-180 days

### Revenue Types

```yaml
revenue_type: one_time     # For e-commerce, one-off purchases
# or
revenue_type: recurring    # For subscriptions (uses MRR/ARR)
```

### Grouping

Aggregate results by multiple dimensions:

```yaml
group_by_fields: "source,medium,campaign"  # Default
# or
group_by_fields: "campaign"  # Campaign-level only
# or
group_by_fields: "source,medium"  # Channel-level
```

### Spend Threshold

Filter out low-spend campaigns:

```yaml
minimum_spend_threshold: 100.00  # Only campaigns with $100+ spend
```

## Example Pipeline

```
┌──────────────┐
│  Marketing   │
│ Spend Data   │
└──────┬───────┘
       │
       ├──────┐
       │      │    ┌──────────────┐
       │      └───▶│   Revenue    │
       │           │ Attribution  │────▶ ROI Dashboard
┌──────▼───────┐   └──────────────┘
│   Stripe     │          │
│   Revenue    │──────────┘
└──────────────┘          │
                          │
┌──────────────┐          │
│  Customer    │──────────┘
│     360      │ (optional)
└──────────────┘
```

## Best Practices

1. **Clean Marketing Data**: Ensure consistent UTM parameters across all campaigns
2. **Match Customer IDs**: Use same identifier (email) in marketing and revenue data
3. **Regular Updates**: Run daily to catch new conversions
4. **Compare Models**: Run multiple attribution models to understand impact
5. **Long Windows**: Use longer windows for high-consideration products
6. **Track Trends**: Monitor ROI/ROAS over time, not just point-in-time

## Troubleshooting

### Low Attribution Rate

**Problem**: Most revenue is unattributed

**Solutions**:
- Increase attribution_window_days
- Check that customer IDs match between marketing and revenue data
- Verify marketing data has timestamps
- Ensure UTM parameters are captured for all traffic

### All Revenue to One Campaign

**Problem**: One campaign gets 100% credit

**Solutions**:
- Check if using "last_touch" with limited data
- Try "linear" or "first_touch" models
- Verify multiple campaigns are in marketing data
- Check timestamp ordering

### ROAS Doesn't Make Sense

**Problem**: Extremely high or low ROAS values

**Solutions**:
- Verify spend data is in same currency as revenue
- Check for NULL or 0 spend values
- Ensure revenue type matches business model
- Look for data quality issues in source data

## Related Components

- **Customer 360**: Enhanced attribution with customer profiles
- **Cohort Analysis**: Analyze attributed customer cohorts
- **Subscription Metrics**: Calculate LTV for CAC payback analysis
- **Marketing Standardizer**: Normalize marketing data before attribution
- **Event Standardizer**: Track conversion events for attribution

## Learn More

- [Attribution Modeling Guide](https://www.optimizely.com/optimization-glossary/attribution-modeling/)
- [Marketing ROI Calculation](https://blog.hubspot.com/marketing/marketing-roi-calculator)
- [CAC vs LTV Benchmarks](https://www.saastr.com/the-cac-ltv-ratio/)
