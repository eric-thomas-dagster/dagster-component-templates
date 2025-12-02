# Customer Health Score Component

Predict customer churn risk and identify expansion opportunities by calculating health scores from engagement, product usage, subscription, and support data.

## Overview

The Customer Health Score component is essential for proactive customer success management. It analyzes multiple data sources to create a composite health score (0-100) that indicates customer satisfaction, engagement level, and likelihood to churn or expand.

## Key Features

- **Multi-Factor Analysis**: Combines engagement, product usage, payment health, and support data
- **Configurable Weights**: Adjust importance of each factor based on your business
- **Risk Categories**: Automatic classification (high_risk, moderate, healthy)
- **Expansion Flags**: Identify customers ready for upsell/cross-sell
- **Factor Breakdown**: See which components drive each customer's score
- **Flexible Inputs**: Works with any combination of data sources
- **Real-Time Scoring**: Calculate scores on demand or scheduled

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| customer_id | string | Unique customer identifier |
| health_score | float | Overall health score (0-100) |
| engagement_score | float | Engagement component score |
| product_usage_score | float | Product usage component score |
| payment_health_score | float | Payment/subscription component score |
| support_health_score | float | Support interaction component score |
| risk_category | string | high_risk, moderate, or healthy |
| is_churn_risk | boolean | True if score < churn_risk_threshold |
| is_expansion_opportunity | boolean | True if score > expansion_opportunity_threshold |
| calculated_at | timestamp | When the score was calculated |

## Health Score Calculation

The overall health score is a weighted average of four component scores:

```
Health Score = (Engagement × W1) + (Product Usage × W2) + (Payment Health × W3) + (Support Health × W4)
```

**Default Weights**:
- Engagement: 25%
- Product Usage: 25%
- Payment Health: 25%
- Support Health: 25%

### Engagement Score (0-100)

Measures customer engagement with your platform and marketing.

**Positive Indicators**:
- Recent logins
- High login frequency
- High feature adoption rate
- Good email open rate
- Many active days

**Negative Indicators**:
- Long time since last activity
- Declining login frequency
- Low feature adoption

**Example**:
- Customer logs in daily → High engagement score
- Customer hasn't logged in for 30 days → Low engagement score

### Product Usage Score (0-100)

Measures how deeply customers use your product.

**Positive Indicators**:
- High daily active days
- Multiple features used
- Core feature usage
- Long session duration
- Many actions per session

**Negative Indicators**:
- Infrequent usage
- Single feature usage
- Short sessions
- Declining usage trend

**Example**:
- Customer uses 8 of 10 features daily → High usage score
- Customer only uses 1 basic feature occasionally → Low usage score

### Payment Health Score (0-100)

Measures subscription and billing health.

**Positive Indicators**:
- Active subscription status
- No payment failures
- Long subscription tenure
- Higher-tier plan
- Consistent payment

**Negative Indicators**:
- Past due or unpaid status
- Multiple payment failures
- Recent downgrades
- Short tenure
- Trial without conversion

**Example**:
- Customer on annual plan, auto-renews → High payment score
- Customer with 3 failed payments → Low payment score

### Support Health Score (0-100)

Measures support interaction quality and volume.

**Positive Indicators**:
- Moderate ticket volume (1-3 per period)
- High CSAT scores
- Quick ticket resolution
- No critical issues

**Negative Indicators**:
- Many open tickets (7+)
- Critical issues
- Low CSAT scores
- Old unresolved tickets

**Example**:
- Customer opens 2 tickets/month, both resolved quickly → High support score
- Customer has 5 critical issues open for 30+ days → Low support score

## Configuration

### Basic Configuration

```yaml
asset_name: customer_health_scores
analysis_period_days: 30
churn_risk_threshold: 40
expansion_opportunity_threshold: 75
```

### Input Sources (Connected via Visual Lineage)

The component accepts 1-4 input data sources. Connect any combination:

1. **Customer Data** (CRM, user profiles)
   - Fields: customer_id, last_login_days, login_frequency, feature_adoption_rate

2. **Subscription Data** (billing, subscriptions)
   - Fields: customer_id, status, payment_failures, days_subscribed, mrr

3. **Product Usage Data** (activity, events)
   - Fields: customer_id, daily_active_days, feature_usage_count, session_count

4. **Support Ticket Data** (support interactions)
   - Fields: customer_id, ticket_count, critical_issues, csat_score

Connect by drawing edges in Dagster Designer UI from data sources → `customer_health_scores`.

### Advanced Configuration

```yaml
asset_name: customer_health_scores
analysis_period_days: 30

# Custom weights (must sum to reasonable total)
engagement_weight: 0.3
product_usage_weight: 0.4
payment_health_weight: 0.2
support_health_weight: 0.1

# Thresholds
churn_risk_threshold: 35
expansion_opportunity_threshold: 80
min_health_score: 0
max_health_score: 100

# Output options
include_factor_breakdown: true
calculate_trend: true
```

## Weight Customization

Adjust weights based on your business model:

### Product-Led Growth (PLG)

```yaml
engagement_weight: 0.15
product_usage_weight: 0.50  # Most important
payment_health_weight: 0.25
support_health_weight: 0.10
```

**Rationale**: Product usage is the primary indicator of value realization.

### Enterprise SaaS

```yaml
engagement_weight: 0.20
product_usage_weight: 0.30
payment_health_weight: 0.30
support_health_weight: 0.20  # More important for high-touch
```

**Rationale**: Payment stability and support relationship are critical.

### Consumer SaaS

```yaml
engagement_weight: 0.40  # Most important
product_usage_weight: 0.35
payment_health_weight: 0.20
support_health_weight: 0.05  # Minimal support
```

**Rationale**: High engagement drives retention in consumer products.

## Use Cases

### 1. Churn Prevention

Identify at-risk customers before they cancel:

```python
df = context.load_asset_value("customer_health_scores")

# Get churn risk customers
churn_risk = df[df['is_churn_risk'] == True].sort_values('health_score')

# Prioritize by lowest scores
critical_churn = churn_risk[churn_risk['health_score'] < 25]

print(f"Critical churn risk: {len(critical_churn)} customers")
print(f"Total churn risk: {len(churn_risk)} customers")

# Analyze what's driving low scores
if 'engagement_score' in df.columns:
    low_engagement = churn_risk[churn_risk['engagement_score'] < 30]
    print(f"Low engagement: {len(low_engagement)} customers")
```

**Actions**:
- Send to customer success team
- Trigger automated re-engagement campaign
- Offer incentive to stay

### 2. Expansion Opportunity Identification

Find customers ready to upgrade:

```python
df = context.load_asset_value("customer_health_scores")

# Get expansion opportunities
expansion = df[df['is_expansion_opportunity'] == True]

# Further filter by high product usage
power_users = expansion[expansion['product_usage_score'] > 85]

# Cross-reference with current plan (if available)
# low_tier_power_users = power_users[power_users['plan_tier'] == 'basic']

print(f"Expansion opportunities: {len(expansion)} customers")
print(f"Power users ready to upgrade: {len(power_users)} customers")
```

**Actions**:
- Route to sales for upsell conversation
- Show in-app upgrade prompts
- Send personalized upgrade offer

### 3. Customer Success Prioritization

Allocate CS resources based on health scores:

```python
df = context.load_asset_value("customer_health_scores")

# Segment customers
critical = df[df['health_score'] < 30]
at_risk = df[df['health_score'].between(30, 50)]
moderate = df[df['health_score'].between(50, 75)]
healthy = df[df['health_score'] > 75]

print("Customer Segmentation:")
print(f"  Critical (< 30): {len(critical)} - Daily check-ins")
print(f"  At Risk (30-50): {len(at_risk)} - Weekly outreach")
print(f"  Moderate (50-75): {len(moderate)} - Monthly QBRs")
print(f"  Healthy (> 75): {len(healthy)} - Quarterly reviews")
```

**Actions**:
- Assign critical customers to senior CSMs
- Create tiered engagement playbooks
- Automate communication for healthy customers

### 4. Root Cause Analysis

Understand what drives poor health scores:

```python
df = context.load_asset_value("customer_health_scores")

churn_risk = df[df['is_churn_risk'] == True]

# Find the weakest component score for each customer
score_cols = ['engagement_score', 'product_usage_score',
              'payment_health_score', 'support_health_score']

churn_risk['weakest_factor'] = churn_risk[score_cols].idxmin(axis=1)

# Count occurrences
print("\nPrimary Churn Risk Drivers:")
print(churn_risk['weakest_factor'].value_counts())

# Example output:
# engagement_score         45  → Focus on re-engagement
# product_usage_score      30  → Focus on product adoption
# payment_health_score     12  → Focus on billing issues
# support_health_score      8  → Focus on support quality
```

**Actions**:
- Build targeted intervention playbooks
- Fix systemic issues (e.g., billing problems)
- Improve product onboarding

### 5. Health Score Monitoring Dashboard

Track overall customer health over time:

```python
import matplotlib.pyplot as plt

df = context.load_asset_value("customer_health_scores")

# Distribution of health scores
plt.figure(figsize=(10, 6))
plt.hist(df['health_score'], bins=20, edgecolor='black')
plt.xlabel('Health Score')
plt.ylabel('Number of Customers')
plt.title('Customer Health Score Distribution')
plt.axvline(40, color='r', linestyle='--', label='Churn Risk Threshold')
plt.axvline(75, color='g', linestyle='--', label='Expansion Threshold')
plt.legend()
plt.show()

# Summary metrics
print(f"\nOverall Health Metrics:")
print(f"  Average Health Score: {df['health_score'].mean():.1f}")
print(f"  Median Health Score: {df['health_score'].median():.1f}")
print(f"  Churn Risk Rate: {(df['is_churn_risk'].sum() / len(df) * 100):.1f}%")
print(f"  Expansion Rate: {(df['is_expansion_opportunity'].sum() / len(df) * 100):.1f}%")
```

## Risk Categories

### High Risk (0-40)

**Characteristics**:
- Very low engagement or usage
- Payment issues or cancellations pending
- Multiple critical support issues
- Likely to churn within 30 days

**Action Required**: Immediate intervention

**Playbook**:
1. Assign to senior CSM
2. Schedule call within 24 hours
3. Identify and resolve blockers
4. Consider retention offer

### Moderate (40-75)

**Characteristics**:
- Adequate engagement and usage
- No major payment or support issues
- Could improve in one or more areas
- Stable but not growing

**Action Required**: Regular monitoring

**Playbook**:
1. Monthly check-in calls
2. Product education and training
3. Feature adoption campaigns
4. Quarterly business reviews

### Healthy (75-100)

**Characteristics**:
- High engagement and usage
- Stable payments
- Minimal support issues
- Happy and successful

**Action Required**: Maintain relationship, explore expansion

**Playbook**:
1. Quarterly strategic reviews
2. Upsell/cross-sell opportunities
3. Ask for referrals and testimonials
4. Beta program invitations

## Thresholds by Business Type

### B2C SaaS (High Volume, Low Touch)

```yaml
churn_risk_threshold: 35
expansion_opportunity_threshold: 80
```

More aggressive thresholds since intervention is automated.

### B2B SaaS (Mid-Market)

```yaml
churn_risk_threshold: 40
expansion_opportunity_threshold: 75
```

Balanced thresholds for manual+automated intervention.

### Enterprise SaaS (High Touch)

```yaml
churn_risk_threshold: 50
expansion_opportunity_threshold: 70
```

Higher thresholds because proactive outreach happens earlier.

## Best Practices

1. **Start with Equal Weights**: Use default 25% weights until you have data
2. **Calibrate Thresholds**: Adjust based on your churn rate and intervention capacity
3. **Track Score Changes**: Monitor trends, not just point-in-time scores
4. **Segment by Customer Type**: Different thresholds for different segments
5. **Act on Insights**: Health scores are useless without action
6. **Validate Predictiveness**: Does low score actually predict churn?
7. **Update Regularly**: Recalculate scores at least weekly
8. **Close the Loop**: Track interventions and outcomes

## Troubleshooting

### All Scores Are 50 (Neutral)

**Problem**: Every customer has a score around 50

**Solutions**:
- Check that input data is connected
- Verify input data has expected columns
- Look for data quality issues (NULLs, zeros)
- Ensure date ranges match analysis_period_days

### Scores Don't Match Reality

**Problem**: Known healthy customers have low scores (or vice versa)

**Solutions**:
- Adjust component weights for your business
- Check threshold settings
- Verify input data quality
- Validate field mappings

### Too Many Churn Risk Customers

**Problem**: 50%+ of customers flagged as churn risk

**Solutions**:
- Lower churn_risk_threshold (try 30 instead of 40)
- Increase weights for reliable indicators
- Improve data quality
- Consider if baseline health is actually low

### Missing Component Scores

**Problem**: Some component scores are always 50 (neutral)

**Solutions**:
- Ensure corresponding data source is connected
- Check that data source has expected fields
- Verify field names match expected patterns
- Add custom field mapping if needed

### Scores Not Updating

**Problem**: Health scores don't change over time

**Solutions**:
- Check materialization schedule
- Verify input data is updating
- Confirm analysis_period_days is appropriate
- Look for caching issues

## Example Pipeline

```
┌─────────────┐
│     CRM     │
│    Data     │
└──────┬──────┘
       │
       ├─────────────────┐
       │                 │
┌──────▼──────┐   ┌──────▼──────┐
│ Subscription│   │   Product   │
│    Data     │   │    Usage    │
└──────┬──────┘   └──────┬──────┘
       │                 │
       └────────┬────────┘
                │
         ┌──────▼──────┐
         │   Support   │
         │   Tickets   │
         └──────┬──────┘
                │
                ▼
         ┌─────────────┐
         │  Customer   │
         │   Health    │
         │   Scores    │
         └──────┬──────┘
                │
                ├──────────────────┐
                │                  │
         ┌──────▼──────┐    ┌──────▼──────┐
         │   Churn     │    │  Expansion  │
         │ Prevention  │    │   Pipeline  │
         │  Campaign   │    │             │
         └─────────────┘    └─────────────┘
```

## Related Components

- **CRM Ingestion**: Source customer data
- **Subscription Metrics**: Source subscription data
- **Product Usage Analytics**: Source usage data (Phase 3)
- **Support Ticket Standardizer**: Source support data
- **Customer 360**: Unified customer view
- **Churn Prediction**: ML-based churn prediction (Phase 4+)

## Learn More

- [Customer Success Metrics](https://www.gainsight.com/guides/the-essential-guide-to-customer-success-metrics/)
- [Health Score Best Practices](https://www.gainsight.com/blog/customer-health-score-best-practices/)
- [Churn Prevention Strategies](https://www.profitwell.com/recur/all/churn-prevention)
- [Expansion Revenue Playbook](https://www.paddle.com/resources/expansion-revenue)
