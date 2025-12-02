# Churn Prediction Component

Predict customer churn risk using heuristic scoring. Identify at-risk customers before they leave and take proactive retention actions.

## Overview

This component analyzes customer behavior to predict churn risk using a weighted scoring system. Unlike ML-based approaches, it uses interpretable heuristics based on:

- **Inactivity**: How long since last activity
- **Activity Decline**: Comparing recent vs. historical activity
- **Value Decline**: Changes in spending patterns
- **Frequency Decline**: Changes in purchase frequency

Each customer receives a **churn risk score (0-100)** and is classified into risk levels with actionable recommendations.

## Use Cases

- **Proactive Retention**: Reach out to at-risk customers before they churn
- **Targeted Campaigns**: Send win-back offers to high-risk segments
- **Customer Success**: Prioritize outreach for high-value at-risk customers
- **Revenue Protection**: Identify and save customers before lost revenue
- **A/B Testing**: Test retention strategies on different risk segments
- **Executive Dashboards**: Monitor churn risk trends over time

## Input Requirements

The component expects customer-level metrics:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `customer_id` | string | ✓ | user_id, customerId, userId, id | Unique customer identifier |
| `last_activity_date` | datetime | ✓ | last_order_date, last_purchase_date, last_seen | Most recent activity timestamp |
| `total_orders` | number | ✓ | order_count, num_orders, orders | Total number of orders |
| `total_revenue` | number | ✓ | lifetime_value, ltv, total_spend | Total customer revenue |
| `lifetime_days` | number | ✓ | customer_age_days, days_since_first_order | Days since first order |

**Compatible Upstream Components:**
- `customer_360`
- `rfm_segmentation`

## Output Schema

Returns one row per customer with churn risk assessment:

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | string | Unique customer identifier |
| `days_inactive` | number | Days since last activity |
| `activity_trend` | string | "Increasing", "Stable", or "Declining" |
| `churn_risk_score` | number | Risk score 0-100 (higher = more likely to churn) |
| `churn_risk_level` | string | "Critical", "High", "Medium", "Low" |
| `recommended_action` | string | Suggested retention action |
| `risk_factors` | string | Detailed risk factor breakdown (optional) |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for the output asset (e.g., `customer_churn_risk`)

### Optional Parameters

- **`source_asset`** (string): Upstream asset name (auto-set via lineage)
- **`inactivity_threshold_days`** (number): Days of inactivity = high risk (default: 90)
- **`lookback_days`** (number): Historical comparison window (default: 365)
- **`include_risk_factors`** (boolean): Include detailed risk breakdown (default: true)
- **`customer_id_field`** (string): Custom column name (auto-detected)
- **`last_activity_field`** (string): Custom column name (auto-detected)
- **`total_orders_field`** (string): Custom column name (auto-detected)
- **`total_revenue_field`** (string): Custom column name (auto-detected)
- **`lifetime_days_field`** (string): Custom column name (auto-detected)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `customer_analytics`)
- **`include_sample_metadata`** (boolean): Include data preview (default: true)

## Example Configuration

### Basic Churn Risk Scoring

```yaml
type: dagster_component_templates.ChurnPredictionComponent
attributes:
  asset_name: customer_churn_risk
  inactivity_threshold_days: 90
  lookback_days: 365
  description: Customer churn risk prediction
  group_name: customer_analytics
```

### With Custom Thresholds

```yaml
type: dagster_component_templates.ChurnPredictionComponent
attributes:
  asset_name: churn_risk_analysis
  inactivity_threshold_days: 60  # More aggressive threshold
  lookback_days: 180  # Shorter comparison window
  include_risk_factors: true
  description: High-sensitivity churn detection
```

## How It Works

### Scoring Algorithm

The churn risk score is calculated using a weighted formula:

```
churn_risk_score = (
    40% × inactivity_score +
    25% × activity_decline_score +
    20% × value_decline_score +
    15% × frequency_decline_score
) × 10
```

Results in a 0-100 scale where:
- **0-25**: Low risk
- **26-50**: Medium risk
- **51-75**: High risk
- **76-100**: Critical risk

### Component Scores

1. **Inactivity Score (40% weight)**
   - Based on `days_inactive / inactivity_threshold_days`
   - Higher weight because recent inactivity is the strongest churn signal

2. **Activity Decline Score (25% weight)**
   - Compares recent activity frequency vs. historical average
   - Detects customers who used to be active but are slowing down

3. **Value Decline Score (20% weight)**
   - Compares recent spending vs. historical average
   - Identifies customers spending less than before

4. **Frequency Decline Score (15% weight)**
   - Compares recent order frequency vs. historical
   - Tracks changes in purchase cadence

### Risk Levels and Actions

| Risk Level | Score | Description | Recommended Action |
|------------|-------|-------------|-------------------|
| **Critical** | 76-100 | Extremely high churn risk | Immediate personal outreach, special offers |
| **High** | 51-75 | Significant churn risk | Targeted win-back campaign |
| **Medium** | 26-50 | Moderate risk, needs attention | Engagement campaign, product updates |
| **Low** | 0-25 | Healthy, engaged customers | Continue standard marketing |

## Reading the Results

### Interpreting Risk Factors

When `include_risk_factors=true`, you'll see detailed breakdowns like:

```
"High inactivity (120 days), Declining activity (-40%), Declining value (-25%)"
```

This tells you:
- Customer hasn't been active in 120 days
- Activity frequency down 40% vs. historical
- Spending down 25% vs. historical

### Prioritization Strategy

1. **Critical + High Value**: Immediate intervention (personal call, custom offer)
2. **High + Medium Value**: Automated win-back campaign
3. **Medium Risk**: Engagement emails, product education
4. **Low Risk**: Standard marketing, loyalty programs

## Use Case Examples

### E-commerce
- Detect customers who stopped buying
- Send personalized discount codes to high-risk customers
- Track churn risk by product category

### SaaS/Subscription
- Identify accounts at risk of cancellation
- Trigger customer success outreach
- Proactive feature education for at-risk users

### Mobile Apps
- Detect declining engagement
- Send push notifications to re-engage
- Offer premium features to high-risk valuable users

## Best Practices

### Tuning Thresholds

- **E-commerce**: 90-120 day inactivity threshold
- **SaaS (monthly)**: 30-45 day threshold
- **SaaS (annual)**: 180-365 day threshold
- **Retail**: 60-90 day threshold

### Action Timing

- **Critical Risk**: Act within 24-48 hours
- **High Risk**: Act within 1 week
- **Medium Risk**: Act within 2 weeks

### Validation

- Track which customers actually churned
- A/B test retention campaigns on predicted high-risk customers
- Refine thresholds based on actual churn rates

## Advantages Over ML Models

1. **No Training Required**: Works immediately with historical data
2. **Interpretable**: Clear understanding of why a score was assigned
3. **Explainable**: Can show customers exactly what factors contribute to risk
4. **No Drift**: Doesn't degrade over time like trained models
5. **Simple Deployment**: No model serving infrastructure needed
6. **Fast**: Real-time scoring on millions of customers

## Limitations

- **Not Predictive**: Reactive to patterns, not truly predictive
- **Equal Weights**: Doesn't learn optimal weight distribution
- **Linear Assumptions**: Assumes linear relationships
- **No Interactions**: Doesn't capture feature interactions

For higher accuracy, consider upgrading to an ML-based churn model after validating the business value with this heuristic approach.

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **Data Freshness**: Update daily or weekly for best results
- **Minimum History**: Works best with at least 3-6 months of customer data
- **New Customers**: May show as high-risk initially (set minimum lifetime threshold)
- **Performance**: Handles millions of customers efficiently
- **Customization**: Weights and thresholds can be adjusted in component code if needed
