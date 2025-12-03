# Propensity Scoring Component

Calculate customer propensity scores for various actions using heuristic scoring. Identify high-propensity customers for targeted campaigns and interventions.

## Overview

Propensity scoring predicts likelihood of customer actions:
- **Purchase Propensity**: Next purchase likelihood
- **Upgrade Propensity**: Plan/tier upgrade likelihood
- **Referral Propensity**: Likelihood to refer others
- **Engagement Propensity**: Content engagement likelihood

Scores range 0-100, classified as High/Medium/Low propensity.

## Use Cases

- **Targeted Marketing**: Focus campaigns on high-propensity customers
- **Sales Prioritization**: Prioritize outreach by upgrade propensity
- **Referral Programs**: Target high-referral-propensity customers
- **Retention**: Identify low-engagement-propensity for re-engagement
- **Resource Optimization**: Allocate resources to highest-value opportunities
- **Conversion Optimization**: Target customers most likely to convert

## Input Requirements

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `customer_id` | string | ✓ | user_id, customerId, userId | Customer identifier |
| `last_activity_date` | datetime | ✓ | last_activity, last_seen | Most recent activity |
| `activity_count` | number | | total_activities, event_count | Activity frequency |
| `engagement_score` | number | | engagement, activity_score | Engagement metric |

**Compatible Upstream Components:**
- `customer_360`
- `rfm_segmentation`
- Any component with customer behavior metrics

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | string | Customer identifier |
| `propensity_score` | number | Propensity score (0-100) |
| `propensity_level` | string | High, Medium, or Low |
| `days_since_activity` | number | Days since last activity |

## Configuration

```yaml
type: dagster_component_templates.PropensityScoringComponent
attributes:
  asset_name: purchase_propensity
  propensity_type: purchase
  scoring_window_days: 90
  score_threshold_high: 70.0
  score_threshold_medium: 40.0
  description: Customer purchase propensity scores
```

## Propensity Types

### Purchase Propensity
Factors: Recency (40%), Frequency (40%), Engagement (20%)
**Use for:** Upsell campaigns, promotional targeting

### Upgrade Propensity
Factors: Engagement (70%), Recency (30%)
**Use for:** Upgrade offers, feature promotions

### Referral Propensity
Factors: Engagement (60%), Tenure (40%)
**Use for:** Referral program invitations

### Engagement Propensity
Factors: Recency (50%), Frequency (50%)
**Use for:** Re-engagement campaigns, content recommendations

## Best Practices

**Threshold Tuning:**
- High: Top 20-30% of customers (score ≥70)
- Medium: Middle 30-40% (score 40-69)
- Low: Bottom 30-50% (score <40)

**Campaign Targeting:**
- High propensity: Premium offers, personalized outreach
- Medium propensity: Standard campaigns, incentives
- Low propensity: Light touch or exclude

**Update Frequency:**
- Real-time scoring for high-touch businesses
- Daily updates for e-commerce
- Weekly updates for B2B

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`
