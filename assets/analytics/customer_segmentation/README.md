# Customer Segmentation (RFM) Component

Segment customers using RFM (Recency, Frequency, Monetary) analysis to identify Champions, Loyal Customers, At-Risk customers, and other actionable segments for targeted marketing and retention strategies.

## Purpose

RFM analysis is a proven method for customer segmentation used by major retailers and online businesses. It analyzes three key dimensions:

- **Recency**: How recently did the customer make a purchase?
- **Frequency**: How often do they purchase?
- **Monetary**: How much do they spend?

By scoring customers on these three dimensions, you can identify distinct customer segments and tailor your marketing, retention, and engagement strategies accordingly.

## Key Features

- **RFM Scoring**: Automatic calculation of recency, frequency, and monetary scores
- **10 Predefined Segments**: Industry-standard segments with clear definitions
- **Actionable Recommendations**: Specific marketing actions for each segment
- **Configurable Weights**: Adjust importance of R, F, and M for your business
- **Segment Analytics**: Total and average value per segment
- **Flexible Scoring**: Quintiles, quartiles, or custom scoring methods

## Output Schema

| Field | Description |
|-------|-------------|
| customer_id | Unique customer identifier |
| recency | Days since last purchase |
| frequency | Number of purchases in analysis period |
| monetary | Total spend in analysis period |
| R_score | Recency score (1-5, higher is better) |
| F_score | Frequency score (1-5, higher is better) |
| M_score | Monetary score (1-5, higher is better) |
| RFM_score | Weighted average of R, F, M scores |
| segment | Predefined segment name |
| recommendation | Actionable recommendation for segment |
| segmented_at | Timestamp of segmentation |

## RFM Segments

### Champions (R: 5, F: 5, M: 5)

**Characteristics**:
- Bought recently
- Buy often
- Spend the most

**Size**: Typically 5-10% of customers
**Value**: 30-40% of revenue

**Recommendation**:
- Reward them with exclusive benefits
- Use as early adopters for new products
- Promote brand advocates and referrals
- VIP treatment and personalized service

**Example Actions**:
- Invite to exclusive preview events
- Ask for testimonials and case studies
- Offer referral bonuses
- Create advisory board

### Loyal Customers (R: 4-5, F: 3-5)

**Characteristics**:
- Buy regularly
- Responsive to promotions
- Good spending habits

**Size**: 10-15% of customers
**Value**: 20-25% of revenue

**Recommendation**:
- Upsell higher value products
- Ask for reviews and feedback
- Engage them with loyalty programs
- Build long-term relationships

**Example Actions**:
- Loyalty rewards program
- Product recommendations
- Special member pricing
- Early access to sales

### Potential Loyalists (R: 4-5, F: 1-2)

**Characteristics**:
- Recent customers
- Haven't purchased many times yet
- Show promise

**Size**: 15-20% of customers
**Value**: 10-15% of revenue

**Recommendation**:
- Offer membership / loyalty program
- Recommend products based on first purchase
- Nurture into loyal customers
- Provide excellent onboarding

**Example Actions**:
- Welcome series with tips
- Product education content
- Limited-time upgrade offers
- Personalized recommendations

### Promising (R: 3-4, F: 2-3, M: 3-4)

**Characteristics**:
- Moderate recency and frequency
- Decent spenders
- Room to grow

**Size**: 10-15% of customers
**Value**: 10-15% of revenue

**Recommendation**:
- Offer free shipping
- Add benefits to build long-term relationship
- Increase purchase frequency
- Boost order value

**Example Actions**:
- Free shipping thresholds
- Bundle offers
- Volume discounts
- Subscription options

### Need Attention (R: 3, F: 1-2)

**Characteristics**:
- Above average recency
- Low frequency and monetary
- Need engagement

**Size**: 10-15% of customers
**Value**: 5-10% of revenue

**Recommendation**:
- Make limited time offers
- Recommend based on past purchases
- Reactivate them with special deals
- Gather feedback on what's missing

**Example Actions**:
- "We miss you" campaigns
- Special discount codes
- Survey about preferences
- Showcase new products

### At Risk (R: 1-2, F: 4-5)

**Characteristics**:
- Were frequent buyers
- Haven't purchased recently
- Losing them!

**Size**: 5-10% of customers
**Value**: 15-20% of revenue (historically)

**Recommendation**:
- Send personalized emails
- Offer renewals and special pricing
- Provide helpful resources
- Win them back proactively

**Example Actions**:
- "We want you back" offers
- Call from account manager
- Exclusive comeback deals
- Address pain points

### About to Sleep (R: 1-2, F: 2-3)

**Characteristics**:
- Below average recency, frequency, monetary
- Losing interest
- Need reactivation

**Size**: 15-20% of customers
**Value**: 5-10% of revenue

**Recommendation**:
- Share valuable resources
- Recommend popular products
- Reconnect with personalized messages
- Create urgency

**Example Actions**:
- Educational content
- Best-seller showcases
- Flash sales
- Preference center updates

### Can't Lose Them (R: 1, F: 1, M: 5)

**Characteristics**:
- Made big purchases historically
- Haven't purchased recently
- High risk, high value

**Size**: 2-5% of customers
**Value**: 10-15% of revenue (historically)

**Recommendation**:
- Win them back via renewals
- Survey to understand issues
- Reach out proactively
- Offer significant incentives

**Example Actions**:
- Executive outreach call
- Major win-back offers
- Loss aversion messaging
- Survey about dissatisfaction

### Hibernating (R: 1, F: 1, M: 2-3)

**Characteristics**:
- Long time since last purchase
- Low frequency and monetary
- Sleeping customers

**Size**: 10-15% of customers
**Value**: 2-5% of revenue

**Recommendation**:
- Offer other relevant products
- Use special offers to revive interest
- Low-cost reactivation attempts
- Consider suppression if unresponsive

**Example Actions**:
- Deep discount offers
- New product announcements
- Reactivation series (3-email max)
- Preference confirmation

### Lost (R: 1, F: 1, M: 1)

**Characteristics**:
- Lowest recency, frequency, monetary
- Least engaged segment
- May never return

**Size**: 20-30% of customers
**Value**: <2% of revenue

**Recommendation**:
- Revive interest with reach out campaign
- If no response, suppress to save costs
- Focus resources elsewhere
- Learn from their exit

**Example Actions**:
- Final win-back attempt
- Feedback survey
- Unsubscribe option
- Move to suppression list

## Configuration

### Basic Configuration

```yaml
asset_name: customer_segments
analysis_period_days: 365
use_predefined_segments: true
include_recommendations: true
```

### Input Sources (Connected via Visual Lineage)

**Transaction Data** (Required)
- Must include: customer_id, date, amount
- Example sources: order data, payment data, subscription charges

Connect by drawing an edge in Dagster Designer UI from `transaction_data` → `customer_segments`.

### Adjusting RFM Weights

Customize weights based on your business model:

**E-commerce (Balanced)**:
```yaml
recency_weight: 1.0
frequency_weight: 1.0
monetary_weight: 1.0
```

**Subscription Business (Recency Focus)**:
```yaml
recency_weight: 2.0  # Churn risk is critical
frequency_weight: 0.5
monetary_weight: 1.0
```

**High-Ticket B2B (Monetary Focus)**:
```yaml
recency_weight: 0.5
frequency_weight: 0.5
monetary_weight: 2.0  # Deal size matters most
```

**Repeat Purchase Business (Frequency Focus)**:
```yaml
recency_weight: 1.0
frequency_weight: 2.0  # Habit formation is key
monetary_weight: 0.5
```

### Scoring Methods

**Quintiles (Default)**: Divides customers into 5 equal groups (1-5 scores)
```yaml
scoring_method: quintiles
```
- Best for: Large customer bases (10,000+)
- Most granular segmentation

**Quartiles**: Divides customers into 4 equal groups (1-4 scores)
```yaml
scoring_method: quartiles
```
- Best for: Medium customer bases (1,000-10,000)
- Simpler segmentation

## Use Cases

### 1. Targeted Marketing Campaigns

Launch segment-specific campaigns:

```python
df = context.load_asset_value("customer_segments")

# Get Champions for VIP program
champions = df[df['segment'] == 'Champions']
print(f"Champions: {len(champions)} customers")
print(f"Action: {champions.iloc[0]['recommendation']}")

# Export for email campaign
champions[['customer_id', 'monetary']].to_csv('champions_list.csv')
```

### 2. Churn Prevention

Identify and rescue at-risk customers:

```python
df = context.load_asset_value("customer_segments")

# High-value at-risk customers
churn_risk_segments = ['At Risk', 'Cant Lose Them', 'About to Sleep']
at_risk = df[df['segment'].isin(churn_risk_segments)]

# Prioritize by historical value
at_risk_sorted = at_risk.sort_values('monetary', ascending=False)

print(f"At-risk customers: {len(at_risk)}")
print(f"At-risk revenue: ${at_risk['monetary'].sum():,.0f}")
print("\nTop 10 at-risk customers:")
print(at_risk_sorted[['customer_id', 'segment', 'monetary', 'recency']].head(10))
```

### 3. Resource Allocation

Optimize customer success team allocation:

```python
df = context.load_asset_value("customer_segments")

# Calculate segment sizes and values
segment_summary = df.groupby('segment').agg({
    'customer_id': 'count',
    'monetary': ['sum', 'mean']
}).round(2)

segment_summary.columns = ['count', 'total_value', 'avg_value']
segment_summary = segment_summary.sort_values('total_value', ascending=False)

print("Segment Resource Allocation:")
print(segment_summary)

# Recommendation: Allocate CSM time proportional to segment value
segment_summary['csm_hours_per_week'] = (
    segment_summary['total_value'] / segment_summary['total_value'].sum() * 40
).round(1)

print("\nRecommended CSM Time Allocation:")
print(segment_summary['csm_hours_per_week'])
```

### 4. Loyalty Program Design

Design tiered loyalty programs based on segments:

```python
df = context.load_asset_value("customer_segments")

# Map segments to loyalty tiers
loyalty_tiers = {
    'Champions': 'Platinum',
    'Loyal Customers': 'Gold',
    'Potential Loyalists': 'Silver',
    'Promising': 'Bronze',
}

df['loyalty_tier'] = df['segment'].map(loyalty_tiers)

# Calculate tier distribution
tier_dist = df['loyalty_tier'].value_counts()
print("Loyalty Program Tiers:")
print(tier_dist)
```

### 5. Win-Back Campaign Prioritization

Prioritize win-back efforts by segment and value:

```python
df = context.load_asset_value("customer_segments")

# Focus on high-value dormant customers
winback_segments = ['At Risk', 'Cant Lose Them', 'About to Sleep', 'Hibernating']
winback_candidates = df[df['segment'].isin(winback_segments)]

# Score by historical value and recoverability
winback_candidates['winback_priority'] = (
    winback_candidates['monetary'] /  # Higher value = higher priority
    (winback_candidates['recency'] / 30 + 1)  # More recent = easier to win back
)

winback_sorted = winback_candidates.sort_values('winback_priority', ascending=False)

print("Win-Back Campaign Priority List:")
print(winback_sorted[['customer_id', 'segment', 'monetary', 'recency', 'winback_priority']].head(20))
```

## Best Practices

1. **Update Regularly**: Recalculate RFM scores weekly or monthly
2. **Act on Insights**: Segments are useless without targeted campaigns
3. **Test and Learn**: A/B test different approaches per segment
4. **Track Movement**: Monitor customers moving between segments
5. **Adjust Weights**: Tune R/F/M weights based on what predicts revenue
6. **Exclude Recent**: Consider excluding customers in first 30 days
7. **Set Minimums**: May want minimum transaction threshold to include
8. **Monitor Trends**: Track segment distribution over time

## Troubleshooting

### All Customers in One Segment

**Problem**: 80%+ of customers in single segment

**Solutions**:
- Increase analysis_period_days to get more variation
- Check scoring_method (try quintiles if using quartiles)
- Verify transaction data has full date range
- Ensure transaction amounts are correct

### Segment Distribution Doesn't Match Benchmarks

**Problem**: Too many/few Champions or Lost customers

**Solutions**:
- This is often correct! Your business may differ
- Adjust RFM weights if needed
- Verify data quality (missing transactions, wrong amounts)
- Consider business model differences

### "Can't Lose Them" Segment Empty

**Problem**: No high-value churned customers identified

**Solutions**:
- Check if you have customers with high historical spend
- Verify recency is calculating correctly
- May indicate good retention (that's good!)
- Consider adjusting segment thresholds

### RFM Scores All Similar

**Problem**: All customers have similar R/F/M scores

**Solutions**:
- Increase analysis_period_days for more variation
- Check if you have enough transaction history
- Verify customer_id is unique
- May indicate very new business

## Industry Benchmarks

### E-commerce
- Champions: 5-10%
- Loyal: 10-15%
- At Risk: 10-15%
- Lost: 20-30%

### SaaS
- Champions: 10-15%
- Loyal: 15-20%
- At Risk: 5-10%
- Lost: 15-25%

### Retail
- Champions: 3-7%
- Loyal: 8-12%
- At Risk: 12-18%
- Lost: 25-35%

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the customer segmentation asset to create |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Asset description |
| `group_name` | `str` | `"analytics"` | Asset group name |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']} |
| `deps` | `list[str]` | — | Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset']) |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `str` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `str` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `transaction_data_asset_key` | `str` | — | Transaction/order data with customer_id, date, and amount |
| `customer_data_asset_key` | `str` | — | Customer data for additional attributes (optional) |
| `recency_weight` | `float` | `1.0` | Weight for recency in RFM score |
| `frequency_weight` | `float` | `1.0` | Weight for frequency in RFM score |
| `monetary_weight` | `float` | `1.0` | Weight for monetary value in RFM score |
| `scoring_method` | `str` | `"quintiles"` | Scoring method: quintiles, quartiles, or custom |
| `analysis_period_days` | `int` | `365` | Number of days to analyze for RFM calculation |
| `use_predefined_segments` | `bool` | `true` | Use predefined RFM segments (Champions, Loyal, At Risk, etc.) |
| `include_recommendations` | `bool` | `true` | Include action recommendations for each segment |
| `calculate_segment_value` | `bool` | `true` | Calculate total and average value per segment |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output DataFrame in metadata (for builder UIs). |
| `preview_rows` | `int` | `25` | Rows in the preview when include_preview_metadata=True. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |

<!-- FIELDS:END -->

## Example Pipeline

```
┌─────────────┐
│  Order/     │
│Transaction  │
│    Data     │
└──────┬──────┘
       │
       │
       ▼
┌─────────────┐
│   Customer  │
│Segmentation │
│    (RFM)    │
└──────┬──────┘
       │
       ├──────────────────┬──────────────────┐
       │                  │                  │
┌──────▼──────┐    ┌──────▼──────┐   ┌──────▼──────┐
│  Champions  │    │   At Risk   │   │    Lost     │
│   Campaign  │    │  Win-Back   │   │ Suppression │
│             │    │   Campaign  │   │    List     │
└─────────────┘    └─────────────┘   └─────────────┘
```

## Related Components

- **Revenue Attribution**: Identify which channels drive best segments
- **Customer Health Score**: Complement RFM with engagement metrics
- **Lead Scoring**: Score prospects before they become customers
- **Churn Prediction**: ML-based churn prediction (Phase 4+)

## Learn More

- [RFM Analysis Guide](https://www.optimove.com/resources/learning-center/rfm-analysis)
- [Customer Segmentation](https://www.putler.com/rfm-analysis/)
- [Segment-Based Marketing](https://www.klaviyo.com/marketing-resources/rfm-segmentation)

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
