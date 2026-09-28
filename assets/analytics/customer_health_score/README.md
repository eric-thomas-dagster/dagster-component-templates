# Customer Health Score Component

Predict customer churn risk and identify expansion opportunities. `scoring_method='heuristic'` (default): the 4-way join described below (engagement + product usage + subscription + support). `scoring_method='ml'`: fits a real scikit-learn classifier against a `target_column` you supply.

## Ingestion (scoring_method='heuristic')

Each of the four named inputs is **independently optional** (at least one must be connected), and each independently supports two ways to get its rows in:

- **`<name>_asset_key`**: the usual Dagster way -- point at any asset producing a DataFrame with that input's columns.
- **`<name>_source: {kind: warehouse_query, resource_key: ..., sql: ...}`**: pull rows directly via SQL, no upstream asset required. Works out of the box with `duckdb_resource` and any resource exposing `.get_engine()`/`.get_connection()`, or a bare SQLAlchemy connection string via `database_url_env_var` when no Dagster resource is registered.

So you get 8 fields total: `customer_data_asset_key`/`customer_data_source`, `subscription_data_asset_key`/`subscription_data_source`, `product_usage_asset_key`/`product_usage_source`, `support_ticket_asset_key`/`support_ticket_source` -- set at most one of each pair.

## `scoring_method: ml` -- a real trained classifier, not a heuristic

**`is_churn_risk`/`is_expansion_opportunity` below are threshold-derived from the same weighted heuristic, not fit against any observed outcome** -- there is no "churned"/"expanded" boolean anywhere in the input schema. So `scoring_method='ml'` is opt-in, requires you bring your own label via `target_column` + `feature_columns`, and works over a single, already-joined `upstream_asset_key`/`source` table -- **it cannot be combined with the 4-way join fields above** (if you need the customer+subscription+usage+support join, do it upstream of this component first, then point `scoring_method='ml'` at the joined result).

```yaml
type: dagster_component_templates.CustomerHealthScoreComponent
attributes:
  asset_name: customer_churn_ml
  scoring_method: ml
  upstream_asset_key: customers_joined_with_churn_label
  target_column: churned
  feature_columns: [login_frequency, feature_adoption_rate, mrr, ticket_count]
  test_size: 0.2
  output_probabilities: true
```

`execution_mode: sql` is also available under `scoring_method: ml` (BigQuery/Snowflake genuine train+predict, Databricks predict-only against an already-served endpoint) -- reuses the exact same audited mapping as `logistic_regression_model`.

## Purpose

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
customer_data_asset_key: customer_profiles
subscription_data_asset_key: subscriptions
churn_risk_threshold: 40
expansion_opportunity_threshold: 75
```

### Input Sources

The component accepts 1-4 input data sources, each independently connected via `<name>_asset_key` (visual lineage / point at any asset) or `<name>_source` (direct SQL, see Ingestion above):

1. **Customer Data** (CRM, user profiles)
   - Fields: customer_id, last_login_days, login_frequency, feature_adoption_rate

2. **Subscription Data** (billing, subscriptions)
   - Fields: customer_id, status, payment_failures, days_subscribed, mrr

3. **Product Usage Data** (activity, events)
   - Fields: customer_id, daily_active_days, feature_usage_count, session_count

4. **Support Ticket Data** (support interactions)
   - Fields: customer_id, ticket_count, critical_issues, csat_score

Connect by drawing edges in Dagster Designer UI from data sources → `customer_health_scores` (asset_key inputs), or point `<name>_source` at a warehouse query directly.

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

**Naming collision to be aware of**: `engagement_weight`/`product_usage_weight`/`payment_health_weight`/`support_health_weight` (documented here) control how the four **component scores** blend into the overall `health_score`. Each component score itself is built from a SEPARATE, hardcoded, non-configurable set of per-indicator weights inside `_calculate_engagement_score`/`_calculate_product_usage_score`/`_calculate_payment_health_score`/`_calculate_support_health_score` (e.g. `last_login_days` contributes 0.4 to the engagement score, `login_frequency` contributes 0.3 -- these numbers are NOT the same thing as `engagement_weight` and aren't exposed as fields). Confirmed by reading the code -- easy to confuse the two levels.

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

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the customer health score asset to create |

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
| `partition_date_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `output_table` | `str` | — | Required when execution_mode='sql'. Destination table the predictions are written to. |
| `model_name` | `str` | — | Required when execution_mode='sql'. For snowflake/bigquery: the identifier this component creates the model under. For databricks: the name of an already-served Model Serving endpoint -- this dialect trains nothing. |
| `target_column` | `Union[str, int]` | — | Required when scoring_method='ml'. Column name of the historical outcome label (e.g. 'churned' or 'expanded') -- this does NOT exist in the heuristic's input schema; you must supply it. |
| `model_path` | `str` | — | scoring_method='ml' only. If set, joblib-dump the trained model to this path after fit. Supports local paths and any fsspec URL (s3://, gs://, abfs://). |
| `output_probabilities` | `bool` | `true` | scoring_method='ml' only. Add predicted_proba_<class> columns per class |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `customer_data_asset_key` | `str` | — | Customer data asset (CRM, user profiles, etc.). Mutually exclusive with `customer_data_source` -- set at most one. |
| `customer_data_source` | `Dict[str, Any]` | — | Pull customer data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `customer_data_asset_ke… _(full docs in schema.json + component README)_ |
| `subscription_data_asset_key` | `str` | — | Subscription/billing data asset. Mutually exclusive with `subscription_data_source` -- set at most one. |
| `subscription_data_source` | `Dict[str, Any]` | — | Pull subscription/billing data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `subscripti… _(full docs in schema.json + component README)_ |
| `product_usage_asset_key` | `str` | — | Product usage/activity data asset. Mutually exclusive with `product_usage_source` -- set at most one. |
| `product_usage_source` | `Dict[str, Any]` | — | Pull product usage/activity data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `product_… _(full docs in schema.json + component README)_ |
| `support_ticket_asset_key` | `str` | — | Support ticket data asset. Mutually exclusive with `support_ticket_source` -- set at most one. |
| `support_ticket_source` | `Dict[str, Any]` | — | Pull support ticket data directly via SQL instead of from an asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `support_ticket_a… _(full docs in schema.json + component README)_ |
| `scoring_method` | `str` | `"heuristic"` | 'heuristic' (default): the 4-way join above and hand-tuned weighted scoring below. 'ml': fits a real scikit-learn classifier against a `target_column` you supply over a single, already-joined `upstream_asset_key`/`source… _(full docs in schema.json + component README)_ |
| `upstream_asset_key` | `str` | — | scoring_method='ml' only. Upstream asset key providing an already-joined DataFrame with target_column + feature_columns. Mutually exclusive with `source` -- set exactly one. |
| `source` | `Dict[str, Any]` | — | scoring_method='ml' only. Pull rows directly via SQL instead of from an upstream asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Also required (with e… _(full docs in schema.json + component README)_ |
| `execution_mode` | `str` | `"python"` | Only meaningful when scoring_method='ml'. 'python' (default): fits a real scikit-learn LogisticRegression locally. 'sql': trains AND predicts server-side via BigQuery/Snowflake ML (Databricks is predict-only). Requires `… _(full docs in schema.json + component README)_ |
| `sql_dialect` | `str` | — | `f"Required when scoring_method='ml' and execution_mode='sql'. One of: {_SQL_MODEL_DIALECTS}."` |
| `feature_columns` | `List[Union[str, int]]` | — | Required when scoring_method='ml'. List of column names to use as classifier features. |
| `test_size` | `float` | `0.2` | scoring_method='ml' only. Fraction of data to hold out for evaluation |
| `random_state` | `int` | `42` | scoring_method='ml' only. Random seed for reproducibility |
| `max_iter` | `int` | `1000` | scoring_method='ml' only. Maximum number of solver iterations |
| `normalize` | `bool` | `true` | scoring_method='ml' only. Standardize features with StandardScaler before fitting |
| `analysis_period_days` | `int` | `30` | scoring_method='heuristic' only. Number of days to analyze for health calculation. NOTE: accepted but not currently used by the scoring math (pre-existing, documented not fixed -- see README). |
| `engagement_weight` | `float` | `0.25` | Weight for engagement metrics (0-1) |
| `product_usage_weight` | `float` | `0.25` | Weight for product usage metrics (0-1) |
| `payment_health_weight` | `float` | `0.25` | Weight for payment/subscription health (0-1) |
| `support_health_weight` | `float` | `0.25` | Weight for support interaction health (0-1) |
| `min_health_score` | `float` | `0.0` | Minimum health score (0-100) |
| `max_health_score` | `float` | `100.0` | Maximum health score (0-100) |
| `churn_risk_threshold` | `float` | `40.0` | Health score below this is considered churn risk |
| `expansion_opportunity_threshold` | `float` | `75.0` | Health score above this is considered expansion opportunity |
| `include_factor_breakdown` | `bool` | `true` | Include breakdown of contributing factors in output |
| `calculate_trend` | `bool` | `true` | scoring_method='heuristic' only. Calculate health score trend (requires historical data). NOTE: accepted but not currently implemented anywhere -- no trend-calculation code exists (pre-existing, documented not fixed -- see README). |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output DataFrame in metadata (for builder UIs). |
| `preview_rows` | `int` | `25` | Rows in the preview when include_preview_metadata=True. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |

[//]: # (FIELDS:END)

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

## Requirements

- `dagster`
- `pandas`
- `numpy`
- `scikit-learn` (only for `scoring_method: ml`)
- `sqlalchemy` (only for `*_source: {kind: warehouse_query}` ingestion)

## Validation

`validation.level: code` for the `*_source`/`scoring_method`/`execution_mode` additions.

**Live-verified (nothing mocked)**: `scoring_method: heuristic` (default) with customer_data + subscription_data connected via asset keys (multi-row, confirming both the fix below and that the healthiest synthetic customer scores higher than the least healthy), `*_source: {kind: warehouse_query}` against a real DuckDB database for one input while another uses an asset key, `scoring_method: ml` against a labeled synthetic dataset (real scikit-learn `LogisticRegression`, real `predict_proba`-based probability columns).

**Structural only, not executed (no live warehouse credentials in this environment)**: `execution_mode: sql` -- reuses the exact same generated-SQL patterns already live-verified-as-structurally-correct for `logistic_regression_model`.

**Found and fixed a real, pre-existing bug while adding this — the heuristic path crashed on every real multi-row input before this change**: `_normalize_score` used a scalar `if pd.isna(value): ...` guard, but all 5 call sites (in `_calculate_engagement_score`, `_calculate_product_usage_score`, `_calculate_payment_health_score`) pass a full pandas Series -- `pd.isna()` on a multi-element Series returns a Series of booleans, and Python's `if <Series>:` raises `ValueError: The truth value of a Series is ambiguous` for any real input with more than one row. This is the exact same bug, from the exact same original template, as `lead_scoring`'s `_normalize_score` -- both were broken independently, confirmed by re-reading both files. Rewritten to be genuinely vectorized (`.fillna(50.0).clip(...)` on the whole Series). This component had no committed tests at all before this change.

**Also added, matching the newer sibling components in this repo** (this component previously had none of this): `dagster/row_count` and `dagster/column_schema` metadata, `build_column_schema_change_checks` asset checks, and honoring the previously-dead `include_preview_metadata`/`preview_rows` fields (declared but never actually used in `build_defs` before this change).

**Documented, not fixed** (would change output numbers for existing users): `analysis_period_days` and `calculate_trend` are both accepted fields with no effect -- neither is referenced anywhere in the scoring math, and no trend-calculation code exists anywhere in the file despite `calculate_trend`'s description implying otherwise.
