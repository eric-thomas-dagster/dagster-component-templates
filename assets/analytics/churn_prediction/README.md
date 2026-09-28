# Churn Prediction Component

Predict customer churn risk. `scoring_method='heuristic'` (default): weighted activity-decline scoring, unchanged from before. `scoring_method='ml'`: fits a real scikit-learn classifier against a `target_column` you supply.

## Ingestion

Two ways to get the rows in, set exactly one, for either `scoring_method`:

- **`upstream_asset_key`**: the usual Dagster way -- point at any asset producing a DataFrame with the input columns below.
- **`source: {kind: warehouse_query, resource_key: ..., sql: ...}`**: pull rows directly via SQL, no upstream asset required. Works out of the box with `duckdb_resource` and any resource exposing `.get_engine()`/`.get_connection()`, or a bare SQLAlchemy connection string via `database_url_env_var` when no Dagster resource is registered.

## `scoring_method: ml` -- a real trained classifier, not a heuristic

**The heuristic below has no access to any historical "did this customer actually churn" label** -- there is no such column anywhere in its input schema (`customer_id`, `last_activity_date`, `total_orders`, `total_revenue`, `lifetime_days` -- all point-in-time activity/spend aggregates, never an observed outcome). So unlike every other SQL-execution-mode component built earlier in this repo's history (those were already real classifiers; this one wasn't), `scoring_method='ml'` is opt-in and requires you bring your own label (e.g. a `churned` boolean observed some fixed window later, built by a separate historical-cohort labeling job upstream) via `target_column`, plus `feature_columns` naming which columns to train on.

```yaml
type: dagster_component_templates.ChurnPredictionComponent
attributes:
  asset_name: customer_churn_ml
  upstream_asset_key: customer_features_with_churn_label
  scoring_method: ml
  target_column: churned
  feature_columns: [total_orders, total_revenue, lifetime_days, days_inactive]
  test_size: 0.2
  output_probabilities: true
```

`execution_mode: sql` is also available under `scoring_method: ml` (BigQuery/Snowflake genuine train+predict, Databricks predict-only against an already-served endpoint) -- reuses the exact same audited mapping as `logistic_regression_model`, since churn becomes an ordinary binary classification task once you have a real label.

## Purpose

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
| `activity_trend` | string | "Active", "Declining", "At Risk", or "Inactive" (an earlier version of this README claimed "Increasing"/"Stable"/"Declining" -- confirmed against the actual code, `determine_trend`, that those values are never produced) |
| `churn_risk_score` | number | Risk score 0-100 (higher = more likely to churn) |
| `churn_risk_level` | string | "Critical", "High", "Medium", "Low" |
| `recommended_action` | string | Suggested retention action |
| `risk_factors` | string | Detailed risk factor breakdown (optional) |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for the output asset (e.g., `customer_churn_risk`)

### Optional Parameters

- **`upstream_asset_key`** / **`source`** (mutually exclusive, set exactly one): where the input DataFrame comes from -- see Ingestion above. (An earlier version of this README called this `source_asset`, a field name that never existed on this component.)
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
- **`include_preview_metadata`** (boolean): Include data preview (default: true)

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset to create |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Asset description |
| `group_name` | `str` | `"customer_analytics"` | Asset group for organization |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']} |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |

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
| `target_column` | `Union[str, int]` | — | Required when scoring_method='ml'. Column name of the historical churn label (e.g. a boolean 'churned' column) -- this does NOT exist in the heuristic's input schema; you must supply it. |
| `model_path` | `str` | — | scoring_method='ml' only. If set, joblib-dump the trained model to this path after fit. Supports local paths and any fsspec URL (s3://, gs://, abfs://). |
| `output_probabilities` | `bool` | `true` | scoring_method='ml' only. Add predicted_proba_<class> columns per class |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | Upstream asset key providing a DataFrame with customer activity data. Mutually exclusive with `source` -- set exactly one. |
| `source` | `Dict[str, Any]` | — | Pull rows directly via SQL instead of from an upstream asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Also required (with execution_mode='sql') to na… _(full docs in schema.json + component README)_ |
| `scoring_method` | `str` | `"heuristic"` | 'heuristic' (default): the original weighted activity-decline scoring below, unchanged. 'ml': fits a real scikit-learn classifier against a `target_column` you supply. The heuristic has no access to any historical 'did t… _(full docs in schema.json + component README)_ |
| `execution_mode` | `str` | `"python"` | Only meaningful when scoring_method='ml'. 'python' (default): fits a real scikit-learn LogisticRegression locally. 'sql': trains AND predicts server-side via BigQuery/Snowflake ML (Databricks is predict-only). Requires `… _(full docs in schema.json + component README)_ |
| `sql_dialect` | `str` | — | `f"Required when scoring_method='ml' and execution_mode='sql'. One of: {_SQL_MODEL_DIALECTS}."` |
| `feature_columns` | `List[Union[str, int]]` | — | Required when scoring_method='ml'. List of column names to use as classifier features. |
| `test_size` | `float` | `0.2` | scoring_method='ml' only. Fraction of data to hold out for evaluation |
| `random_state` | `int` | `42` | scoring_method='ml' only. Random seed for reproducibility |
| `max_iter` | `int` | `1000` | scoring_method='ml' only. Maximum number of solver iterations |
| `normalize` | `bool` | `true` | scoring_method='ml' only. Standardize features with StandardScaler before fitting |
| `inactivity_threshold_days` | `int` | `90` | scoring_method='heuristic' only. Days of inactivity to consider high risk |
| `lookback_days` | `int` | `365` | scoring_method='heuristic' only. Days to look back for historical comparison. NOTE: accepted but not currently used by the heuristic scoring math (pre-existing, documented not fixed -- see README). |
| `include_risk_factors` | `bool` | `true` | scoring_method='heuristic' only. Include detailed risk factors in output |
| `customer_id_field` | `str` | — | Customer ID column name (auto-detected if not specified) |
| `last_activity_field` | `str` | — | Last activity date column (auto-detected if not specified) |
| `total_orders_field` | `str` | — | Total orders column (auto-detected if not specified) |
| `total_revenue_field` | `str` | — | Total revenue column (auto-detected if not specified) |
| `lifetime_days_field` | `str` | — | Customer lifetime days column (auto-detected if not specified) |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `include_preview_metadata` | `bool` | `true` | Include sample data preview in metadata |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when `include_preview_metadata` is True. For long DataFrames (>10x preview_rows), a random sample is used so the preview reflects the data distribution; otherwise head() is used. |

[//]: # (FIELDS:END)

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
   - **Confirmed against the actual code: this is not independently computed.** `frequency_decline_score` is literally set equal to `activity_decline_score` (`churn_df['frequency_decline_score'] = churn_df['activity_decline_score']`) -- there are really only 3 independent factors, not 4, despite the weight breakdown implying otherwise.

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

## Heuristic vs. `scoring_method: ml`

**`scoring_method: heuristic` (default) advantages:**

1. **No Training Required**: Works immediately with historical data
2. **Interpretable**: Clear understanding of why a score was assigned
3. **Explainable**: Can show customers exactly what factors contribute to risk
4. **No Drift**: Doesn't degrade over time like trained models
5. **Simple Deployment**: No model serving infrastructure needed
6. **Fast**: Real-time scoring on millions of customers

**Heuristic limitations:**

- **Not Predictive**: Reactive to patterns, not truly predictive
- **Equal Weights**: Doesn't learn optimal weight distribution (and, per the note above, only 3 of the 4 named factors are actually independent)
- **Linear Assumptions**: Assumes linear relationships
- **No Interactions**: Doesn't capture feature interactions

If you have (or can build) a real historical churn label, `scoring_method: ml` (see the section near the top of this README) trains an actual scikit-learn classifier instead -- this repo previously said "consider upgrading to an ML-based churn model after validating the business value" without that option existing on this component; it now does.

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`
- `scikit-learn` (only for `scoring_method: ml`)
- `sqlalchemy` (only for `source: {kind: warehouse_query}` ingestion)

## Notes

- **Data Freshness**: Update daily or weekly for best results
- **Minimum History**: Works best with at least 3-6 months of customer data
- **New Customers**: May show as high-risk initially (set minimum lifetime threshold)
- **Performance**: Handles millions of customers efficiently
- **Customization**: Weights and thresholds can be adjusted in component code if needed

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

## Validation

`validation.level: code` for the `source`/`scoring_method`/`execution_mode` additions.

**Live-verified (nothing mocked)**: `scoring_method: heuristic` (default) against synthetic customer data -- confirmed byte-for-byte unchanged behavior from before this change. `scoring_method: ml` against a labeled synthetic dataset (real scikit-learn `LogisticRegression`, real `predict_proba`-based probability columns, real accuracy/row-count/column-schema metadata). `source: {kind: warehouse_query}` against a real DuckDB database.

**Structural only, not executed (no live warehouse credentials in this environment)**: `execution_mode: sql` -- reuses the exact same generated-SQL patterns already live-verified-as-structurally-correct for `logistic_regression_model` (BigQuery `CREATE MODEL...OPTIONS(model_type='LOGISTIC_REG')`, Snowflake `SNOWFLAKE.ML.CLASSIFICATION`, Databricks `ai_query()` predict-only).

**Also found and documented (not silently fixed, since it would change output numbers for existing users) two pre-existing heuristic-scoring bugs while adding this**: `lookback_days` is accepted but never referenced anywhere in the actual scoring math (dead config), and `frequency_decline_score` is not independently computed -- it's set equal to `activity_decline_score` verbatim, so the documented "4 independently-weighted factors" is really 3. Also fixed a false README claim that `activity_trend` produces "Increasing"/"Stable"/"Declining" -- the real code (`determine_trend`) only ever produces "Active"/"Declining"/"At Risk"/"Inactive".
