# A/B Test Analysis Component

Perform rigorous statistical analysis of A/B tests with significance testing, confidence intervals, and data-driven recommendations.

## Overview

Statistical A/B test analysis including:
- **Significance Testing**: Z-test for conversion rate differences
- **Confidence Intervals**: 95% CI for treatment effects
- **Sample Size Validation**: Ensure statistical power
- **Lift Calculation**: Percentage improvement over control
- **Recommendations**: Clear guidance on test outcomes

## Use Cases

- **Feature Releases**: Validate new features before full rollout
- **UI/UX Changes**: Test design variations
- **Pricing Experiments**: Optimize pricing strategies
- **Marketing Copy**: Test headlines, CTAs, messaging
- **Product Optimization**: Data-driven product decisions
- **Risk Mitigation**: Avoid shipping harmful changes

## Input Requirements

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `experiment_id` | string | ✓ | test_id, experimentId | Experiment identifier |
| `variant` | string | ✓ | group, variation, test_group | Variant name (control/treatment) |
| `user_id` | string | ✓ | customer_id, userId | User identifier |
| `converted` | boolean | ✓ | conversion, success | Whether user converted |
| `timestamp` | datetime | | date, enrolled_at | Enrollment time (for runtime validation) |

**Compatible Upstream Components:**
- `event_data_standardizer`
- Any component outputting experiment assignment and conversion data

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `experiment_id` | string | Experiment identifier |
| `control_variant` | string | Control group name |
| `treatment_variant` | string | Treatment group name |
| `control_n` | number | Control sample size |
| `treatment_n` | number | Treatment sample size |
| `control_rate` | number | Control conversion rate (%) |
| `treatment_rate` | number | Treatment conversion rate (%) |
| `lift_pct` | number | Percentage lift over control |
| `ci_lower` | number | Lower confidence interval (%) |
| `ci_upper` | number | Upper confidence interval (%) |
| `z_score` | number | Z-score for significance |
| `p_value` | number | P-value (< 0.05 = significant) |
| `is_significant` | boolean | Statistical significance flag |
| `sample_size_valid` | boolean | Meets minimum sample size |
| `runtime_valid` | boolean | Meets minimum runtime |
| `recommendation` | string | Actionable recommendation |
| `winner` | string | Winning variant (if applicable) |

## Configuration

```yaml
type: dagster_component_templates.ABTestAnalysisComponent
attributes:
  asset_name: ab_test_results
  confidence_level: 0.95
  minimum_sample_size: 1000
  minimum_runtime_hours: 168  # 7 days
  minimum_detectable_effect: 0.05  # 5%
  description: A/B test statistical analysis
```

## Interpreting Results

### Statistical Significance
- `p_value < 0.05`: Result is statistically significant (95% confidence)
- `is_significant = true`: Difference is unlikely due to chance

### Recommendations
- **"Implement treatment"**: Treatment significantly better, ship it!
- **"Keep control"**: Treatment significantly worse, don't ship
- **"Continue"**: Not enough data yet, keep running test
- **"No practical difference"**: Significant but below MDE, not worth implementing

### Example Output
```
| experiment | control_rate | treatment_rate | lift_pct | p_value | recommendation |
|-----------|-------------|----------------|----------|---------|----------------|
| checkout_v2 | 3.2% | 4.1% | 28.1% | 0.003 | Implement treatment |
| new_homepage | 12.5% | 12.3% | -1.6% | 0.612 | No significant difference |
```

## Best Practices

**Sample Size:**
- **Minimum per variant**: 1,000+ users
- **Ideal**: 10,000+ users for small effects
- Use power calculators to determine required N

**Runtime:**
- **Minimum**: 1-2 weeks to capture weekly patterns
- **Ideal**: 2-4 weeks for stable results
- Avoid stopping tests early (peeking problem)

**Significance Threshold:**
- **Standard**: p < 0.05 (95% confidence)
- **Higher stakes**: p < 0.01 (99% confidence)
- **Exploratory**: p < 0.10 (90% confidence)

**Minimum Detectable Effect (MDE):**
- **5-10%**: Typical for product changes
- **20%+**: Large swings, new features
- Don't ship insignificant improvements (maintenance cost)

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

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

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset to create |
| `upstream_asset_key` | `str` | Upstream asset key providing a DataFrame with experiment data |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Asset description |
| `group_name` | `str` | `"experimentation"` | Asset group for organization |
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
| `confidence_level` | `float` | `0.95` | Confidence level for significance testing (default: 0.95 for 95%) |
| `minimum_sample_size` | `int` | `1000` | Minimum sample size per variant to consider test valid |
| `minimum_runtime_hours` | `int` | — | Minimum test runtime required (hours, optional) |
| `minimum_detectable_effect` | `float` | `0.05` | Minimum detectable effect (MDE) as decimal (e.g., 0.05 = 5%) |
| `experiment_id_field` | `str` | — | Experiment ID column (auto-detected) |
| `variant_field` | `str` | — | Variant column (control/treatment, A/B, etc.) |
| `user_id_field` | `str` | — | User ID column (auto-detected) |
| `converted_field` | `str` | — | Conversion indicator column (auto-detected) |
| `timestamp_field` | `str` | — | Timestamp column for runtime validation (optional) |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `include_preview_metadata` | `bool` | `true` | Include sample data preview in metadata |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when `include_preview_metadata` is True. For long DataFrames (>10x preview_rows), a random sample is used so the preview reflects the data distribution; otherwise head() is used. |

<!-- FIELDS:END -->
