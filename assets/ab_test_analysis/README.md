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
