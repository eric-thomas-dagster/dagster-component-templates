# survival_analysis

Estimate survival functions from time-to-event data using the Kaplan-Meier estimator or the Cox proportional hazards model via the `lifelines` library. Supports grouped analysis (e.g. by cohort or plan tier) and optional time-point filtering.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `duration_column` | `str` | required | Time-to-event column |
| `event_column` | `str` | required | Binary event indicator (1=event occurred, 0=censored) |
| `group_column` | `Optional[str]` | `null` | Column for grouped Kaplan-Meier analysis |
| `method` | `str` | `"kaplan_meier"` | `"kaplan_meier"` or `"cox"` |
| `covariate_columns` | `Optional[List[str]]` | `null` | Covariate columns for Cox model |
| `time_points` | `Optional[List[float]]` | `null` | Specific time points to evaluate |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output

### Kaplan-Meier

DataFrame with columns: `timeline`, `survival_probability`, and optionally `<group_column>`.

### Cox

DataFrame containing the `lifelines` model summary: hazard ratios, confidence intervals, p-values, and concordance index.

## Example YAML

```yaml
component_type: survival_analysis
description: Estimate subscription survival by plan tier.

asset_name: subscription_survival_curves
upstream_asset_key: subscription_cohort_data
duration_column: days_active
event_column: cancelled
group_column: plan_tier
method: kaplan_meier
group_name: retention_analysis
```

## Cox Model Example

```yaml
component_type: survival_analysis
asset_name: churn_cox_hazards
upstream_asset_key: churn_data
duration_column: days_to_churn
event_column: churned
method: cox
covariate_columns:
  - age
  - plan_price
  - support_tickets
```

## Metadata Logged

Kaplan-Meier: `method`, `n_subjects`, `n_events`, `median_survival`

Cox: `method`, `n_subjects`, `n_events`, `concordance_index`

## Requirements

- `dagster`
- `pandas`
- `lifelines`
