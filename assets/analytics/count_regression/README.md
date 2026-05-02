# CountRegression

Fits a Poisson generalized linear model for count outcomes (events per unit, click counts, etc.) using statsmodels. Emits per-row predicted counts. Use when the target is a non-negative integer count.

## Example

```yaml
type: dagster_component_templates.CountRegressionComponent
attributes:
  asset_name: support_ticket_counts
  upstream_asset_key: customer_features
  target_column: tickets_per_month
  feature_columns: [tenure_months, plan_tier_index, num_users]
  output_mode: predictions
  group_name: model
```


## Requirements

```
pandas
statsmodels
numpy
```
