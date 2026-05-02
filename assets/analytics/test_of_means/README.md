# TestOfMeans

Two-sample t-test (independent or paired) using scipy.stats. Returns a single-row DataFrame with mean of each group, t-statistic, p-value, degrees of freedom, and significance flag. Useful as a stat-test gate downstream of an experiment exposure asset.

## Example

```yaml
type: dagster_component_templates.TestOfMeansComponent
attributes:
  asset_name: signup_lift_test
  upstream_asset_key: signups_by_variant
  value_column: signups
  group_column: variant
  group_a: control
  group_b: treatment
  test_type: independent
  alpha: 0.05
  group_name: stats
```


## Requirements

```
pandas
scipy
```
