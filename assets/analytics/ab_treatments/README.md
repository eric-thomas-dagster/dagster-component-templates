# ABTreatments

Takes a DataFrame of users and assigns each to a variant using a stable hash of (user_id, experiment_id). The split is deterministic — re-running on the same input gives the same assignments — so you can rebuild the asset without reshuffling users mid-experiment.

## Example

```yaml
type: dagster_component_templates.ABTreatmentsComponent
attributes:
  asset_name: checkout_button_assignments
  upstream_asset_key: eligible_users
  user_id_column: user_id
  experiment_id: checkout_button_v2
  variants: [control, treatment]
  weights: [0.5, 0.5]
  output_column: variant
  group_name: experiments
```


## Requirements

```
pandas
```
