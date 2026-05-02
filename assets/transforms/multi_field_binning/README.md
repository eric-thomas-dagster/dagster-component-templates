# MultiFieldBinning

Apply the same binning logic to many columns in one shot. Each column gets a sibling `_bin` column (e.g. `age_bin`, `income_bin`) holding the bin label. Quantile bins use n equal-frequency tiles; width bins use equal-range cuts.

## Example

```yaml
type: dagster_component_templates.MultiFieldBinningComponent
attributes:
  asset_name: customers_binned
  upstream_asset_key: customer_features
  columns: [age, income, lifetime_days]
  n_bins: 4
  method: quantile
  labels: [low, mid_low, mid_high, high]
  group_name: prep
```


## Requirements

```
pandas
```
