# Stepwise

Performs sequential forward selection using sklearn's SequentialFeatureSelector. Returns the chosen subset of features along with each candidate's relative score, so a downstream model can fit on a smaller feature set.

## Example

```yaml
type: dagster_component_templates.StepwiseComponent
attributes:
  asset_name: house_features_chosen
  upstream_asset_key: housing_features
  target_column: price
  feature_columns: [sqft, bedrooms, bathrooms, lot_size, year_built, garage, has_pool]
  n_features_to_select: 4
  task_type: regression
  direction: forward
  cv: 5
  group_name: feature_selection
```


## Requirements

```
pandas
scikit-learn
numpy
```
