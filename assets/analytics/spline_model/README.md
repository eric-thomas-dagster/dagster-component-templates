# SplineModel

Fits ordinary least squares using sklearn's SplineTransformer to allow smooth non-linear feature effects (alternative to a hand-engineered polynomial). Emits per-row predictions.

## Example

```yaml
type: dagster_component_templates.SplineModelComponent
attributes:
  asset_name: temperature_predictions
  upstream_asset_key: weather_features
  target_column: load_kwh
  feature_columns: [temperature_c, humidity, hour_of_day]
  n_knots: 5
  degree: 3
  group_name: model
```


## Requirements

```
pandas
scikit-learn
numpy
```
