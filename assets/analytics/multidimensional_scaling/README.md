# MultidimensionalScaling

Applies Multidimensional Scaling (sklearn.manifold.MDS) to compress a high-dimensional feature space into 2 or 3 components, preserving pairwise distances as much as possible. Output keeps the original rows with new MDS columns appended.

## Example

```yaml
type: dagster_component_templates.MultidimensionalScalingComponent
attributes:
  asset_name: customers_mds_2d
  upstream_asset_key: customer_features
  feature_columns: [recency, frequency, monetary, lifetime_days, support_tickets]
  n_components: 2
  group_name: dim_reduction
```


## Requirements

```
pandas
scikit-learn
numpy
```
