# oversample_field

Balance imbalanced datasets by oversampling the minority class using SMOTE, random oversampling, or ADASYN via the `imbalanced-learn` library. Use this component before model training when your target class distribution is skewed.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `target_column` | `str` | required | Binary or categorical target column to balance |
| `method` | `str` | `"smote"` | `"smote"`, `"random"`, or `"adasyn"` |
| `sampling_strategy` | `str \| float` | `"auto"` | `"auto"`, `"minority"`, `"majority"`, or a float ratio |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `feature_columns` | `Optional[List[str]]` | `null` | Feature columns (null = all except target) |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Method Guide

| Method | Description |
|---|---|
| `smote` | Synthetic Minority Over-sampling Technique — generates synthetic samples along feature-space line segments between existing minority samples |
| `random` | Randomly duplicates existing minority samples |
| `adasyn` | Adaptive Synthetic Sampling — focuses synthetic generation on harder-to-classify minority samples |

## Sampling Strategy

| Value | Behavior |
|---|---|
| `"auto"` | Resample all classes except the majority to match majority count |
| `"minority"` | Resample only the minority class |
| `"majority"` | Resample all classes except the majority |
| `0.5` (float) | Ratio of minority to majority after resampling |

## Example YAML

```yaml
component_type: oversample_field
description: Balance churn dataset using SMOTE.

asset_name: churn_balanced_features
upstream_asset_key: customer_features_enriched
target_column: churned
method: smote
sampling_strategy: auto
group_name: data_preparation
```

## Metadata Logged

`method`, `original_rows`, `resampled_rows`, `rows_added`, `original_class_distribution`, `resampled_class_distribution`

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
- `imbalanced-learn`
