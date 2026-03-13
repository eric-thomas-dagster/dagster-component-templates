# CreateSamplesComponent

Split a DataFrame into train, test, and optionally validation sets using
scikit-learn's `train_test_split`. Adds a `split` column (configurable) to
the original DataFrame rather than producing separate DataFrames.

## Use Cases

- Prepare labeled datasets for machine learning model training
- Stratified splitting to preserve class balance across splits
- Three-way splits for hyperparameter tuning with a held-out test set
- Reproducible dataset splitting with a fixed random seed

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `test_size` | `float` | No | `0.2` | Fraction of data for the test set |
| `validation_size` | `float` | No | `0.0` | Fraction of total data for validation (0 = no validation) |
| `random_state` | `int` | No | `42` | Random seed for reproducibility |
| `stratify_column` | `str` | No | `None` | Column for stratified splitting |
| `output_split_column` | `str` | No | `"split"` | Name of the added split label column |
| `group_name` | `str` | No | `None` | Dagster asset group name |

## Split Logic

- Rows are tagged in-place with `"train"`, `"test"`, or `"validation"`.
- `validation_size` is expressed as a fraction of the **total** dataset.
- The validation split is taken from the training portion after the test split.

## Example YAML

```yaml
type: dagster_component_templates.CreateSamplesComponent
attributes:
  asset_name: customers_split
  upstream_asset_key: cleaned_customers
  test_size: 0.2
  validation_size: 0.1
  random_state: 42
  stratify_column: churn_label
  output_split_column: split
  group_name: ml_datasets
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame to split |
| Output | `pd.DataFrame` | Original DataFrame with an added split label column |

## Requirements

This component requires **scikit-learn** in addition to dagster and pandas.

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames.

## Notes

- All original rows and columns are preserved; only a new column is added.
- Split counts are logged after each run for easy verification.
- Stratified splitting requires the `stratify_column` to have at least 2 samples per class.
