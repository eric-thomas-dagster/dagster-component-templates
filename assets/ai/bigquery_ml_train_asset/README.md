# BigQuery ML Train

Train a BQML model directly inside BigQuery — no Python ML stack, no scikit-learn, no PyTorch. Compiles to:

```sql
CREATE OR REPLACE MODEL `<destination_model_id>`
  OPTIONS(<model_options>)
  AS <select_query>
```

Supports every BQML model type: `LINEAR_REG`, `LOGISTIC_REG`, `KMEANS`, `ARIMA_PLUS`, `ARIMA_PLUS_XREG`, `DNN_REGRESSOR`, `DNN_CLASSIFIER`, `BOOSTED_TREE_REGRESSOR`, `BOOSTED_TREE_CLASSIFIER`, `RANDOM_FOREST_*`, `MATRIX_FACTORIZATION`, `AUTOENCODER`, `PCA`, `AUTOML_REGRESSOR`, `AUTOML_CLASSIFIER`, `TENSORFLOW`.

Asset's stored value is the result of `ML.EVALUATE` (model fit metrics: r2, log_loss, AUC, etc.). Headline metrics are also surfaced as Dagster materialization metadata for use in `freshness_check` / `quality_check`-style asset checks downstream.

```yaml
type: dagster_component_templates.BigQueryMLTrainAssetComponent
attributes:
  asset_name: iris_logreg
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  destination_model_id: my-project.ml.iris_logreg
  model_options:
    model_type: LOGISTIC_REG
    input_label_cols: [species]
    auto_class_weights: true
  select_query: |
    SELECT * FROM `bigquery-public-data.ml_datasets.iris`
```

Required SA roles: `roles/bigquery.dataEditor` (destination dataset) + `roles/bigquery.jobUser` (project).
