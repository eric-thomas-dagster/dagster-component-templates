# BigQuery ML Predict

Run inference against a trained BQML model. Wraps:

- `ML.PREDICT` — classifications / regressions
- `ML.FORECAST` — ARIMA / time-series
- `ML.EXPLAIN_PREDICT` — feature attributions
- `ML.DETECT_ANOMALIES` — anomaly detection

Returns a pandas DataFrame of predictions; `keep_input_columns: true` (default) preserves the input rows alongside the predicted ones.

```yaml
type: dagster_component_templates.BigQueryMLPredictAssetComponent
attributes:
  asset_name: iris_predictions
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  model_id: my-project.ml.iris_logreg
  operation: predict
  input_query: |
    SELECT * FROM `bigquery-public-data.ml_datasets.iris` LIMIT 10
```

For ML.FORECAST set `operation: forecast` and put forecast options in `options`:

```yaml
operation: forecast
options:
  horizon: 30
  confidence_level: 0.95
```

Required SA roles: `roles/bigquery.modelDataViewer` + `roles/bigquery.jobUser`.
