# MLflowModelInferenceComponent

Materializable asset that loads a registered MLflow model + scores an upstream DataFrame. Uses `mlflow.pyfunc.load_model()` so works with sklearn / xgboost / lightgbm / pytorch / any pyfunc flavor.

Right for: "score today's batch" as a first-class Dagster asset in the graph — not a shell script that runs somewhere on a cron.

## Selection modes

- `model_stage: Production` — loads latest version at that stage (typical)
- `model_version: "7"` — pinned version (reproducibility / A/B)

## Example (score with latest Production model)

```yaml
type: dagster_community_components.MLflowModelInferenceComponent
attributes:
  asset_name: daily_churn_scores
  upstream_asset_key: customer_features
  tracking_uri_env_var: MLFLOW_TRACKING_URI
  model_name: churn_model
  model_stage: Production
  output_column: churn_score
  group_name: ml_scoring
```

## Example (pin version, drop input columns, keep id)

```yaml
attributes:
  asset_name: daily_churn_scores
  upstream_asset_key: customer_features
  model_name: churn_model
  model_version: "7"
  output_column: churn_score
  keep_input_columns: false
  id_columns: [customer_id]
```

## Metadata attached to each materialization

- `model_uri` (e.g. `models:/churn_model/Production`)
- `model_name`
- `row_count`
- `load_seconds`, `score_seconds`, `rows_per_second`
- `input_columns`, `output_columns`

## Related

- `mlflow_model_version_check` — gate on model existence/stage before this asset runs
- `mlflow_model_promotion` — the CD step that promotes a version this asset then loads
