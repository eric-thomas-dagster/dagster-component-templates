# MLflowModelVersionCheckComponent

Asset check that verifies an MLflow registered model's state. Attach to any Dagster asset — the check runs when Dagster evaluates checks (or before that asset is materialized, if `severity: ERROR`).

Validates:
- Model exists in registry
- Optional: specific version exists (`required_version`)
- Optional: model has ≥1 version at `required_stage` (Production / Staging / Archived)
- Optional: latest version at that stage was created within `max_age_days` (freshness gate)

## Example (production model must exist)

```yaml
type: dagster_community_components.MLflowModelVersionCheckComponent
attributes:
  asset_key: daily_churn_predictions
  check_name: churn_model_prod_exists
  tracking_uri_env_var: MLFLOW_TRACKING_URI
  model_name: churn_model
  required_stage: Production
```

## Example (pin version + freshness gate)

```yaml
attributes:
  asset_key: daily_churn_predictions
  check_name: churn_model_v7_recent
  tracking_uri_env_var: MLFLOW_TRACKING_URI
  model_name: churn_model
  required_version: "7"
  max_age_days: 30
  severity: WARN
```

## Related

- `mlflow_model_sensor` — react to new versions
- `mlflow_model_promotion` — promote a version to a stage
- `mlflow_model_inference` — score data with the model
