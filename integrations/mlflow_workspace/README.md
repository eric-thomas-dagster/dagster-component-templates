# MLflowWorkspaceComponent

Auto-emit Dagster assets for both **MLflow experiments** (recent runs + metrics + params) and **registered models** (versions). `StateBackedComponent` — discovery cached to disk. Materializing:

- **experiment asset** → recent runs DataFrame (each row = one run, with dynamic `metric_*` and `param_*` columns)
- **model asset** → model versions DataFrame (each row = one version)

## Example

```yaml
type: dagster_community_components.MLflowWorkspaceComponent
attributes:
  tracking_uri_env_var: MLFLOW_TRACKING_URI
  experiment_selector:
    by_pattern: ["prod_*"]
  model_selector:
    by_pattern: ["*_classifier"]
  runs_limit: 100
```

## Auth

MLflow Tracking servers commonly use basic auth. Set `username_env_var` + `password_env_var` if your server enforces it. Databricks-hosted MLflow uses a personal access token — set `username_env_var: token` (literal string) and `password_env_var: DATABRICKS_TOKEN`.

## Related

- `mlflow_resource`
- `mlflow_model_sensor` — trigger on model registry state changes
