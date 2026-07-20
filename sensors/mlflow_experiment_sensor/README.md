# MLflowExperimentSensorComponent

Polls an MLflow experiment for new runs and fires a Dagster `RunRequest` per new run. Cursor-based dedup on `run_id` — only unseen runs trigger.

Right for: data scientist finishes a training run → downstream evaluation / feature-recompute / promotion-review job auto-fires.

## Example

```yaml
type: dagster_community_components.MLflowExperimentSensorComponent
attributes:
  sensor_name: churn_experiment_runs
  tracking_uri_env_var: MLFLOW_TRACKING_URI
  experiment_name: churn_prediction
  filter_string: "attributes.status = 'FINISHED'"
  target_job: evaluate_new_run
  minimum_interval_seconds: 60
```

## Injected run config

For each new run, the sensor injects into the triggered job's `run_config.ops.config`:

- `mlflow_run_id`
- `mlflow_experiment_id`
- `mlflow_status`
- `mlflow_artifact_uri`
- `mlflow_start_time`, `mlflow_end_time`
- `mlflow_tags` (dict)

Downstream ops read these to fetch the specific run's artifacts.

## Related

- `mlflow_model_sensor` — sibling, watches the Model Registry for stage transitions (not experiment runs)
- `mlflow_model_version_check` — asset check gate on model-registry state
- `mlflow_model_promotion` — the promotion action itself
- `mlflow_model_inference` — score data with a registered model
