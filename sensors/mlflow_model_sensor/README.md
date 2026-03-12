# MLflow Model Sensor

Monitor the MLflow Model Registry and automatically trigger Dagster jobs whenever a registered model version transitions to a specified stage (typically "Production").  This bridges the gap between model development workflows and production data pipelines: when a data scientist promotes a model, downstream scoring or inference jobs run immediately — no manual hand-off required.

## Features

- Watches any registered model in any MLflow-compatible registry (open-source, Databricks, Azure ML MLflow-compatible)
- Configurable target stage: Production, Staging, or Archived
- Cursor-based deduplication — each model version fires at most one run, even across restarts
- Passes `model_name`, `model_version`, `model_uri`, and `stage` to the triggered job via `run_config`
- Supports optional extra `run_config` merged alongside the model metadata
- No credentials stored in YAML — Tracking URI is resolved from an environment variable at evaluation time

## MLflow Model Registry Stages

The MLflow Model Registry defines four lifecycle stages for model versions:

| Stage | Meaning |
|---|---|
| **None** | Freshly registered; not yet promoted |
| **Staging** | Validated and awaiting production approval |
| **Production** | Live model — typically what triggers scoring pipelines |
| **Archived** | Retired; no longer active |

Set `target_stage` to the stage you want to act on.  Most teams trigger on `Production`.

## Configuration

### Required Parameters

- **sensor_name** — Unique sensor identifier
- **tracking_uri_env_var** — Environment variable name that holds the MLflow Tracking URI (e.g., `MLFLOW_TRACKING_URI`)
- **model_name** — Registered model name exactly as it appears in the Model Registry
- **target_job** — Dagster job to trigger when a new version is detected

### Optional Parameters

- **target_stage** (default: `"Production"`) — Registry stage to watch
- **minimum_interval_seconds** (default: `60`) — Poll frequency
- **run_config** (default: `None`) — Additional run config merged into the triggered job

## Usage Example

```yaml
type: dagster_component_templates.MLflowModelSensor
attributes:
  sensor_name: production_model_trigger
  tracking_uri_env_var: MLFLOW_TRACKING_URI
  model_name: customer_churn_model
  target_stage: Production
  target_job: run_churn_scoring_pipeline
  minimum_interval_seconds: 120
```

### With extra run config

```yaml
type: dagster_component_templates.MLflowModelSensor
attributes:
  sensor_name: fraud_model_trigger
  tracking_uri_env_var: MLFLOW_TRACKING_URI
  model_name: fraud_detection_v2
  target_stage: Production
  target_job: run_fraud_scoring_pipeline
  run_config:
    ops:
      config:
        batch_size: 10000
        output_table: fraud_scores_prod
```

## How Cursor-Based Deduplication Works

The sensor stores the **highest model version number** it has already processed in the Dagster cursor.  On each evaluation:

1. `client.get_latest_versions(model_name, stages=[target_stage])` returns the latest version(s) in the target stage.
2. Any version number greater than the stored cursor generates a `RunRequest`.
3. After yielding run requests the cursor is advanced to the new maximum version.

This means:
- Promoting version 5 to Production fires exactly one run.
- The sensor being evaluated 100 times while version 5 is still the latest fires the run only once.
- Rolling back to version 4 does not re-fire (the cursor already passed 4).

## Run Config Injected by the Sensor

The sensor always injects the following keys under `ops.config` in the triggered job's run config:

```python
{
    "ops": {
        "config": {
            "model_name": "customer_churn_model",
            "model_version": "7",
            "model_uri": "models:/customer_churn_model/7",
            "stage": "Production",
            # ... any extra keys from your run_config attribute
        }
    }
}
```

### Consuming Model Metadata in Your Job

```python
from dagster import op, job

@op
def run_scoring(context):
    model_uri = context.op_config["model_uri"]
    model_version = context.op_config["model_version"]

    import mlflow.pyfunc
    model = mlflow.pyfunc.load_model(model_uri)

    context.log.info(f"Scoring with model version {model_version}")
    # ... load data, run predictions, write results

@job
def run_churn_scoring_pipeline():
    run_scoring()
```

## Wiring with a Scoring or Inference Asset Job

A common pattern is to define a Dagster asset that reads model metadata from run config, loads the model, and materializes scored predictions:

```python
from dagster import asset, define_asset_job

@asset
def churn_predictions(context):
    model_uri = context.op_config.get("model_uri")
    # load model from MLflow, score, write to feature store / warehouse
    ...

score_job = define_asset_job(
    name="run_churn_scoring_pipeline",
    selection=["churn_predictions"],
)
```

Then point `target_job: run_churn_scoring_pipeline` in your component YAML.

## Environment Variables

| Variable | Description |
|---|---|
| `MLFLOW_TRACKING_URI` | HTTP(S) URI of your MLflow Tracking Server, e.g. `http://mlflow.internal:5000` |

The variable name is configurable via `tracking_uri_env_var`.

## Requirements

- `mlflow>=2.0.0`
- A running MLflow Tracking Server with the Model Registry feature enabled

## Troubleshooting

**"Environment variable 'MLFLOW_TRACKING_URI' is not set"**
Set the variable in your Dagster deployment environment before starting the daemon.

**"No versions of 'my_model' found in stage 'Production'"**
The model exists in the registry but no version has been promoted to the target stage yet. Promote a version in the MLflow UI or via `client.transition_model_version_stage(...)`.

**Sensor fires on every evaluation for the same version**
Check that the Dagster cursor is persisting correctly. This can happen if the Dagster instance storage is ephemeral (e.g., in-memory SQLite in dev). Use a persistent storage backend in production.

**mlflow import error**
Install the dependency: `pip install mlflow>=2.0.0`.
