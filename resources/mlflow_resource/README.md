# Mlflow Resource

Registers an [MLflow](https://mlflow.org) resource for experiment tracking and model registry in your Dagster project.

## Install

```
pip install mlflow
```

## Configuration

```yaml
type: dagster_component_templates.MLflowResourceComponent
attributes:
  resource_key: mlflow_resource
  tracking_uri: http://localhost:5000
  experiment_name: my-experiment   # optional
  username_env_var: MLFLOW_USER    # optional
  password_env_var: MLFLOW_PASS    # optional
```

## Auth

For authenticated MLflow servers, set the environment variables named in `username_env_var` and `password_env_var`. For Databricks-hosted MLflow use `tracking_uri: databricks` and configure Databricks credentials separately.

```
export MLFLOW_USER=<username>
export MLFLOW_PASS=<password>
```
