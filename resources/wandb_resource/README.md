# Wandb Resource

Registers a [Weights & Biases](https://wandb.ai) resource for experiment tracking in your Dagster project.

## Install

```
pip install wandb
```

## Configuration

```yaml
type: dagster_component_templates.WandbResourceComponent
attributes:
  resource_key: wandb_resource
  api_key_env_var: WANDB_API_KEY
  entity: my-team          # optional
  project: my-project      # optional
```

## Auth

Set the environment variable named in `api_key_env_var` to your W&B API key. Retrieve it from https://wandb.ai/authorize.

```
export WANDB_API_KEY=<your-api-key>
```
