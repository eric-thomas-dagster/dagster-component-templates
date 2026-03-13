# Weights & Biases Asset Component

Logs metrics from a database table or uploads file/directory artifacts to [Weights & Biases](https://wandb.ai), creating a tracked W&B run for every Dagster materialization.

---

## What it does

Supports two modes, selected by the `mode` field:

| Mode | Description |
|------|-------------|
| `log_metrics` | Reads rows from a source database table via SQLAlchemy and logs each row as a step in a W&B run using `run.log({...})`. |
| `log_artifact` | Uploads a local file or directory as a W&B artifact using `wandb.Artifact` and `run.log_artifact(...)`. |

---

## Required packages

| Package | Minimum version | Purpose |
|---------|----------------|---------|
| `dagster` | 1.8.0 | Orchestration framework |
| `wandb` | 0.17.0 | W&B Python SDK |
| `sqlalchemy` | 2.0.0 | Database connectivity (log_metrics mode) |

Install with:

```bash
pip install wandb>=0.17.0 sqlalchemy>=2.0.0
```

You will also need the appropriate SQLAlchemy dialect driver for your source database (e.g. `psycopg2` for PostgreSQL, `pymysql` for MySQL).

---

## Configuration fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | — | Dagster asset key name. |
| `project` | `str` | — | W&B project name. |
| `entity` | `str \| null` | `null` | W&B entity (team or username). |
| `run_name` | `str \| null` | `null` | W&B run name. Auto-generated when null. |
| `mode` | `str` | `log_metrics` | `"log_metrics"` or `"log_artifact"`. |
| `wandb_api_key_env_var` | `str` | `WANDB_API_KEY` | Env var holding the W&B API key. |
| `database_url_env_var` | `str \| null` | `null` | Env var with SQLAlchemy DB URL. Required for `log_metrics`. |
| `source_table` | `str \| null` | `null` | Table to read metrics from. Required for `log_metrics`. |
| `metrics_columns` | `list \| null` | `null` | Columns to log. `null` logs all numeric columns. |
| `step_column` | `str \| null` | `null` | Column to use as the W&B step/epoch number. |
| `artifact_path` | `str \| null` | `null` | Local file or directory path. Required for `log_artifact`. |
| `artifact_name` | `str \| null` | `null` | Artifact name in W&B. Defaults to the basename of `artifact_path`. |
| `artifact_type` | `str` | `dataset` | W&B artifact type: `"dataset"`, `"model"`, `"evaluation"`, etc. |
| `tags` | `list \| null` | `null` | String tags attached to the W&B run. |
| `group_name` | `str \| null` | `ml_tracking` | Dagster asset group name shown in the UI. |
| `deps` | `list \| null` | `null` | Upstream asset keys for lineage. |

---

## Example YAML

### Log metrics from a training table

```yaml
type: dagster_component_templates.WandbAssetComponent
attributes:
  asset_name: model_training_run
  project: churn-prediction
  entity: my-team
  mode: log_metrics
  wandb_api_key_env_var: WANDB_API_KEY
  database_url_env_var: DATABASE_URL
  source_table: model_training_results
  metrics_columns:
    - train_loss
    - val_loss
    - accuracy
  step_column: epoch
  tags:
    - production
    - v2
  group_name: ml_tracking
  deps:
    - marts/training_data
```

### Upload a model checkpoint

```yaml
type: dagster_component_templates.WandbAssetComponent
attributes:
  asset_name: model_checkpoint_upload
  project: churn-prediction
  entity: my-team
  mode: log_artifact
  wandb_api_key_env_var: WANDB_API_KEY
  artifact_path: /tmp/checkpoints/epoch_50
  artifact_name: churn_model_epoch50
  artifact_type: model
  tags:
    - checkpoint
  group_name: ml_tracking
```

---

## Required environment variables

| Variable | Description |
|----------|-------------|
| `WANDB_API_KEY` (or custom) | W&B API key from https://wandb.ai/settings |
| `DATABASE_URL` (or custom) | SQLAlchemy connection URL for the source database (log_metrics mode only) |

---

## Materialization metadata

Each run emits the following metadata visible in the Dagster UI:

| Key | Description |
|-----|-------------|
| `run_id` | W&B run ID |
| `project` | W&B project name |
| `run_url` | Direct link to the W&B run page |
| `mode` | The mode used (`log_metrics` or `log_artifact`) |
