# W&B Reader Component

Fetch Weights & Biases experiment runs, config parameters, and summary metrics as a Dagster asset DataFrame.

## Overview

The `WandbReaderComponent` connects to the W&B API and retrieves experiment run metadata, flattening config and summary metrics into a DataFrame. Useful for experiment analysis, model comparison, and MLOps pipelines.

## Use Cases

- **Experiment tracking**: Analyze all runs in a project
- **Model selection**: Compare metrics across runs
- **Hyperparameter analysis**: Study config vs performance
- **Reporting**: Build dashboards from W&B data

## Configuration

### Fetch finished runs

```yaml
type: dagster_component_templates.WandbReaderComponent
attributes:
  asset_name: training_runs
  project: image-classification
  entity: my-team
  filters:
    state: finished
  n_runs: 100
```

### All runs with history

```yaml
type: dagster_component_templates.WandbReaderComponent
attributes:
  asset_name: all_experiments
  project: nlp-experiments
  n_runs: 50
  include_history: true
```

## Output Schema

| Column | Description |
|--------|-------------|
| `run_id` | W&B run ID |
| `run_name` | Human-readable run name |
| `state` | Run state (finished, running, failed) |
| `created_at` | Run creation timestamp |
| `config_*` | Config parameters (prefixed with `config_`) |
| `summary_*` | Summary metrics (prefixed with `summary_`) |
| `history_steps` | Number of training steps (if `include_history=true`) |

## Requirements

- `WANDB_API_KEY` environment variable set

## Dependencies

- `pandas>=1.5.0`
- `wandb>=0.16.0`
