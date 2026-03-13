# ModalAssetComponent

Trigger a [Modal](https://modal.com) function as a Dagster asset so that GPU training jobs, batch inference runs, and other serverless compute workloads become first-class, observable steps inside your data pipeline.

## Use case

Modal lets you define Python functions that run on managed, scalable cloud infrastructure — including GPU instances — without maintaining any servers.  `ModalAssetComponent` bridges Modal and Dagster so you can:

- **Gate downstream assets** (e.g. model evaluation, deployment) on a successful Modal training run via `deps`.
- **Pass hyperparameters and configuration** to Modal functions directly from your component YAML.
- **Track every invocation** as a Dagster asset materialization with metadata (function reference, kwargs, return value).
- **Choose your execution path** — use the Modal Python SDK when it is available in the worker environment, with automatic fallback to the `modal run` CLI.

## Prerequisites

The `modal` package must be installed in your Dagster worker environment for SDK execution. The CLI fallback requires `modal` to be on the worker's `PATH`.

```bash
pip install "modal>=0.55.0"
modal token new   # authenticate once
```

## Modal app setup

A minimal Modal app file (`training_app.py`) looks like:

```python
import modal

app = modal.App("ml_training_app")

@app.function(gpu="A100", timeout=3600)
def train_model(experiment_name: str = "default", n_estimators: int = 100):
    # your training code here
    return {"accuracy": 0.94, "experiment": experiment_name}
```

The `app.name` ("ml_training_app") becomes the first half of the `modal_function` reference; the decorated function name ("train_model") becomes the second half.

## Quick start

```yaml
type: dagster_component_templates.ModalAssetComponent
attributes:
  asset_name: train_churn_model
  modal_function: "ml_training_app::train_model"
  app_file: "{{ project_root }}/ml/training_app.py"
  function_name: train_model
  kwargs:
    experiment_name: churn_v2
    n_estimators: 500
  group_name: ml_training
  description: Train churn model on Modal A100 GPU
```

## SDK vs CLI execution

### SDK path (preferred)

When `modal` is importable in the worker, the component uses `modal.Function.lookup(app_name, function_name).remote(**kwargs)`.  This gives you:

- Synchronous result capture (surfaced as `return_value` in asset metadata).
- Proper exception propagation — a Modal function crash becomes a Dagster run failure.
- `detach=true` support via `.spawn()` for fire-and-forget dispatch.

### CLI fallback

When the SDK is not installed, the component falls back to:

```
modal run app_file::function_name -- '{"kwarg": "value"}'
```

stdout is streamed line-by-line to the Dagster run log. Non-zero exit codes raise a `RuntimeError` and fail the Dagster run.  The CLI path does not capture return values.

## GPU workloads

Modal handles GPU allocation transparently.  Annotate your Modal function with the GPU type you need:

```python
@app.function(gpu="H100", memory=32768, timeout=7200)
def fine_tune(model_id: str, dataset_path: str): ...
```

No changes are required in the Dagster component YAML — the GPU spec lives in the Modal app definition, not in the component config.

## Passing parameters

Kwargs are serialised and forwarded as Python keyword arguments (SDK) or as JSON via the `--` CLI separator. Keep values JSON-serialisable (strings, numbers, lists, dicts).

```yaml
kwargs:
  learning_rate: 0.001
  epochs: 50
  dataset: "s3://my-bucket/training-data"
```

## Surfacing Modal function outputs as metadata

When using the SDK path, the return value of the Modal function is captured and stored as the `return_value` asset metadata field (truncated to 2 000 characters for very large objects). Use this to surface metrics directly in the Dagster asset catalogue:

```python
# In your Modal function
def train_model(**kwargs):
    ...
    return {
        "val_accuracy": 0.943,
        "model_path": "s3://bucket/models/churn_v2.pkl",
        "training_duration_s": 412,
    }
```

The dict will appear as a JSON string under `return_value` in the materialisation event.  For richer metadata, consider writing results to a shared store (S3, MLflow, W&B) and reading them back in a downstream Dagster asset.

## Fire-and-forget with detach

Set `detach: true` to dispatch the Modal function without blocking:

```yaml
attributes:
  asset_name: kick_off_weekly_retraining
  modal_function: "training_app::train_all_models"
  detach: true
```

Dagster marks the asset as materialised as soon as the function is dispatched. Downstream assets will not wait for the Modal job to finish. Use this pattern when Modal jobs are very long-running and you monitor them directly in the Modal dashboard.

## Authentication

Modal credentials are read from the active Modal profile by default (`~/.modal.toml` or `MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET` env vars).

Override credentials per-component:

```yaml
modal_token_id_env_var: MY_MODAL_TOKEN_ID
modal_token_secret_env_var: MY_MODAL_TOKEN_SECRET
```

The component reads the values from those environment variables at runtime and injects them into the subprocess or SDK client — credentials are never written to disk.

## Asset dependencies

```yaml
attributes:
  asset_name: train_churn_model
  deps:
    - feature_engineering_output   # upstream asset that must be materialised first
  ...
```

Downstream assets (e.g. model evaluation, champion/challenger comparison) can in turn declare a dependency on `train_churn_model`.

## Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset key |
| `modal_function` | `str` | required | `app_name::function_name` reference |
| `app_file` | `str` | `None` | Path to Modal app Python file |
| `function_name` | `str` | `None` | Function name within the app file |
| `args` | `list` | `None` | Positional args forwarded to the function |
| `kwargs` | `dict` | `None` | Keyword args forwarded to the function |
| `modal_token_id_env_var` | `str` | `None` | Env var for MODAL_TOKEN_ID override |
| `modal_token_secret_env_var` | `str` | `None` | Env var for MODAL_TOKEN_SECRET override |
| `detach` | `bool` | `false` | Fire-and-forget dispatch |
| `environment_name` | `str` | `None` | Modal environment / workspace namespace |
| `group_name` | `str` | `"compute"` | Dagster asset group |
| `description` | `str` | `None` | Asset description |
| `deps` | `list[str]` | `None` | Upstream asset keys |
