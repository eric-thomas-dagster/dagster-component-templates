# HuggingFace Space Status Sensor

Fires a Dagster `RunRequest` when a [HuggingFace Space](https://huggingface.co/spaces) reaches a target stage. Best for triggering downstream Dagster work after a Space finishes rebuilding / restarting after a model push.

## How it works

Polls `HfApi().space_info(space_id)` on `minimum_interval_seconds`. Reads `info.runtime.stage` and compares against `target_stages`. When the Space hits a target stage **and** the `(stage, last_modified)` pair has changed since the last fire, the sensor emits a `RunRequest` for `job_name` with the Space metadata in `run_config`.

## Space stages

| Stage | Meaning |
|---|---|
| `RUNNING` | Space is up and serving (default target) |
| `RUNNING_BUILDING` | Space is up but a rebuild is in-flight |
| `BUILDING` | First-time build in progress |
| `STOPPED` / `PAUSED` / `SLEEPING` | Space is down for cost reasons |
| `BUILD_ERROR` / `CONFIG_ERROR` / `RUNTIME_ERROR` / `NO_APP_FILE` | Failed states — sensor skips, doesn't trigger |
| `DELETING` | Space is being torn down |

## Example

```yaml
type: dagster_community_components.HuggingfaceSpaceStatusSensorComponent
attributes:
  sensor_name: space_rebuilt
  space_id: my-org/my-app
  target_stages: ["RUNNING"]
  job_name: downstream_eval_job
  hf_token_env_var: HF_TOKEN
  minimum_interval_seconds: 60
```

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `sensor_name` | `str` | ✓ | — | Unique sensor name |
| `space_id` | `str` | ✓ | — | HF Space id (`my-org/my-app`) |
| `target_stages` | `List[str]` | — | `["RUNNING"]` | Stages that fire a RunRequest |
| `job_name` | `str` | ✓ | — | Dagster job to trigger |
| `hf_token_env_var` | `str` | — | — | Env var with HF token (for gated/private Spaces) |
| `minimum_interval_seconds` | `int` | — | 60 | Hub API poll cadence |
| `default_status` | `"running"` / `"stopped"` | — | `"running"` | Sensor default state |

## RunRequest config

Each fire passes the Space context to the downstream job:

```python
run_config = {"ops": {"config": {
    "huggingface_space_id": "my-org/my-app",
    "huggingface_space_stage": "RUNNING",
    "huggingface_space_last_modified": "2026-05-22T19:00:00",
}}}
```

## Requirements

```
huggingface-hub>=0.20.0
```

## See also

- [HuggingFace Spaces documentation](https://huggingface.co/docs/hub/en/spaces)
- [`huggingface_model_asset`](https://dagster-component-ui.vercel.app/c/huggingface_model_asset) — observe a model's Hub metadata
- [`huggingface_pipeline`](https://dagster-component-ui.vercel.app/c/huggingface_pipeline) — run any pipeline task
