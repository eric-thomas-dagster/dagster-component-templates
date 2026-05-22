# External HuggingFace Space Asset

Declares a [HuggingFace Space](https://huggingface.co/spaces) as an external asset in the Dagster catalog. The Space lives on HF's infra; Dagster observes its lifecycle via the paired `huggingface_space_status_sensor`.

## Pattern

1. **Declare** the Space as an external asset:
   ```yaml
   type: dagster_community_components.ExternalHuggingfaceSpaceAsset
   attributes:
     asset_key: hf/spaces/my_app
     space_id: my-org/my-app
   ```
2. **Pair** with the sensor (same `asset_key`):
   ```yaml
   type: dagster_community_components.HuggingfaceSpaceStatusSensorComponent
   attributes:
     sensor_name: my_app_running
     space_id: my-org/my-app
     asset_key: hf/spaces/my_app          # same as above
     target_stages: ["RUNNING"]
     job_name: downstream_eval_job
     hf_token_env_var: HF_TOKEN
   ```

On each Space stage transition that matches `target_stages`, the sensor emits `AssetMaterialization` for the asset_key — materialization history of this asset reflects every Space rebuild / restart you've watched.

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_key` | `str` | ✓ | Dagster asset key |
| `space_id` | `str` | ✓ | HuggingFace Space id (e.g. `my-org/my-app`) |
| `group_name`, `description`, `owners`, `tags`, `kinds`, `deps` | — | — | Standard catalog metadata |

The asset auto-includes the `huggingface` + `space` kinds and an `observability_type: external` metadata marker.

## See also

- [`huggingface_space_status_sensor`](https://dagster-component-ui.vercel.app/c/huggingface_space_status_sensor) — paired sensor
- [`huggingface.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/huggingface.md) — full HF walkthrough
