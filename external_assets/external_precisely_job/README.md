# External Precisely Job Asset

Declares a [Precisely Connect ETL](https://www.precisely.com/product/precisely-connect/connect) job as an **external asset** in the Dagster catalog. Precisely runs the job on its own schedule; Dagster surfaces the run state via a paired sensor.

## Why an "external" asset?

Precisely owns the schedule and execution. Dagster doesn't trigger Precisely jobs (the submit endpoint isn't in Precisely's public REST docs). The right Dagster shape for this is:

- **`external_precisely_job` (this component)** — declares the asset in the catalog (`AssetSpec` only, no execution function)
- **`precisely_job_sensor`** — watches the Precisely Job Status endpoint and emits `AssetMaterialization(asset_key=...)` on terminal SUCCESS, which lights up this asset in the Dagster catalog

Together they give you catalog presence + automatic materialization events without pretending Dagster can start a Precisely run.

## Example

```yaml
type: dagster_community_components.ExternalPreciselyJobAsset
attributes:
  asset_key: precisely/etl/load_customers
  job_id: "abc-123-xyz"
  host: https://precisely.mycompany.com
  group_name: precisely
  description: "Precisely Connect ETL job that loads customer records."
```

Pair with the sensor:

```yaml
type: dagster_community_components.PreciselyJobSensorComponent
attributes:
  sensor_name: precisely_load_customers_done
  job_run_id: "abc-123-xyz-run-456"
  asset_key: precisely/etl/load_customers      # ← same asset_key as above
  host_env_var: PRECISELY_HOST
  api_token_env_var: PRECISELY_API_TOKEN
  job_name: downstream_processing_job
```

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_key` | `str` | ✓ | Dagster asset key |
| `job_id` | `str` | — | Precisely Connect ETL job id (stable, not run-id) |
| `job_run_id` | `str` | — | Specific run id (optional metadata) |
| `host` | `str` | — | Precisely Connect ETL host URL (rendered as clickable link) |
| `group_name`, `description`, `owners`, `tags`, `kinds`, `deps` | — | — | Standard catalog metadata |

The asset auto-includes the `precisely` + `etl` kinds and an `observability_type: external` metadata marker that Dagster Cloud uses to render the asset differently in the catalog.

## See also

- [`precisely_job_sensor`](https://dagster-component-ui.vercel.app/c/precisely_job_sensor) — the paired sensor that materializes this asset
- [`precisely_validation.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/precisely_validation.md) — end-to-end walkthrough
