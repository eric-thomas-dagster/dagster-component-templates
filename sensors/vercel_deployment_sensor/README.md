# vercel_deployment_sensor

Poll Vercel's REST API for deployment state and emit an `AssetMaterialization` / `AssetObservation` when a deployment reaches `READY`. Fires a downstream Dagster job on terminal success (and optionally on terminal errors, for alert / rollback flows).

## When to use

**Primary pattern.** Vercel typically deploys from git pushes on Vercel's own schedule — Dagster's role is to **observe** the deployment lifecycle and connect it to downstream data workflows:
- Fire post-deploy smoke tests as a Dagster job.
- Kick off a cache warmer or CDN purge asset after production goes green.
- Record deployments in the catalog for lineage (paired with [`external_vercel_deployment`](../../external_assets/external_vercel_deployment)).

## Example

```yaml
type: dagster_community_components.VercelDeploymentSensorComponent
attributes:
  sensor_name: vercel_prod_ready
  project_id: prj_AbCdEfGh12345
  target: production
  api_token_env_var: VERCEL_API_TOKEN
  job_name: post_deploy_smoke_tests_job
  asset_key: vercel/website/production
```

Team-scoped:

```yaml
attributes:
  # ...
  team_id: team_AbCdEfGh12345
```

Fire the job on errors too (for alert/rollback flows):

```yaml
attributes:
  # ...
  error_state_triggers_job: true
```

## Fields

| Field | Type | Description |
|---|---|---|
| `sensor_name` | string | Unique sensor name. |
| `project_id` | string | Vercel project ID (`prj_...`). Set this OR `project_name`. Prefer `project_id` — stable across renames. |
| `project_name` | string | Vercel project slug. Set this OR `project_id`. |
| `target` | string | `production` (default), `preview`, `development`, or `any`. |
| `team_id` | string | Vercel team ID (`team_...`). Required for team-scoped projects. |
| `api_token_env_var` | string | Env var with a Vercel API token. Default `VERCEL_API_TOKEN`. |
| `api_base_url` | string | Default `https://api.vercel.com`. |
| `job_name` | string | Dagster job fired on terminal success. |
| `asset_key` | string | Optional asset materialized / observed on terminal success. |
| `asset_event_type` | string | `materialization` (default) or `observation`. |
| `error_state_triggers_job` | bool | Default `false`. When true, ERROR deployments ALSO fire the job (with a tag). |
| `minimum_interval_seconds` | int | Default 60. |
| `default_status` | string | `running` or `stopped`. |

## Metadata emitted on success

| Key | Source |
|---|---|
| `vercel_deployment_uid` | Vercel deployment `uid` |
| `vercel_state` | `READY` (or `ERROR` when opted in) |
| `vercel_target` | `production` / `preview` / `development` |
| `vercel_deployment_url` | Clickable URL to the preview / prod URL |
| `vercel_commit_sha` / `vercel_branch` / `vercel_commit_message` | From `meta.githubCommit*` when the project is git-linked |
| `vercel_created_at` | Deployment creation timestamp |

## Auth

Create a personal or team API token at https://vercel.com/account/tokens. Set it as an env var and pass the var name via `api_token_env_var`. Never commit the token.

## Terminal state handling

- `READY` → success. Fires job + asset event, advances cursor.
- `ERROR` → skip_reason by default; set `error_state_triggers_job: true` to fire too.
- `CANCELED` → skip_reason (treat as no-op).
- `BUILDING` / `QUEUED` / `INITIALIZING` → skip_reason, poll again.

Cursor is the deployment `uid`, so re-polling the same terminal deployment across ticks doesn't double-fire.

## Requirements

```
requests>=2.28
```
