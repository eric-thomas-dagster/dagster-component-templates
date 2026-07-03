# external_vercel_deployment

Declare a Vercel project's deployment stream as an external Dagster asset. Vercel owns the build lifecycle (git pushes, deploy hooks, cron); Dagster observes via the paired [`vercel_deployment_sensor`](../../sensors/vercel_deployment_sensor).

## Why

- **Catalog presence.** The Vercel deployment is a first-class node in the Dagster graph — you can `deps: [...]` against it from data assets that depend on the site being green.
- **Lineage.** Downstream Dagster assets (post-deploy smoke tests, CDN warmers, analytics ETL) can express dependence on production deployments.
- **UI links.** `vercel_dashboard_url` and `production_url` surface as clickable links in the Dagster catalog.

## Example

```yaml
type: dagster_community_components.ExternalVercelDeploymentAsset
attributes:
  asset_key: vercel/website/production
  project_name: my-marketing-site
  target: production
  vercel_dashboard_url: https://vercel.com/my-team/my-marketing-site
  production_url: https://my-marketing-site.com
```

## Fields

| Field | Type | Description |
|---|---|---|
| `asset_key` | string | Dagster asset key, `/`-separated. |
| `project_id` / `project_name` | string | Prefer `project_id` — stable across renames. |
| `target` | string | `production` (default), `preview`, `development`. |
| `team_id` | string | Vercel team ID for team-scoped projects. |
| `vercel_dashboard_url` | string | Clickable link to the project's Vercel dashboard. |
| `production_url` | string | Canonical URL of the production site. |
| `group_name` / `description` / `owners` / `tags` / `kinds` / `deps` | — | Standard catalog fields. |

## Metadata

The AssetSpec ships with these metadata keys:
- `dagster.observability_type=external`
- `vercel_project_id` / `vercel_project_name` / `vercel_team_id` (when set)
- `vercel_target`
- `vercel_dashboard_url` / `vercel_production_url` (as clickable URLs)

## Pairing

Point the [`vercel_deployment_sensor`](../../sensors/vercel_deployment_sensor) at the same `asset_key` — it will emit `AssetMaterialization` or `AssetObservation` events on this asset every time Vercel reports a terminal deployment.
