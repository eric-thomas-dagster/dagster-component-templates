# Argo Workflows

[Argo Workflows](https://argoproj.github.io/workflows/) is the CNCF Kubernetes-native workflow engine. Customers running Argo already on k8s want to bridge to Dagster — let Argo continue running the underlying workflows (often Spark / Flink / ML training / custom containers) while Dagster owns the catalog + lineage + downstream orchestration. Pattern mirrors our Precisely Connect ETL surface.

The community registry ships **3 Argo components** today, covering both directions of integration:

## Components

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`external_argo_workflow`](https://dagster-component-ui.vercel.app/c/external_argo_workflow) | external asset | Declare-only `AssetSpec` — surfaces an Argo Workflow (or WorkflowTemplate) as a real catalog asset. Downstream `deps:` work, lineage shows up, materialization history is real. No execution. | `code` |
| [`argo_workflow_sensor`](https://dagster-component-ui.vercel.app/c/argo_workflow_sensor) | sensor | Polls Argo Server `/api/v1/workflows/{namespace}/{name}` (or by label selector). On terminal `Succeeded` / `Skipped`, emits `AssetMaterialization` (or `AssetObservation` — see `asset_event_type`) and fires a `RunRequest` against any downstream Dagster job. | `code` |
| [`argo_workflow_trigger`](https://dagster-component-ui.vercel.app/c/argo_workflow_trigger) | infrastructure | Materializable asset that submits an Argo Workflow via `POST /api/v1/workflows/{namespace}/submit` (from a WorkflowTemplate) or `POST /api/v1/workflows/{namespace}` (inline manifest). Captures the server-generated workflow name in metadata so a paired sensor can watch it. | `code` |

## Two valid pairings — pick ONE per `asset_key`

| Case | Schedule owner | Components | Materialization timeline |
|---|---|---|---|
| **A. Observe-only** | Argo | `external_argo_workflow` + `argo_workflow_sensor` (`asset_event_type=materialization`) | Sensor emits AssetMaterialization on terminal success |
| **B. Dagster-triggered** | Dagster | `argo_workflow_trigger` + optional `argo_workflow_sensor` (`asset_event_type=observation`) | Trigger emits Materialization on submit; sensor emits Observation on terminal success |

**Do NOT mix** `external_argo_workflow` and `argo_workflow_trigger` on the same `asset_key` — they're alternatives. Same `asset_key` value across the components in your chosen Case is the glue.

## Connection / auth — quick reference

| Surface | Setting | Notes |
|---|---|---|
| Argo Server URL | `host_env_var: ARGO_SERVER_URL` | `https://argo.mycompany.com` — port-forward `argo-server` if running locally |
| Bearer token | `token_env_var: ARGO_TOKEN` | Service-account token: `kubectl -n argo create token argo-server-sa` (replace SA name to match your install). Leave empty on clusters with anonymous read. |
| Namespace | `namespace:` (default `argo`) | The k8s namespace where workflows live |
| TLS verify | `verify_ssl: true` (default) | Set `false` only for dev clusters with self-signed certs |

Auth-mode summary: Argo Server's REST API accepts standard k8s service-account tokens. The token's RBAC determines what workflows + namespaces are visible. For a sensor that only reads, `view` on workflows in the target namespace suffices; for the trigger, `create` is required.

## Workflow phases

Argo's terminal phases (`status.phase` field on a Workflow):

| Phase | Treated as | Triggers sensor emit? |
|---|---|---|
| `Succeeded` | Success | ✅ yes |
| `Skipped` | Success (workflow opted out) | ✅ yes |
| `Failed` | Terminal failure | ❌ no |
| `Error` | Terminal failure | ❌ no |
| `Omitted` | Terminal failure | ❌ no |
| `Pending` / `Running` | In progress | ❌ — sensor will check again next tick |

## Gotchas

- **`workflow_name` vs `workflow_template_label`** — the sensor needs exactly one. Use `workflow_name` when Dagster knows the name (Case B); use `workflow_template_label` when Argo schedules workflows on its own and Dagster wants "watch the latest run of this template" semantics (Case A).
- **Server-generated names** — when submitting from a WorkflowTemplate, Argo appends a 5-character suffix to the name (`nightly-aggregation-xyz12`). Pre-knowing the name from Dagster's side requires reading it back from the trigger's materialization metadata. A future enhancement could auto-discover the name from the latest materialization (`workflow_name_from_asset_key`).
- **`generateName` in inline manifests** — when submitting a one-off `workflow_manifest`, prefer `metadata.generateName` over `metadata.name` so re-runs don't conflict on the existing-name uniqueness constraint.
- **Token rotation** — service-account tokens in modern k8s are short-lived (~1h) by default. For production, generate a long-lived `kubernetes.io/service-account-token` secret or use OIDC. Token-expiry errors surface as `401 Unauthorized` from the sensor poll loop.

## Roadmap

- **Auto-discover workflow_name from trigger metadata** — same pattern proposed for Precisely (`job_run_id_from_asset_key`). Closes the submit→watch loop fully on Case B.
- **Argo CronWorkflow integration** — Argo has a separate `CronWorkflow` CRD for scheduled workflows. Today the sensor's label-selector mode handles this, but a dedicated `external_argo_cron_workflow` component would be cleaner for declarative catalog entry.
- **Log-tail asset** — pull recent workflow logs as asset metadata for debugging. Useful for live-validation runs.
- **Sensor → emit one event per workflow** instead of "latest terminal" — handle backfills + multi-fire-in-tick correctly.

## See also

- [Argo Workflows REST API docs](https://argo-workflows.readthedocs.io/en/latest/rest-api/)
- [Argo Workflows operator manual (CRDs, kubectl plugin)](https://argo-workflows.readthedocs.io/)
- [`external_argo_workflow` README](https://dagster-component-ui.vercel.app/c/external_argo_workflow)
- [`argo_workflow_sensor` README](https://dagster-component-ui.vercel.app/c/argo_workflow_sensor)
- [`argo_workflow_trigger` README](https://dagster-component-ui.vercel.app/c/argo_workflow_trigger)
