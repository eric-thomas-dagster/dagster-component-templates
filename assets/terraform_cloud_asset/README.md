# HCP Terraform (Terraform Cloud) Asset

Discovers all workspaces in an HCP Terraform organization at prepare time and creates one Dagster asset per workspace. Materialization triggers a remote run in HCP Terraform and polls for completion, making it straightforward to provision or validate infrastructure before running downstream data pipelines.

## How StateBackedComponent caching works

This component extends `StateBackedComponent`. On first load, Dagster calls `write_state_to_path` once — this is the only moment the HCP Terraform API is contacted. The resulting workspace list is written to a local JSON file (the "state file"). On every subsequent load (code-server reload, `dagster dev` restart, etc.) `build_defs_from_state` reads the state file from disk and constructs asset definitions with zero network calls, keeping startup fast.

Refresh the cached workspace list with:

```bash
dg utils refresh-defs-state   # explicit refresh (CI/CD, image build)
dagster dev                   # automatic refresh in dev mode
```

## HCP Terraform API token setup

The component reads the token from the environment variable named by `api_token_env_var` (default field value: `TFC_TOKEN`).

**User token** — suitable for development. Generate one at *User Settings → Tokens* in the HCP Terraform UI.

**Team token** — recommended for CI/CD and production. Generate one at *Organization → Settings → Teams → {team} → Team API Token*. The team must have at least *Plan* access on every workspace the component will manage.

Export the token before running Dagster:

```bash
export TFC_TOKEN="<your-token>"
```

Or add it to your secrets manager / environment file and reference it via `api_token_env_var`.

## Required packages

```
requests>=2.28.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `api_token_env_var` | Yes | — | Env var containing the HCP Terraform API token |
| `organization` | Yes | — | HCP Terraform organization name |
| `workspace_filter` | No | `None` | Only include workspaces whose name contains this substring |
| `exclude_workspaces` | No | `None` | List of workspace names to exclude |
| `tags` | No | `None` | Only include workspaces tagged with ALL of these tags |
| `group_name` | No | `infrastructure` | Dagster asset group name |
| `key_prefix` | No | `None` | Optional prefix prepended to every asset key |
| `plan_only` | No | `false` | Create plan-only runs — useful for CI drift checks (no apply) |
| `auto_apply` | No | `true` | Set the run's `auto-apply` attribute in HCP Terraform |
| `message` | No | `Triggered by Dagster` | Run message visible in the HCP Terraform UI |
| `poll_interval_seconds` | No | `15` | Seconds between run-status polls |
| `run_timeout_seconds` | No | `3600` | Maximum seconds to wait for a run before raising an error |

## Workspace filtering

Three filters are applied in order (each is optional):

1. **`workspace_filter`** — substring match on the workspace name (e.g. `"data-platform-"` matches `data-platform-prod` and `data-platform-staging`).
2. **`exclude_workspaces`** — explicit deny-list of workspace names.
3. **`tags`** — include only workspaces that carry every listed tag (AND logic).

## Plan-only mode for CI drift checks

Set `plan_only: true` to create non-destructive plan runs. No infrastructure is changed. This is ideal for:

- Drift detection in a scheduled pipeline
- Pull-request checks that validate Terraform plans before merge

```yaml
type: dagster_component_templates.TerraformCloudAssetComponent
attributes:
  api_token_env_var: TFC_TOKEN
  organization: my-company
  plan_only: true
  message: "Drift check from Dagster"
```

## Using as the first step in a pipeline

Because each workspace becomes a standard Dagster asset, downstream assets can declare a dependency on it using `deps`:

```python
@dg.asset(deps=["data_platform_prod"])  # matches the workspace name
def load_raw_events(context):
    # infrastructure is guaranteed to be up-to-date before this runs
    ...
```

Or wire an entire group as an upstream dependency using `AssetSelection` in your job definition:

```python
terraform_job = dg.define_asset_job(
    "provision_then_ingest",
    selection=dg.AssetSelection.groups("infrastructure") | dg.AssetSelection.groups("ingestion"),
)
```

## Example YAML

```yaml
type: dagster_component_templates.TerraformCloudAssetComponent
attributes:
  api_token_env_var: TFC_TOKEN
  organization: my-company
  workspace_filter: "data-platform-"
  group_name: infrastructure
  auto_apply: true
  message: "Triggered by Dagster pipeline"
```
