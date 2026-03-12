# Azure Data Factory Component

Import Azure Data Factory pipelines and triggers as Dagster assets with fast startup
via **StateBackedComponent** caching and a built-in observation sensor.

## How It Works

`AzureDataFactoryComponent` extends `StateBackedComponent` (dagster>=1.8):

1. **`write_state_to_path`** — called once at prepare time (e.g. `dagster dev` or
   `dg utils refresh-defs-state`). Calls the ADF Management API to list all pipelines
   (and triggers), and writes `{name, description, parameters, activities_count}` JSON
   to a local cache file.
2. **`build_defs_from_state`** — called on every code-server reload. Reads the cached
   JSON and builds one `@dg.asset` per pipeline / trigger with **zero network calls**,
   keeping restarts fast.

If no cache exists yet, `build_defs_from_state` returns empty `Definitions` and logs a
warning. Run `dg utils refresh-defs-state` (or just start `dagster dev`) to populate it.

Dagster <1.8 falls back to the original behaviour: `build_defs` calls the API on every
load.

## Features

- **Pipelines**: Trigger pipeline runs on demand; polls until Succeeded/Failed/Cancelled
- **Triggers**: Start triggers programmatically (no-op if already running)
- **Observation Sensor**: Tracks runs triggered outside Dagster and emits `AssetMaterialization` events
- **Filtering**: Include/exclude by regex name pattern and tag keys
- **Authentication**: DefaultAzureCredential or explicit Service Principal

## Quick Start

```yaml
# defs/azure_data_factory/component.yaml
type: dagster_component_templates.AzureDataFactoryComponent
attributes:
  subscription_id: "12345678-1234-1234-1234-123456789012"
  resource_group_name: my-resource-group
  factory_name: my-data-factory
  tenant_id: "{{ env('AZURE_TENANT_ID') }}"
  client_id: "{{ env('AZURE_CLIENT_ID') }}"
  client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"
  import_pipelines: true
```

Then populate the cache:

```bash
dg utils refresh-defs-state   # CI/CD / Docker image build
# or
dagster dev                    # automatic in development
```

## Configuration Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `subscription_id` | str | **required** | Azure subscription ID |
| `resource_group_name` | str | **required** | Azure resource group name |
| `factory_name` | str | **required** | Azure Data Factory name |
| `tenant_id` | str | `None` | Azure AD tenant ID (omit to use DefaultAzureCredential) |
| `client_id` | str | `None` | Azure AD client/application ID |
| `client_secret` | str | `None` | Azure AD client secret |
| `import_pipelines` | bool | `true` | Import pipelines as materializable assets |
| `import_triggers` | bool | `false` | Import triggers as materializable assets |
| `filter_by_name_pattern` | str | `None` | Regex to include matching entity names |
| `exclude_name_pattern` | str | `None` | Regex to exclude matching entity names |
| `filter_by_tags` | str | `None` | Comma-separated tag keys entities must have |
| `generate_sensor` | bool | `true` | Generate observation sensor |
| `poll_interval_seconds` | int | `60` | Sensor minimum poll interval |
| `group_name` | str | `azure_data_factory` | Dagster asset group name |
| `description` | str | `None` | Component description |
| `defs_state` | object | local filesystem | StateBackedComponent cache location |

### Advanced Example

```yaml
type: dagster_component_templates.AzureDataFactoryComponent
attributes:
  subscription_id: "12345678-1234-1234-1234-123456789012"
  resource_group_name: my-resource-group
  factory_name: my-data-factory
  tenant_id: "{{ env('AZURE_TENANT_ID') }}"
  client_id: "{{ env('AZURE_CLIENT_ID') }}"
  client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"

  import_pipelines: true
  import_triggers: true

  # Only include production entities
  filter_by_name_pattern: ^prod_.*
  exclude_name_pattern: test|dev

  generate_sensor: true
  poll_interval_seconds: 60

  group_name: adf_workspace
  description: Production Azure Data Factory integration
```

## Authentication

Three options (all ultimately resolved via the `azure-identity` library):

1. **DefaultAzureCredential** (recommended) — omit `tenant_id`, `client_id`, `client_secret`.
   Works with Azure CLI (`az login`), Managed Identity, and the `AZURE_*` env vars.

2. **Service Principal** — provide `tenant_id`, `client_id`, `client_secret` as env-var
   references (`{{ env('...') }}`).

3. **Environment variables** — set `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
   `AZURE_CLIENT_SECRET`; picked up automatically by `DefaultAzureCredential`.

## Asset Execution

### Pipeline Assets (`adf_pipeline_<name>`)

When materialised:
1. Calls `client.pipelines.create_run()` — returns a `run_id`.
2. Polls `client.pipeline_runs.get()` every 30 seconds (up to 60 minutes).
3. On success, returns `MaterializeResult` with `run_id`, `status`, `start_time`,
   `end_time`, `duration_seconds`.
4. On failure, raises an exception with the ADF error message.

### Trigger Assets (`adf_trigger_<name>`)

When materialised:
1. Calls `client.triggers.get()` to check runtime state.
2. If not already `Started`, calls `client.triggers.begin_start().result()`.
3. Returns `MaterializeResult` with `trigger_name`, `runtime_state`, `trigger_type`.

## Observation Sensor

The `<group_name>_observation_sensor` polls `pipeline_runs.query_by_factory()` since the
last cursor timestamp and emits `AssetMaterialization` events for runs triggered outside
Dagster. This enables:

- Downstream dependencies on ADF pipeline results
- Historical tracking in the Dagster UI
- Alerting on pipeline failures

## Required Azure Permissions

Assign the **Data Factory Contributor** role (or the individual actions below):

- `Microsoft.DataFactory/factories/pipelines/read`
- `Microsoft.DataFactory/factories/pipelines/createRun/action`
- `Microsoft.DataFactory/factories/triggers/read`
- `Microsoft.DataFactory/factories/triggers/start/action`
- `Microsoft.DataFactory/factories/pipelineruns/read`
- `Microsoft.DataFactory/factories/pipelineruns/queryByFactory/action`
- `Microsoft.DataFactory/factories/triggerruns/queryByFactory/action`

## Troubleshooting

**Authentication errors** — verify service principal permissions, check env vars, test
with `az login`.

**Cache not populated** — run `dg utils refresh-defs-state` or `dagster dev`.

**Sensor not detecting runs** — confirm `generate_sensor: true` and that pipeline names
match any configured filters.

**Pipeline timeout** — the default wait is 60 minutes. Very long pipelines should be
monitored via the observation sensor instead of synchronous execution.

## Resources

- [Azure Data Factory Documentation](https://learn.microsoft.com/en-us/azure/data-factory/)
- [Python SDK Reference](https://learn.microsoft.com/en-us/python/api/overview/azure/data-factory)
- [Pipeline Execution and Triggers](https://learn.microsoft.com/en-us/azure/data-factory/concepts-pipeline-execution-triggers)
- [Programmatic Monitoring](https://learn.microsoft.com/en-us/azure/data-factory/monitor-programmatically)
