# Microsoft Fabric Workspace

Imports a Microsoft Fabric workspace's items as Dagster external assets.
Mirrors `azure_synapse` / `azure_data_factory` in shape but uses the
**Fabric REST API** (not Synapse SDKs) since Fabric is a separate product
family with a different API surface.

## What gets imported

| Fabric Item Type | As Dagster | Materialize?  |
|---|---|---|
| Lakehouse | external/observable asset | No (lineage-only) |
| Warehouse | external/observable asset | No (lineage-only) |
| Notebook | trigger asset | Yes — runs the notebook |
| DataPipeline | trigger asset | Yes — runs the pipeline |
| Dataflow | trigger asset | Yes — refreshes the dataflow |
| SemanticModel | external asset | No |
| Report | external asset | No |

## Auth

Standard `DefaultAzureCredential` chain. Service principal needs to be
added as a **Contributor** on the Fabric workspace. To grant via portal:
Workspace → Manage access → Add SP UPN → Member or Admin.

## API

The component calls:
- `GET /v1/workspaces/{workspace_id}/items` — list items
- `POST /v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType={Pipeline|RunNotebook|Refresh}` — trigger a run
- `GET <Location>` — poll job status

Reference: https://learn.microsoft.com/rest/api/fabric/

## Companion components

- `dataframe_to_fabric_lakehouse` — write DataFrame to a Fabric Lakehouse
  Delta table on OneLake (auto-registers in the SQL endpoint)
- `fabric_pipeline_trigger_job` — op-job to fire one specific Fabric run
  on a schedule (when you don't want full-workspace import)
- `dataframe_to_table` (existing) — write to Fabric Data Warehouse via
  the SQL endpoint (`mssql+pyodbc://<id>.datawarehouse.fabric.microsoft.com`)

## Validation status

This component is **code-validated against the Fabric REST API spec**.
End-to-end validation requires a Fabric capacity (F2 minimum, ~$0.21/hr
pay-as-you-go) AND tenant-level Fabric enablement in Power BI Admin
Portal — which often requires a tenant admin. Ship in your environment
once Fabric is enabled in your tenant.

## Provisioning a Fabric capacity (one-time)

```bash
az provider register --namespace Microsoft.Fabric --wait
az fabric capacity create -g my-rg -n my-fabric -l eastus \
    --sku '{"name":"F2","tier":"Fabric"}' \
    --administration "{\"members\":[\"admin@yourtenant.com\"]}"
```

If the create returns `Unauthorized` ↦ Fabric tenant settings haven't
been enabled. A Power BI tenant admin needs to:
1. Go to admin.powerbi.com → Tenant settings
2. Enable "Fabric capacity admins" group
3. Add yourself or your SP

## Cost

| Capacity SKU | $/hour | Notes |
|---|---|---|
| F2 | ~$0.21/hr (~$154/mo) | Smallest. Good for dev / small workloads. |
| F4 | ~$0.42/hr | |
| F8 | ~$0.83/hr | |
| Trial | $0 | 60-day trial; check Power BI portal for activation |

Pause + resume the capacity to control cost — same pattern as Synapse.
