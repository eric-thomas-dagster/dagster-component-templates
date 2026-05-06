# Synapse SQL Pool Admin Job

Pause / resume / scale a Synapse dedicated SQL pool via the management
REST API. **Op-job, not an asset** — pause/resume produces no data; it's
an operational lifecycle action you schedule to save money.

## Why this is a job, not an asset

Per the registry's pattern (mirrors `adls_cleanup_job`,
`fivetran_sync_trigger_job`, `airbyte_sync_trigger_job`): operations that
don't produce a materialized result belong as **op-jobs**, not assets.
Schedules drive them; assets sit on the data plane.

## Cost savings

Synapse dedicated SQL pools bill DWU-by-the-hour, even when idle. A
DW100c pool costs ~$1.20/hour ($870/month if always on). Pausing
overnight + on weekends roughly halves that.

Typical schedule pair:

```yaml
# Pause: weekday 19:00 + Friday 19:00 (stays paused all weekend)
- type: dagster_component_templates.SynapseSqlPoolAdminJobComponent
  attributes:
    job_name: synapse_pool_pause_nightly
    action: pause
    schedule: "0 19 * * *"

# Resume: weekday 06:00 (Mon-Fri)
- type: dagster_component_templates.SynapseSqlPoolAdminJobComponent
  attributes:
    job_name: synapse_pool_resume_morning
    action: resume
    schedule: "0 6 * * 1-5"
```

## Auth

Standard `DefaultAzureCredential` chain:
1. Environment variables (the `*_env_var` config fields)
2. Managed identity (when running in Azure compute — ACA / AKS / VM)
3. `az login` user (for local development)

The service principal needs `Microsoft.Synapse/workspaces/sqlPools/pause/action`
+ `.../resume/action` (covered by the `Synapse Administrator` role at
the workspace scope, or the `Microsoft.Synapse/sqlPools/Contributor` Azure
RBAC role at the subscription/RG/pool scope).

## API

The component calls:

| Action | Method | Endpoint |
|---|---|---|
| pause | POST | `/sqlPools/{name}/pause` |
| resume | POST | `/sqlPools/{name}/resume` |
| scale | PATCH | `/sqlPools/{name}` (body: `{"sku":{"name":"DW200c"}}`) |

All return 202 Accepted with an `Azure-AsyncOperation` header (the actual
state transition takes ~1-2 minutes). The job returns immediately;
inspect Synapse Studio or `az synapse sql pool show` to verify completion.

## Validation status

This component is **code-validated against the documented Synapse
management REST API spec**, but not e2e-validated against a live dedicated
SQL pool (provisioning a DSP costs $$$ even briefly). The pattern matches
identically-shaped existing components (`fivetran_sync_trigger_job`,
`airbyte_sync_trigger_job`).
