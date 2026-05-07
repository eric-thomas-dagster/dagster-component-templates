# Fabric Capacity Admin Job

Pause / resume / scale a Microsoft Fabric capacity via the management REST API. Op-job — schedule it nightly + weekends to halve Fabric capacity spend.

## Cost savings

Fabric capacities bill hourly even when idle. F2 is ~$0.21/hr (~$154/mo always-on). Pausing nightly + on weekends roughly halves spend.

Typical schedule pair:

```yaml
# Pause every day at 19:00 (stays paused all weekend)
- type: dagster_component_templates.FabricCapacityAdminJobComponent
  attributes:
    job_name: fabric_capacity_pause_nightly
    action: suspend
    schedule: '0 19 * * *'

# Resume weekday mornings at 06:00
- type: dagster_component_templates.FabricCapacityAdminJobComponent
  attributes:
    job_name: fabric_capacity_resume_morning
    action: resume
    schedule: '0 6 * * 1-5'
```

## Companion components

- `synapse_sql_pool_admin_job` — same pattern for Synapse dedicated SQL pools
- `fabric_pipeline_trigger_job` — trigger ONE Fabric pipeline
- `fabric_workspace` — discover Fabric items as Dagster assets
