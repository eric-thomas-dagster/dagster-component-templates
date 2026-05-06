# Fabric Pipeline Trigger Job

Op-job that triggers a Microsoft Fabric Data Pipeline / Notebook /
Dataflow Gen2 run via the Fabric REST API. For workspace-wide imports
(one Dagster asset per Fabric pipeline) use the `fabric_workspace`
integration component instead — use this when you want **one** specific
Fabric item run as a Dagster job, with optional schedule.

## Pattern

```yaml
- type: dagster_component_templates.FabricPipelineTriggerJobComponent
  attributes:
    job_name: fabric_daily_etl
    workspace_id: ...
    item_id: ...                       # the pipeline GUID
    item_type: DataPipeline
    parameters: { date: "{{run_date}}" }
    schedule: "0 2 * * *"
```

## Companion components

- `fabric_workspace` — for workspace-wide import as Dagster assets
- `dataframe_to_fabric_lakehouse` — write DataFrame to a Lakehouse
- `dataframe_to_table` (existing) — write to Fabric Data Warehouse via
  SQL endpoint

## Validation status

Code-validated against the Fabric REST API spec
(https://learn.microsoft.com/rest/api/fabric/core/job-scheduler).
End-to-end validation requires a Fabric capacity + workspace.
