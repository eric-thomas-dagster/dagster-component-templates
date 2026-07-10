# JDEOrchestrationOutputIngestionComponent

Execute a **JDE orchestration** and materialize its row-list output as a Dagster DataFrame. Common pattern: orchestrations that wrap JDE Data Services (table reads) or Business Function calls returning tabular results.

Pairs with `jde_orchestrator_resource`.

## Row extraction

JDE orchestration responses vary — some return flat top-level lists, others nest under `ServiceRequest1.RowSet`, `result`, etc. Use `output_field` (dotted path) to point at the right key. If unset, the component walks the response looking for the first list-of-dicts it can find.

## Example

```yaml
type: dagster_community_components.JDEOrchestrationOutputIngestionComponent
attributes:
  asset_key: open_ap_invoices
  orchestration: JDE_Fetch_Open_APs
  inputs:
    CompanyCode: "00001"
    AsOfDate: "2026-07-10"
  output_field: "ServiceRequest1.RowSet"
  resource_key: jde_orchestrator_resource
```

## Chaining

- `dataframe_to_snowflake` — land JDE data in a warehouse for BI
- `filter` + `summarize` — analytics on JDE data
- `dataframe_to_csv` — export for downstream teams

## Related

- `jde_orchestrator_resource`
- `jde_orchestration_trigger_job` — no-return sync trigger
- `jde_orchestrator_workspace` — workspace shape
