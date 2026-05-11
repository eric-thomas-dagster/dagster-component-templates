# Dataform Repository Resource

YAML wrapper around `dagster_dataform.DataformRepositoryResource` (community package). Exposes Dataform compilation, workflow invocation, asset loading, and asset-check loading without writing Python.

```yaml
type: dagster_component_templates.DataformRepositoryResourceComponent
attributes:
  resource_key: dataform_repo
  location: us-central1
  repository_id: my-dataform-repo
  environment: production
```

## What it gives you

Installing this component plus `dagster-dataform` produces a Definitions with:
- `resources.dataform_repo`: full `DataformRepositoryResource` — call `.create_compilation_result()`, `.create_workflow_invocation()`, `.get_workflow_invocation_details()`, etc.
- `assets`: every Dataform table/view auto-loaded as Dagster assets via `load_dataform_assets`
- `asset_checks`: every Dataform assertion auto-loaded as Dagster asset checks

You can then add `dagster_dataform`'s schedule + sensor on top:

```python
from dagster_dataform import (
    create_dataform_orchestration_schedule,
    create_dataform_workflow_invocation_sensor,
)


@dg.definitions
def defs(context: dg.ComponentLoadContext):
    dataform = context.resources["dataform_repo"]
    sched = create_dataform_orchestration_schedule(
        dataform_repository_resource=dataform,
        cron_schedule="0 6 * * *",
    )
    sensor = create_dataform_workflow_invocation_sensor(
        dataform_repository_resource=dataform,
    )
    return dg.Definitions(schedules=[sched], sensors=[sensor])
```

## Auth

Service account needs `roles/dataform.editor` (or `roles/dataform.admin`) on the repository.

## Why a Component shell on top of an existing package?

`dagster-dataform` is the official-style integration (community package, not Dagster Labs but the de facto solution). It ships rich Python APIs but no YAML / Component surface — this component fills that gap so YAML-driven projects can use Dataform without writing Python plumbing.

## Sister components

- `bigquery_query_asset` — run ad-hoc SQL against BigQuery (no Dataform abstraction).
- `bigquery_create_table_from_query_asset` — single CTAS step (lighter than a Dataform workflow for one-shot jobs).
- `composer_airflow_auth_backend` — for the Airflow-on-Composer pattern (very different orchestration model).
