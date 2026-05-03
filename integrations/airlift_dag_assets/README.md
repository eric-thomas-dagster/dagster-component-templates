# AirliftDagAssets

Wraps `dagster-airlift` (Airflow ↔ Dagster bridge). Imports an Airflow DAG's tasks as Dagster assets, letting you incrementally migrate workflows or run side-by-side. The official path for Airflow→Dagster migrations.

Wraps the official `dagster-airlift` package.

## Example

```yaml
type: dagster_component_templates.AirliftDagAssetsComponent
attributes:
  airflow_url: <fill in>
  auth_user_env_var: <fill in>
  auth_pass_env_var: <fill in>
  dag_id: <fill in>
```

## Requirements

```
dagster
dagster-airlift
```
