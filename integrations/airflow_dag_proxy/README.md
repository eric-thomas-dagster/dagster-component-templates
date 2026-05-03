# AirflowDagProxy

Wraps `dagster-airflow.make_dagster_definitions_from_airflow_dag` to lift an in-process Airflow DAG into Dagster — useful when you have legacy DAGs you want to run via Dagster's executor without changing the DAG code. Different from `airlift` which talks to a remote Airflow instance.

Wraps the official `dagster-airflow` package.

## Example

```yaml
type: dagster_component_templates.AirflowDagProxyComponent
attributes:
  dag_module: <fill in>
  dag_id: <fill in>
  target: <fill in>
```

## Requirements

```
dagster
dagster-airflow
```
