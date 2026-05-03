"""AirflowDagProxyComponent.

Wraps `dagster-airflow.make_dagster_definitions_from_airflow_dag` to lift an in-process Airflow DAG into Dagster — useful when you have legacy DAGs you want to run via Dagster's executor without changing the DAG code. Different from `airlift` which talks to a remote Airflow instance.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class AirflowDagProxyComponent(dg.Component, dg.Model, dg.Resolvable):
    """Convert an Airflow DAG to Dagster definitions via dagster-airflow."""

    dag_module: str = Field(description="Importable module containing the Airflow DAG.")
    dag_id: str = Field(description="DAG ID variable name in that module (or fully-qualified attribute).")
    target: str = Field(default="job", description="'job' (run as Dagster job) or 'asset' (assets per task).")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_airflow import make_dagster_definitions_from_airflow_dag
        import importlib
        module = importlib.import_module(self.dag_module)
        dag = getattr(module, self.dag_id)
        return make_dagster_definitions_from_airflow_dag(dag=dag)

