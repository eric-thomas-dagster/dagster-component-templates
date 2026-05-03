"""AirliftDagAssetsComponent.

Wraps `dagster-airlift` (Airflow ↔ Dagster bridge). Imports an Airflow DAG's tasks as Dagster assets, letting you incrementally migrate workflows or run side-by-side. The official path for Airflow→Dagster migrations.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class AirliftDagAssetsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Mirror an Airflow DAG as Dagster assets via dagster-airlift."""

    airflow_url: str = Field(description="Airflow webserver URL.")
    auth_user_env_var: str = Field(default="AIRFLOW_USERNAME", description="Env var with Airflow username.")
    auth_pass_env_var: str = Field(default="AIRFLOW_PASSWORD", description="Env var with Airflow password.")
    dag_id: str = Field(description="Airflow DAG ID to mirror.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_airlift.core import AirflowInstance, BasicAuthBackend, build_defs_from_airflow_instance
        airflow_instance = AirflowInstance(
            auth_backend=BasicAuthBackend(
                webserver_url=self.airflow_url,
                username=dg.EnvVar(self.auth_user_env_var).get_value() or "",
                password=dg.EnvVar(self.auth_pass_env_var).get_value() or "",
            ),
            name="airflow",
        )
        return build_defs_from_airflow_instance(airflow_instance=airflow_instance)

