"""Databricks Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class DatabricksResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Databricks resource for use by other components."""

    resource_key: str = Field(default="databricks_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(description="Databricks workspace URL, e.g. 'https://adb-1234567890.azuredatabricks.net'")
    token_env_var: str = Field(description="Environment variable holding the Databricks personal access token")
    cluster_id: Optional[str] = Field(default=None, description="Default cluster ID for job submissions")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_databricks import DatabricksClientResource
        resource = DatabricksClientResource(
            host=self.host,
            token=dg.EnvVar(self.token_env_var),
            cluster_id=self.cluster_id,
        )
        return dg.Definitions(resources={self.resource_key: resource})
