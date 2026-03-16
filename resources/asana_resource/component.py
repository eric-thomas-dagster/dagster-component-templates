"""Asana Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class AsanaResource(ConfigurableResource):
    """Dagster resource wrapping the Asana Python SDK."""

    access_token_env_var: str = Field(
        description="Env var holding Asana personal access token"
    )
    workspace_gid: Optional[str] = Field(
        default=None,
        description="Asana workspace GID",
    )

    def get_client(self):
        import asana

        configuration = asana.Configuration()
        configuration.access_token = os.environ.get(self.access_token_env_var, "")
        return asana.WorkspacesApi(asana.ApiClient(configuration))


@dataclass
class AsanaResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an AsanaResource for use by other components."""

    resource_key: str = Field(
        default="asana_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    access_token_env_var: str = Field(
        description="Env var holding Asana personal access token",
    )
    workspace_gid: Optional[str] = Field(
        default=None,
        description="Asana workspace GID",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = AsanaResource(
            access_token_env_var=self.access_token_env_var,
            workspace_gid=self.workspace_gid,
        )
        return dg.Definitions(resources={self.resource_key: resource})
