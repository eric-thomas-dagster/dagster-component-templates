"""Asana Resource component."""
from typing import Optional
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
        configuration.access_token = dg.EnvVar(self.access_token_env_var)
        return asana.WorkspacesApi(asana.ApiClient(configuration))

    def get_tasks_api(self):
        import asana

        configuration = asana.Configuration()
        configuration.access_token = dg.EnvVar(self.access_token_env_var)
        return asana.TasksApi(asana.ApiClient(configuration))

    def create_task(self, name: str, notes: str = None, projects: list = None, custom_fields: dict = None, assignee: str = None) -> dict:
        """Create an Asana task. Asana has no upsert concept for tasks --
        this always creates a new task. Use this for "spin up a task per
        row" reverse-ETL patterns (e.g. one task per data-quality failure),
        not for mirroring a table that should stay in sync over time.
        """
        body: dict = {"data": {"name": name}}
        if notes is not None:
            body["data"]["notes"] = notes
        if projects:
            body["data"]["projects"] = projects
        if custom_fields:
            body["data"]["custom_fields"] = custom_fields
        if assignee:
            body["data"]["assignee"] = assignee
        if self.workspace_gid and not projects:
            body["data"]["workspace"] = self.workspace_gid
        return self.get_tasks_api().create_task(body, {})


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
