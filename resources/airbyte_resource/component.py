"""Airbyte Resource component.

Registers a real `dagster_airbyte.AirbyteCloudWorkspace` (the official
dagster-airbyte integration, Airbyte Cloud's REST API, OAuth client-
credentials auth) as a shared resource. `integrations/airbyte_assets`
already uses `AirbyteCloudWorkspace` but builds it inline each time and
never registers it under a resource_key, so there was previously no way
for `airbyte_sync_sensor` to reuse the same workspace/credentials.

Note: this is specifically Airbyte Cloud, not self-hosted/OSS Airbyte.
`airbyte_sync_sensor`'s inline fallback path (airbyte_url + optional basic
auth) targets the OSS Config API (`/api/v1/jobs/list`) instead -- a
different product surface with different auth, not interchangeable with
this resource.
"""
import dagster as dg
from pydantic import Field


class AirbyteResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Airbyte Cloud workspace (dagster-airbyte) for use by other components."""

    resource_key: str = Field(
        default="airbyte_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    workspace_id: str = Field(description="Airbyte Cloud workspace ID.")
    client_id_env_var: str = Field(default="AIRBYTE_CLIENT_ID", description="Env var with the Airbyte client_id.")
    client_secret_env_var: str = Field(default="AIRBYTE_CLIENT_SECRET", description="Env var with the Airbyte client_secret.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Airbyte Resource", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_airbyte import AirbyteCloudWorkspace

        workspace = AirbyteCloudWorkspace(
            workspace_id=self.workspace_id,
            client_id=dg.EnvVar(self.client_id_env_var),
            client_secret=dg.EnvVar(self.client_secret_env_var),
        )
        return dg.Definitions(resources={self.resource_key: workspace})
