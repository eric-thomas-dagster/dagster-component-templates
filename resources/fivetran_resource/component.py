"""Fivetran Resource component.

Registers a real `dagster_fivetran.FivetranWorkspace` (the official
dagster-fivetran integration) as a shared resource. `integrations/
fivetran_assets` already uses `FivetranWorkspace` but builds it inline
each time and never registers it under a resource_key, so there was
previously no way for `fivetran_sync_sensor` to reuse the same
workspace/credentials.
"""
import dagster as dg
from pydantic import Field


class FivetranResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Fivetran workspace (dagster-fivetran) for use by other components."""

    resource_key: str = Field(
        default="fivetran_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    account_id: str = Field(description="Fivetran account ID.")
    api_key_env_var: str = Field(default="FIVETRAN_API_KEY", description="Env var with the Fivetran API key.")
    api_secret_env_var: str = Field(default="FIVETRAN_API_SECRET", description="Env var with the Fivetran API secret.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Fivetran Resource", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_fivetran import FivetranWorkspace

        workspace = FivetranWorkspace(
            account_id=self.account_id,
            api_key=dg.EnvVar(self.api_key_env_var),
            api_secret=dg.EnvVar(self.api_secret_env_var),
        )
        return dg.Definitions(resources={self.resource_key: workspace})
