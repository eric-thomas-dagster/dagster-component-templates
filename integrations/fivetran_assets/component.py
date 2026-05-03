"""FivetranAssetsComponent.

Wraps `dagster-fivetran`'s `FivetranWorkspace` to materialize one Dagster asset per Fivetran connector. Pair with `fivetran_sync_sensor` (already in the registry) to trigger downstream runs when a sync completes.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class FivetranAssetsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Import each Fivetran connector as a Dagster external asset via dagster-fivetran."""

    account_id: str = Field(description="Fivetran account ID.")
    api_key_env_var: str = Field(default="FIVETRAN_API_KEY", description="Env var with the Fivetran API key.")
    api_secret_env_var: str = Field(default="FIVETRAN_API_SECRET", description="Env var with the Fivetran API secret.")
    connector_filter: Optional[List[str]] = Field(default=None, description="Optional list of connector_ids to include.")
    group_name: str = Field(default="fivetran", description="Asset group.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_fivetran import FivetranWorkspace, build_fivetran_assets_definitions
        workspace = FivetranWorkspace(
            account_id=self.account_id,
            api_key=dg.EnvVar(self.api_key_env_var),
            api_secret=dg.EnvVar(self.api_secret_env_var),
        )
        assets_defs = build_fivetran_assets_definitions(
            workspace=workspace,
            connector_selector_fn=(lambda c: c.id in self.connector_filter) if self.connector_filter else None,
        )
        return dg.Definitions(assets=assets_defs)

