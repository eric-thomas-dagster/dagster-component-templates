"""AirbyteAssetsComponent.

Wraps `dagster-airbyte`'s `load_assets_from_airbyte_instance` (or `AirbyteCloudWorkspace`) to materialize one Dagster asset per Airbyte connection. Pair with `airbyte_sync_sensor` (already in the registry) to trigger Dagster runs when an Airbyte sync completes.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class AirbyteAssetsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Import each Airbyte connection as a Dagster external asset via dagster-airbyte."""

    workspace_id: str = Field(description="Airbyte Cloud workspace ID.")
    client_id_env_var: str = Field(default="AIRBYTE_CLIENT_ID", description="Env var with the Airbyte client_id.")
    client_secret_env_var: str = Field(default="AIRBYTE_CLIENT_SECRET", description="Env var with the Airbyte client_secret.")
    connection_filter: Optional[List[str]] = Field(default=None, description="Optional list of connection_ids to include.")
    group_name: str = Field(default="airbyte", description="Asset group for the imported connections.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_airbyte import AirbyteCloudWorkspace, build_airbyte_assets_definitions
        workspace = AirbyteCloudWorkspace(
            workspace_id=self.workspace_id,
            client_id=dg.EnvVar(self.client_id_env_var),
            client_secret=dg.EnvVar(self.client_secret_env_var),
        )
        assets_defs = build_airbyte_assets_definitions(
            workspace=workspace,
            connection_selector_fn=(lambda c: c.id in self.connection_filter) if self.connection_filter else None,
        )
        return dg.Definitions(assets=assets_defs)

