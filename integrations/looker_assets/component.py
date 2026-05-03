"""LookerAssetsComponent.

Wraps `dagster-looker`'s LookerResource to surface Looker explores, dashboards, and looks as Dagster external assets. Combine with a warehouse asset graph for full lineage from raw tables → LookML → dashboards.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class LookerAssetsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Import Looker LookML views + dashboards as Dagster external assets via dagster-looker."""

    base_url: str = Field(description="Looker instance base URL (e.g. 'https://mycompany.looker.com').")
    client_id_env_var: str = Field(default="LOOKER_CLIENT_ID", description="Env var with API3 client_id.")
    client_secret_env_var: str = Field(default="LOOKER_CLIENT_SECRET", description="Env var with API3 client_secret.")
    project: Optional[str] = Field(default=None, description="Optional LookML project filter.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_looker import LookerResource, build_looker_asset_specs
        looker = LookerResource(
            base_url=self.base_url,
            client_id=dg.EnvVar(self.client_id_env_var),
            client_secret=dg.EnvVar(self.client_secret_env_var),
        )
        specs = build_looker_asset_specs(looker_resource=looker, project=self.project)
        return dg.Definitions(assets=specs)

