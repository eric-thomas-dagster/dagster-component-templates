"""HexProjectAssetsComponent.

Wraps `dagster-hex` to model a Hex project as a Dagster asset — materializing the Dagster asset triggers a Hex project run via the Hex API.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class HexProjectAssetsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Hex project on materialization via dagster-hex."""

    api_key_env_var: str = Field(default="HEX_API_KEY", description="Env var with the Hex API key.")
    project_id: str = Field(description="Hex project UUID to run on materialization.")
    asset_name: str = Field(description="Dagster asset name to expose in the catalog.")
    group_name: str = Field(default="hex", description="Asset group name.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_hex.resources import HexResource
        from dagster_hex.asset_defs import build_hex_asset
        hex_resource = HexResource(api_key=dg.EnvVar(self.api_key_env_var))
        asset = build_hex_asset(
            project_id=self.project_id,
            name=self.asset_name,
            group_name=self.group_name,
        )
        return dg.Definitions(assets=[asset], resources={"hex": hex_resource})

