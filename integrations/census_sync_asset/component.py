"""CensusSyncAssetComponent.

Wraps `dagster-census` to expose a Census sync as a Dagster asset. Materializing the Dagster asset triggers a Census run that pushes warehouse data to your downstream SaaS.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class CensusSyncAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Census reverse-ETL sync on materialization via dagster-census."""

    api_key_env_var: str = Field(default="CENSUS_API_KEY", description="Env var with the Census API key.")
    sync_id: str = Field(description="Census sync ID to run.")
    asset_name: str = Field(description="Dagster asset name.")
    group_name: str = Field(default="census", description="Asset group.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_census.resources import CensusResource
        from dagster_census.asset_defs import build_census_asset
        census = CensusResource(api_key=dg.EnvVar(self.api_key_env_var))
        asset = build_census_asset(
            sync_id=self.sync_id,
            name=self.asset_name,
            group_name=self.group_name,
        )
        return dg.Definitions(assets=[asset], resources={"census": census})

