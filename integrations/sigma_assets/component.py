"""SigmaAssetsComponent.

Wraps `dagster-sigma`'s `SigmaOrganization` to surface Sigma workbooks and dataset definitions as Dagster external assets — they show up in the asset graph alongside their upstream warehouse tables, giving you BI-tier lineage.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class SigmaAssetsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Import Sigma workbooks + datasets as Dagster external assets via dagster-sigma."""

    base_url: str = Field(description="Sigma base URL (e.g. 'https://aws-api.sigmacomputing.com').")
    client_id_env_var: str = Field(default="SIGMA_CLIENT_ID", description="Env var with the Sigma client_id.")
    client_secret_env_var: str = Field(default="SIGMA_CLIENT_SECRET", description="Env var with the Sigma client_secret.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_sigma import SigmaOrganization, SigmaBaseUrl, load_sigma_asset_specs
        org = SigmaOrganization(
            base_url=self.base_url,
            client_id=dg.EnvVar(self.client_id_env_var),
            client_secret=dg.EnvVar(self.client_secret_env_var),
        )
        specs = load_sigma_asset_specs(org)
        return dg.Definitions(assets=specs)

