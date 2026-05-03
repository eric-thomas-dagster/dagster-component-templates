"""TableauAssetsComponent.

Wraps `dagster-tableau`'s workspace clients to materialize Tableau workbooks, dashboards, and sheets as Dagster external assets — full lineage from warehouse → semantic layer → Tableau dashboards.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class TableauAssetsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Import Tableau workbooks + sheets as Dagster external assets via dagster-tableau."""

    server_url: str = Field(description="Tableau server URL (e.g. 'https://10ay.online.tableau.com').")
    site_name: str = Field(description="Tableau site name.")
    token_name_env_var: str = Field(default="TABLEAU_TOKEN_NAME", description="Env var with PAT name.")
    token_value_env_var: str = Field(default="TABLEAU_TOKEN_VALUE", description="Env var with PAT value.")
    online: bool = Field(default=True, description="Tableau Online (vs Server).")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_tableau import TableauCloudWorkspace, TableauServerWorkspace, load_tableau_asset_specs
        ws_cls = TableauCloudWorkspace if self.online else TableauServerWorkspace
        ws = ws_cls(
            connected_app_client_id="",
            connected_app_secret_id="",
            connected_app_secret_value="",
            username="",
            site_name=self.site_name,
            pod_name=self.server_url,
        )
        specs = load_tableau_asset_specs(ws)
        return dg.Definitions(assets=specs)

