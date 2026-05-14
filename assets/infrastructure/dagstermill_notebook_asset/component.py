"""DagstermillNotebookAssetComponent.

Wraps `dagstermill.define_dagstermill_asset` so a Jupyter notebook becomes a real Dagster asset — papermill-execute the notebook on materialization, capture outputs, surface lineage. Useful for analyses + reports that should be scheduled and tracked.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class DagstermillNotebookAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Jupyter notebook as a Dagster asset via dagstermill."""

    asset_name: str = Field(description="Dagster asset name.")
    notebook_path: str = Field(description="Path to the .ipynb file (relative to project root or absolute).")
    group_name: str = Field(default="notebooks", description="Asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagstermill import define_dagstermill_asset, local_output_notebook_io_manager
        asset_def = define_dagstermill_asset(
            name=self.asset_name,
            notebook_path=self.notebook_path,
            group_name=self.group_name,
            description=self.description,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        return dg.Definitions(
            assets=[asset_def],
            resources={"output_notebook_io_manager": local_output_notebook_io_manager},
        )

