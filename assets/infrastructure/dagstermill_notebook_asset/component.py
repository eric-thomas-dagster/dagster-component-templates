"""DagstermillNotebookAssetComponent.

Wraps `dagstermill.define_dagstermill_asset` so a Jupyter notebook becomes a real Dagster asset — papermill-execute the notebook on materialization, capture outputs, surface lineage. Useful for analyses + reports that should be scheduled and tracked.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class DagstermillNotebookAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Jupyter notebook as a Dagster asset via dagstermill."""

    asset_name: str = Field(description="Dagster asset name.")
    notebook_path: str = Field(description="Path to the .ipynb file (relative to project root or absolute).")
    group_name: str = Field(default="notebooks", description="Asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps',), csv=(), tags=())

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

