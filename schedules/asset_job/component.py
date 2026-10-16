"""AssetJobComponent.

Creates a job over the configured asset selection so it shows up as a discrete unit in the Dagster UI's Jobs tab. Useful when you want a one-button 'run this subset' experience without committing to a schedule.
"""
from typing import Dict, List, Optional

import dagster as dg
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
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


class AssetJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Define a named Dagster job from a set of asset keys (without scheduling it)."""

    job_name: str = Field(description="Unique job name shown in the Jobs tab.")
    asset_keys: List[str] = Field(description="Slash-separated asset keys included in the job.")
    description: Optional[str] = Field(default=None, description="Description shown in the UI.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Tags applied to runs of this job.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=('tags',))

    @classmethod
    def get_form_config(cls):
        """UI-editable via the Dagster / Dagster+ Components tab
        (dagster>=1.13.8 + `flagComponentInstanceUI`)."""
        from dagster.components.resolved.form_config import ComponentFormConfig

        return ComponentFormConfig(label="Asset Job", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in self.asset_keys]
        job = define_asset_job(
            name=self.job_name,
            selection=AssetSelection.assets(*targets),
            description=self.description,
            tags=self.tags or {},
        )
        return Definitions(jobs=[job])

