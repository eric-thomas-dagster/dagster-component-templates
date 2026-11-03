"""AssetJobComponent.

Creates a job over the configured asset and/or asset check selection so it shows up as a discrete unit in the Dagster UI's Jobs tab. Useful when you want a one-button 'run this subset' experience without committing to a schedule.
"""
from typing import Dict, List, Optional

import dagster as dg
from dagster import (
    AssetCheckKey,
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
    """Define a named Dagster job from a set of asset keys and/or asset check keys (without scheduling it)."""

    job_name: str = Field(description="Unique job name shown in the Jobs tab.")
    asset_keys: Optional[List[str]] = Field(
        default=None,
        description="Slash-separated asset keys included in the job. Set this, asset_check_keys, or both.",
    )
    asset_check_keys: Optional[List[str]] = Field(
        default=None,
        description=(
            "Asset check keys included in the job, as '<asset_key>:<check_name>' "
            "(asset key segments slash-separated, e.g. 'marts/orders:row_count_check'). "
            "Runs just the check -- via Dagster's real AssetSelection.checks(...), "
            "confirmed to execute with zero re-materialization of the parent asset "
            "-- for a one-button 'just re-run this check' job. Combine with "
            "asset_keys to also materialize assets in the same job."
        ),
    )
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

    @staticmethod
    def _parse_check_key(raw: str) -> AssetCheckKey:
        """Parse '<asset_key>:<check_name>' (asset key segments slash-separated)
        into a real AssetCheckKey. Splits on the LAST colon so a check name
        can't accidentally swallow part of the asset key."""
        asset_part, sep, check_name = raw.rpartition(":")
        if not sep or not asset_part or not check_name:
            raise ValueError(
                f"asset_check_keys entry {raw!r} must be '<asset_key>:<check_name>', "
                f"e.g. 'marts/orders:row_count_check'."
            )
        return AssetCheckKey(AssetKey.from_user_string(asset_part), check_name)

    def build_defs(self, context: dg.ComponentLoadContext) -> Definitions:
        _self = self
        targets = [AssetKey.from_user_string(k) for k in (self.asset_keys or [])]
        selection = AssetSelection.assets(*targets) if targets else None
        if self.asset_check_keys:
            check_keys = [self._parse_check_key(k) for k in self.asset_check_keys]
            check_selection = AssetSelection.checks(*check_keys)
            selection = check_selection if selection is None else selection | check_selection
        if selection is None:
            raise ValueError("Set at least one of asset_keys or asset_check_keys.")
        job = define_asset_job(
            name=self.job_name,
            selection=selection,
            description=self.description,
            tags=self.tags or {},
        )
        return Definitions(jobs=[job])

