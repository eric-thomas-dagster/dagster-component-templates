"""External HuggingFace Space — declare-only AssetSpec.

Surfaces a [HuggingFace Space](https://huggingface.co/spaces) as an
external asset in the Dagster catalog. The Space lives on HF's infra
(or on a customer's HF-hosted dedicated Space hardware); Dagster
observes its lifecycle via the paired ``huggingface_space_status_sensor``.

Pattern:

  1. Declare the Space as an external asset with this component
  2. Pair with ``huggingface_space_status_sensor`` (same ``asset_key``).
     When the sensor sees the Space hit a target stage (e.g. RUNNING
     after a rebuild), it emits ``AssetMaterialization(asset_key=...)``
     — lighting up this asset's materialization history.
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


class ExternalHuggingfaceSpaceAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a HuggingFace Space as an external Dagster asset.

    Example:
        ```yaml
        type: dagster_community_components.ExternalHuggingfaceSpaceAsset
        attributes:
          asset_key: hf/spaces/my_app
          space_id: my-org/my-app
          group_name: huggingface
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'hf/spaces/my_app').")
    space_id: str = Field(description="HuggingFace Space id (e.g. 'gradio/hello_world').")
    group_name: Optional[str] = Field(default="huggingface", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'huggingface').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("huggingface")
        kinds.add("space")

        spec = dg.AssetSpec(
            key=dg.AssetKey.from_user_string(self.asset_key),
            group_name=self.group_name,
            description=self.description or (
                f"HuggingFace Space {self.space_id} — declared external; "
                f"updated via huggingface_space_status_sensor."
            ),
            kinds=kinds,
            owners=self.owners,
            tags=self.tags,
            metadata={
                "huggingface_space_id": self.space_id,
                "hub_url": dg.MetadataValue.url(f"https://huggingface.co/spaces/{self.space_id}"),
                "dagster.observability_type": "external",
            },
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        return dg.Definitions(assets=[spec])
