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
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'huggingface').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("huggingface")
        kinds.add("space")

        spec = dg.AssetSpec(
            key=dg.AssetKey(self.asset_key.split("/")),
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
