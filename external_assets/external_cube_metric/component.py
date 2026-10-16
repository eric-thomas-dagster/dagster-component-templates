"""External Cube Metric — declare-only AssetSpec.

Surfaces a Cube semantic-layer measure/dimension as an external asset in the
Dagster catalog. Cube owns the metric definition (in cube.js/cube.py schema);
Dagster represents it for lineage + catalog + downstream `deps:`.

Pair with ``cube_query_asset`` when Dagster should also fetch the values,
or with an ``llm_agent`` when the metric is consumed via LLM-generated queries
against Cube's semantic layer.

Docs: https://cube.dev/docs/product/data-modeling
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


class ExternalCubeMetricAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a Cube metric (measure or dimension) as an external Dagster asset.

    Example:
        ```yaml
        type: dagster_community_components.ExternalCubeMetricAsset
        attributes:
          asset_key: cube/orders/count
          cube_name: Orders
          measure_name: Orders.count
          cube_playground_url: http://localhost:4000/#/build
          group_name: cube
        ```

    For dimensions (grouping fields):
        ```yaml
        attributes:
          asset_key: cube/orders/status
          cube_name: Orders
          dimension_name: Orders.status
        ```
    """

    asset_key: str = Field(description="Dagster asset key, '/'-separated (e.g. 'cube/orders/count').")
    cube_name: str = Field(description="Cube name (e.g. 'Orders', 'Customers').")
    measure_name: Optional[str] = Field(
        default=None,
        description="Cube measure name (e.g. 'Orders.count', 'Orders.totalAmount'). Set this OR dimension_name.",
    )
    dimension_name: Optional[str] = Field(
        default=None,
        description="Cube dimension name (e.g. 'Orders.status'). Set this OR measure_name.",
    )
    metric_type: Optional[str] = Field(
        default=None,
        description="Cube type (count / sum / avg / string / time / number / boolean). Surfaced as metadata.",
    )
    cube_playground_url: Optional[str] = Field(
        default=None,
        description="Cube Playground URL — clickable link in the catalog to inspect the metric interactively.",
    )
    cube_docs_url: Optional[str] = Field(
        default=None,
        description="URL to the metric's docs / schema file (GitHub / internal wiki).",
    )
    group_name: Optional[str] = Field(default="cube", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'cube', 'semantic-layer').",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.measure_name) == bool(self.dimension_name):
            raise ValueError(
                f"external_cube_metric {self.asset_key!r}: set exactly one of "
                f"`measure_name` or `dimension_name`."
            )

        _kinds = set(self.kinds or [])
        _kinds.update({"cube", "semantic-layer"})

        metadata: Dict[str, object] = {"dagster.observability_type": "external"}
        metadata["cube_name"] = self.cube_name
        if self.measure_name:
            metadata["cube_measure"] = self.measure_name
        if self.dimension_name:
            metadata["cube_dimension"] = self.dimension_name
        if self.metric_type:
            metadata["cube_metric_type"] = self.metric_type
        if self.cube_playground_url:
            metadata["cube_playground_url"] = dg.MetadataValue.url(self.cube_playground_url)
        if self.cube_docs_url:
            metadata["cube_docs_url"] = dg.MetadataValue.url(self.cube_docs_url)

        spec = dg.AssetSpec(
            key=dg.AssetKey.from_user_string(self.asset_key),
            group_name=self.group_name,
            description=self.description or (
                f"Cube {'measure' if self.measure_name else 'dimension'} "
                f"{self.measure_name or self.dimension_name} on cube {self.cube_name!r} — "
                f"declared external; fetched via cube_query_asset."
            ),
            kinds=_kinds,
            owners=self.owners,
            tags=self.tags,
            metadata=metadata,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        return dg.Definitions(assets=[spec])
