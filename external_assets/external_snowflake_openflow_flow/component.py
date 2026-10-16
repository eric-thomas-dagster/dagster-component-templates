"""External Snowflake OpenFlow flow — declare-only AssetSpec.

Declares a Snowflake [OpenFlow](https://docs.snowflake.com/en/user-guide/openflow/about)
process group as an external asset. OpenFlow runs the flow on its
own infra (BYOC EKS or Snowflake-managed); Dagster observes the
``SNOWFLAKE.TELEMETRY.EVENTS`` table via ``snowflake_openflow_status_sensor``
and emits ``AssetMaterialization`` on completion.

Pattern matches ``external_precisely_job`` / ``external_huggingface_space``:
external thing runs on its own schedule; Dagster declares + observes.
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


class ExternalSnowflakeOpenflowFlowAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a Snowflake OpenFlow flow as an external Dagster asset.

    Example:
        ```yaml
        type: dagster_community_components.ExternalSnowflakeOpenflowFlowAsset
        attributes:
          asset_key: snowflake/openflow/customer_sync
          flow_name: customer_sync
          runtime_id: my-runtime
          group_name: snowflake
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'snowflake/openflow/customer_sync').")
    flow_name: str = Field(description="OpenFlow process-group name.")
    runtime_id: Optional[str] = Field(default=None, description="OpenFlow runtime id (optional metadata).")
    group_name: Optional[str] = Field(default="snowflake", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'snowflake', 'openflow').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("snowflake")
        kinds.add("openflow")

        metadata: Dict[str, object] = {
            "openflow_flow_name": self.flow_name,
            "dagster.observability_type": "external",
        }
        if self.runtime_id:
            metadata["openflow_runtime_id"] = self.runtime_id

        spec = dg.AssetSpec(
            key=dg.AssetKey.from_user_string(self.asset_key),
            group_name=self.group_name,
            description=self.description or (
                f"Snowflake OpenFlow process group {self.flow_name} — "
                f"declared external; updated via snowflake_openflow_status_sensor."
            ),
            kinds=kinds,
            owners=self.owners,
            tags=self.tags,
            metadata=metadata,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        return dg.Definitions(assets=[spec])
