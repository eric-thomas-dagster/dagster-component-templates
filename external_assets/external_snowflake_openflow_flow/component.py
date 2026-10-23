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

    asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster asset key (e.g. 'snowflake/openflow/customer_sync') for a single flow. "
            "Mutually exclusive with `flows:` -- use this (with `flow_name:`) for one flow, or "
            "`flows:` to declare several from one component instance."
        ),
    )
    flow_name: Optional[str] = Field(default=None, description="OpenFlow process-group name for a single flow. Mutually exclusive with `flows:`.")
    flows: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Declare several OpenFlow process groups as external assets from one component "
            "instance, instead of one block per flow. Each entry: `key` (Dagster asset key, "
            "required), `flow_name` (OpenFlow process-group name, required), plus optional "
            "`runtime_id`, `deps`, `owners`, `tags`, `kinds`, `group_name`, `description`. "
            "Mutually exclusive with the top-level `asset_key`/`flow_name` fields."
        ),
    )
    runtime_id: Optional[str] = Field(default=None, description="OpenFlow runtime id (optional metadata). Single-flow mode only.")
    group_name: Optional[str] = Field(default="snowflake", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description. Single-flow mode only.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners. Single-flow mode only.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags. Single-flow mode only.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'snowflake', 'openflow'). Single-flow mode only.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys. Single-flow mode only.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('tags',))

    def _resolve_flow_entries(self) -> List[Dict[str, Any]]:
        has_single = bool(self.asset_key or self.flow_name)
        has_multi = bool(self.flows)
        if has_single and has_multi:
            raise ValueError("Set either asset_key/flow_name (single flow) or flows: (several flows), not both.")
        if not has_single and not has_multi:
            raise ValueError("Set either asset_key/flow_name (single flow) or flows: (several flows).")

        if has_single:
            if not self.asset_key or not self.flow_name:
                raise ValueError("Both asset_key and flow_name are required in single-flow mode.")
            return [{
                "key": self.asset_key,
                "flow_name": self.flow_name,
                "runtime_id": self.runtime_id,
                "deps": self.deps,
                "owners": self.owners,
                "tags": self.tags,
                "kinds": self.kinds,
                "group_name": self.group_name,
                "description": self.description,
            }]

        entries = []
        seen_keys = set()
        for i, raw in enumerate(self.flows):
            if not isinstance(raw, dict):
                raise ValueError(f"flows[{i}] must be a mapping, got {type(raw).__name__}")
            key = raw.get("key")
            flow_name = raw.get("flow_name")
            if not key:
                raise ValueError(f"flows[{i}] is missing required field 'key'")
            if not flow_name:
                raise ValueError(f"flows[{i}] ({key!r}) is missing required field 'flow_name'")
            if key in seen_keys:
                raise ValueError(f"flows: duplicate key {key!r}")
            seen_keys.add(key)
            entries.append({
                "key": key,
                "flow_name": flow_name,
                "runtime_id": raw.get("runtime_id"),
                "deps": raw.get("deps"),
                "owners": raw.get("owners"),
                "tags": raw.get("tags"),
                "kinds": raw.get("kinds"),
                "group_name": raw.get("group_name", self.group_name),
                "description": raw.get("description"),
            })
        return entries

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        entries = self._resolve_flow_entries()
        specs = []
        for e in entries:
            kinds = set(e["kinds"]) if e["kinds"] else set()
            kinds.add("snowflake")
            kinds.add("openflow")

            metadata: Dict[str, object] = {
                "openflow_flow_name": e["flow_name"],
                "dagster.observability_type": "external",
            }
            if e["runtime_id"]:
                metadata["openflow_runtime_id"] = e["runtime_id"]

            specs.append(dg.AssetSpec(
                key=dg.AssetKey.from_user_string(e["key"]),
                group_name=e["group_name"],
                description=e["description"] or (
                    f"Snowflake OpenFlow process group {e['flow_name']} — "
                    f"declared external; updated via snowflake_openflow_status_sensor."
                ),
                kinds=kinds,
                owners=e["owners"],
                tags=e["tags"],
                metadata=metadata,
                deps=[dg.AssetKey.from_user_string(k) for k in (e["deps"] or [])],
            ))
        return dg.Definitions(assets=specs)
