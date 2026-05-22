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
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'snowflake', 'openflow').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

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
            key=dg.AssetKey(self.asset_key.split("/")),
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
