"""External Apache Doris table — declare-only AssetSpec.

Surfaces an Apache Doris table (or materialized view) as an external
asset in the Dagster catalog. Use when Doris owns the table's lifecycle
(populated by another system — Flink CDC, Routine Load from Kafka,
external Stream Load consumers) and Dagster's role is catalog presence
+ lineage + downstream orchestration.

Mirrors the shape of every other ``external_<vendor>_table`` component
in the registry: declare-only ``AssetSpec``, no execution, marks
``dagster.observability_type=external``.
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


class ExternalDorisTableAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare an Apache Doris table as an external Dagster asset.

    Example:
        ```yaml
        type: dagster_community_components.ExternalDorisTableAsset
        attributes:
          asset_key: doris/analytics/events_realtime
          database: analytics
          table: events_realtime
          fe_host: doris-fe.mycompany.com
          group_name: doris
          description: |
            Real-time events table populated by Routine Load from Kafka.
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'doris/analytics/events_realtime').")
    database: str = Field(description="Doris database (catalog) name.")
    table: str = Field(description="Doris table name.")
    fe_host: Optional[str] = Field(
        default=None,
        description="Doris Frontend host — surfaced as catalog metadata.",
    )
    table_model: Optional[str] = Field(
        default=None,
        description=(
            "Doris table model: 'duplicate' (default) / 'unique' / 'aggregate' / "
            "'mv' (materialized view). Surfaced as metadata; doesn't affect "
            "the declare-only AssetSpec."
        ),
    )
    group_name: Optional[str] = Field(default="doris", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'doris').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("doris")

        metadata: Dict[str, object] = {
            "dagster.observability_type": "external",
            "doris_database": self.database,
            "doris_table": self.table,
            "doris_qualified_name": f"{self.database}.{self.table}",
        }
        if self.fe_host:
            metadata["doris_fe_host"] = self.fe_host
        if self.table_model:
            metadata["doris_table_model"] = self.table_model

        spec = dg.AssetSpec(
            key=dg.AssetKey.from_user_string(self.asset_key),
            group_name=self.group_name,
            description=self.description or (
                f"Doris table {self.database}.{self.table} — declared "
                f"external; lifecycle owned by Doris."
            ),
            kinds=kinds,
            owners=self.owners,
            tags=self.tags,
            metadata=metadata,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        return dg.Definitions(assets=[spec])
