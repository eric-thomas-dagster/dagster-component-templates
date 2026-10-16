"""External StarRocks table — declare-only AssetSpec.

Surfaces a StarRocks table as an external asset in the Dagster catalog.
Use when StarRocks owns the table's lifecycle (Routine Load from Kafka,
Flink CDC, external Stream Load consumers, etc.) and Dagster's role is
catalog presence + lineage.

Mirror of ``external_doris_table`` — StarRocks is a Doris fork.
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


class ExternalStarRocksTableAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a StarRocks table as an external Dagster asset.

    Example:
        ```yaml
        type: dagster_community_components.ExternalStarRocksTableAsset
        attributes:
          asset_key: starrocks/analytics/events_realtime
          database: analytics
          table: events_realtime
          fe_host: starrocks-fe.mycompany.com
          group_name: starrocks
        ```
    """

    asset_key: str = Field(description="Dagster asset key.")
    database: str = Field(description="StarRocks database (catalog) name.")
    table: str = Field(description="StarRocks table name.")
    fe_host: Optional[str] = Field(default=None, description="StarRocks Frontend host (metadata only).")
    table_model: Optional[str] = Field(
        default=None,
        description="StarRocks table model: 'duplicate' / 'unique' / 'aggregate' / 'primary' / 'mv'.",
    )
    group_name: Optional[str] = Field(default="starrocks", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("starrocks")

        metadata: Dict[str, object] = {
            "dagster.observability_type": "external",
            "starrocks_database": self.database,
            "starrocks_table": self.table,
            "starrocks_qualified_name": f"{self.database}.{self.table}",
        }
        if self.fe_host:
            metadata["starrocks_fe_host"] = self.fe_host
        if self.table_model:
            metadata["starrocks_table_model"] = self.table_model

        spec = dg.AssetSpec(
            key=dg.AssetKey.from_user_string(self.asset_key),
            group_name=self.group_name,
            description=self.description or (
                f"StarRocks table {self.database}.{self.table} — declared external."
            ),
            kinds=kinds,
            owners=self.owners,
            tags=self.tags,
            metadata=metadata,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        return dg.Definitions(assets=[spec])
