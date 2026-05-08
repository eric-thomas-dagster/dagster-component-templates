"""External Snowflake Table Asset Component."""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _build_partitions_def(
    partition_type: Optional[str],
    partition_start: Optional[str],
    partition_values: Optional[str],
    dynamic_partition_name: Optional[str],
    partition_dimensions: Optional[List[Dict[str, Any]]],
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Two paths:
      1. Flat-field shape (`partition_type` + sibling fields). Covers the
         single-axis cases including `daily/weekly/monthly/hourly/static/dynamic`
         and the legacy `multi` shape ((date, static_dim)) where both dimensions
         are inferred from the flat fields.
      2. `partition_dimensions` (list of dim specs). When set, overrides the
         flat fields — gives you full multi-axis control including
         (tenant, date), (static, static), (dynamic, date), etc.

    Each dim spec is a dict:
        {"name": str,
         "type": "daily"|"weekly"|"monthly"|"hourly"|"static"|"dynamic",
         # type-specific:
         "start": "YYYY-MM-DD",                # time-based types
         "values": ["a", "b", ...],            # static
         "dynamic_partition_name": "tenants",  # dynamic
        }
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    def _build_axis(spec: Dict[str, Any]):
        t = spec.get("type")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition dimension requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    # Path 2: explicit multi-axis spec
    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    # Path 1: flat fields
    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "static":
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name "
                "(e.g. 'tenants') — this becomes the name= on the "
                "DynamicPartitionsDefinition that runtime tooling uses to "
                "register/read keys."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        # legacy shape: (date Daily) × (static_dim Static). For richer
        # multi-axis combinations, use partition_dimensions instead.
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start or "2024-01-01"),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class ExternalSnowflakeTableAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a Snowflake table as an observable external asset.

    Supports partitioning — useful when the Snowflake table is itself
    partitioned (date-partitioned tables, multi-tenant warehouses) and
    downstream Dagster assets need to materialize one partition at a time.

    Example (multi-tenant):
        ```yaml
        type: dagster_component_templates.ExternalSnowflakeTableAsset
        attributes:
          asset_key: snowflake/raw/tenant_orders
          account: myorg-us-east-1
          database: RAW
          schema_name: PUBLIC
          table_name: TENANT_ORDERS
          partition_type: dynamic
          dynamic_partition_name: tenants
          group_name: snowflake_sources
        ```
    """
    asset_key: str = Field(description="Dagster asset key")
    account: str = Field(description="Snowflake account identifier (e.g. myorg-us-east-1)")
    database: str = Field(description="Snowflake database name")
    schema_name: str = Field(description="Snowflake schema name")
    table_name: str = Field(description="Snowflake table name")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    # ── Partition fields (canonical shape across the registry) ──────────
    partition_type: Optional[str] = Field(
        default=None,
        description=(
            "Partition type: 'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | "
            "'dynamic' | 'multi' (legacy date×static), or None for unpartitioned. "
            "For richer multi-axis combinations, use partition_dimensions."
        ),
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO start date for time-based partition types (e.g. '2024-01-01'). Required for daily/weekly/monthly/hourly.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static / multi partition types (e.g. 'us,eu,apac').",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description=(
            "Name argument for DynamicPartitionsDefinition (when partition_type='dynamic'). "
            "Runtime tooling uses this to register/read keys, e.g. 'tenants'."
        ),
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Multi-axis partition spec. List of dim dicts: "
            "[{name, type: daily|weekly|monthly|hourly|static|dynamic, start, values, dynamic_partition_name}]. "
            "Overrides the flat fields when set. Use this for (tenant, date), "
            "(static, static), (dynamic, date), etc."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Snowflake {self.database}.{self.schema_name}.{self.table_name}",
            kinds={"snowflake", "sql", "table"},
            metadata={
                "account": self.account,
                "database": self.database,
                "schema": self.schema_name,
                "table": self.table_name,
                "dagster/uri": f"snowflake://{self.account}/{self.database}/{self.schema_name}/{self.table_name}",
                "dagster.observability_type": "external",
            },
            partitions_def=partitions_def,
        )
        return dg.Definitions(assets=[spec])
