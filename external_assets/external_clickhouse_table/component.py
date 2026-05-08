"""External ClickHouse Table Component.

Declares a ClickHouse table as an observable external asset in Dagster.
Includes a ClickHouseResource for reuse across components.

Use alongside clickhouse_table_observation_sensor for continuous health monitoring.
"""
from typing import Any, Dict, List, Optional
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class ClickHouseResource(ConfigurableResource):
    """Resource for connecting to ClickHouse.

    Example (dagster.yaml or Definitions):
        ```python
        ClickHouseResource(
            host=EnvVar("CLICKHOUSE_HOST"),
            password=EnvVar("CLICKHOUSE_PASSWORD"),
        )
        ```
    """

    host: str = Field(description="ClickHouse host")
    port: int = Field(default=8443, description="ClickHouse port (8443 for HTTPS, 8123 for HTTP)")
    username: str = Field(default="default", description="ClickHouse username")
    password: str = Field(default="", description="ClickHouse password")
    secure: bool = Field(default=True, description="Use HTTPS (recommended)")

    def get_client(self):
        """Return a clickhouse_connect client."""
        import clickhouse_connect
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            secure=self.secure,
        )

    def query_value(self, sql: str):
        """Execute a scalar query and return the result."""
        client = self.get_client()
        return client.command(sql)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    def _build_axis(spec):
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

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

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
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start or "2024-01-01"),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class ExternalClickHouseTableComponent(dg.Component, dg.Model, dg.Resolvable):
    """Declare a ClickHouse table as an observable external asset.

    Example:
        ```yaml
        type: dagster_component_templates.ExternalClickHouseTableComponent
        attributes:
          asset_key: clickhouse/analytics/events
          database: analytics
          table: events
          host_env_var: CLICKHOUSE_HOST
          password_env_var: CLICKHOUSE_PASSWORD
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'clickhouse/analytics/events')")
    database: str = Field(description="ClickHouse database name")
    table: str = Field(description="ClickHouse table name")
    host_env_var: str = Field(description="Env var with ClickHouse host")
    port: int = Field(default=8443, description="ClickHouse port")
    username_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse password")
    description: Optional[str] = Field(default=None, description="Human-readable description")
    group_name: Optional[str] = Field(default="clickhouse", description="Dagster asset group name")
    owners: Optional[list] = Field(default=None, description="List of owner emails or team names")

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily'|'weekly'|'monthly'|'hourly'|'static'|'dynamic'|'multi', or None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO start date for time-based partition types (e.g. '2024-01-01').",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static / multi partition types.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic').",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec; overrides flat fields when set.",
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
            key=dg.AssetKey(self.asset_key.split("/")),
            description=self.description or f"ClickHouse table {self.database}.{self.table}",
            group_name=self.group_name,
            owners=self.owners or [],
            kinds={"clickhouse", "sql"},
            metadata={
                "dagster/storage_kind": "clickhouse",
                "dagster/observability_type": "external",
                "database": self.database,
                "table": self.table,
                "full_table_name": f"{self.database}.{self.table}",
            },
            partitions_def=partitions_def,
        )
        return dg.Definitions(assets=[spec])
