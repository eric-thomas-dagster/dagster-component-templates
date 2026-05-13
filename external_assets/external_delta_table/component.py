"""External Delta Lake Table Asset Component.

Declare a Delta Lake table (written by Spark, Databricks, Trino, Flink, etc.)
as an observable external Dagster asset. Engine-agnostic — works with any
Delta writer on any storage backend.

This is the **declaration** — the table shows up in the asset graph with proper
metadata + kinds. Pair with `delta_ingestion` if you also need to read it, or
with `databricks_table_observation_sensor` (Databricks-specific) for liveness.

Sibling to `external_databricks_table` (Databricks-specific). Use this one when
the Delta table sits outside Databricks (raw S3 / ADLS / GCS), or when you want
to keep the declaration engine-agnostic.
"""

from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name
):
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None
    _values = (
        [str(v).strip() for v in partition_values if str(v).strip()]
        if isinstance(partition_values, (list, tuple))
        else [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    )
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class ExternalDeltaTableAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare an external Delta Lake table as a Dagster observable external asset.

    Example — Delta on S3 owned by Spark:

        ```yaml
        type: dagster_component_templates.ExternalDeltaTableAsset
        attributes:
          asset_key: lakehouse/events
          table_uri: s3://my-bucket/lakehouse/events
          owner_engine: spark
          group_name: lakehouse
        ```

    Example — UC-managed Delta from Databricks:

        ```yaml
        attributes:
          asset_key: sales/orders
          table_uri: uc://main.sales.orders
          owner_engine: databricks
        ```
    """

    asset_key: str = Field(description="Dagster asset key (slash-delimited)")

    table_uri: str = Field(
        description="Delta table location: s3://, az://, gs://, /local, or uc://catalog.schema.table"
    )
    owner_engine: Optional[str] = Field(
        default=None,
        description="Which engine writes the table (databricks / spark / trino / flink / snowflake-uniform). Adds to kinds + metadata.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name
        )

        kinds = {"delta", "lakehouse"}
        if self.owner_engine:
            kinds.add(self.owner_engine.lower())

        metadata: Dict[str, Any] = {
            "table_uri": self.table_uri,
            "dagster.observability_type": "external",
        }
        if self.owner_engine:
            metadata["owner_engine"] = self.owner_engine

        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"External Delta table {self.table_uri}",
            kinds=kinds,
            owners=self.owners or [],
            tags=self.tags or {},
            metadata=metadata,
            partitions_def=partitions_def,
        )
        return dg.Definitions(assets=[spec])
