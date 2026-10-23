"""External Iceberg Table Asset Component.

Declare an Apache Iceberg table (written by Snowflake / Trino / Spark /
Flink / Databricks / etc.) as an observable external Dagster asset.
Engine-agnostic — works with any Iceberg-compatible writer.

This is the **declaration** — it shows up in the asset graph with proper
metadata + kinds. Pair with `iceberg_ingestion` if you need to read it,
or with a future `iceberg_table_observation_sensor` for liveness.

Sibling to `external_databricks_table` (Databricks/UC-specific) and
`external_assets/external_*` for other engines. Use this one when the
table is consumed via Iceberg semantics specifically — Snowflake's
managed Iceberg catalog, Polaris, Nessie, S3 Tables, Glue Iceberg, etc.
"""

from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")



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


class ExternalIcebergTableAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare an external Apache Iceberg table as a Dagster external asset.

    Example:

        ```yaml
        type: dagster_component_templates.ExternalIcebergTableAsset
        attributes:
          asset_key: sales/orders
          catalog_name: my_polaris_catalog
          namespace: sales
          table_name: orders
          warehouse: s3://my-warehouse
          owner_engine: trino
          group_name: lakehouse
        ```
    """

    asset_key: str = Field(description="Dagster asset key (slash-delimited)")

    catalog_name: str = Field(
        description="Iceberg catalog handle (e.g. 'my_polaris_catalog', 'glue_main', 'snowflake_managed')"
    )
    namespace: str = Field(description="Iceberg namespace")
    table_name: str = Field(description="Iceberg table name")

    warehouse: Optional[str] = Field(
        default=None,
        description="Warehouse location (e.g. 's3://bucket/warehouse'). Used as metadata only.",
    )
    catalog_type: Optional[str] = Field(
        default=None,
        description="'rest' | 'glue' | 'hive' | 'hadoop' | 'sql' — informational metadata.",
    )
    owner_engine: Optional[str] = Field(
        default=None,
        description="Which engine writes the table (snowflake / trino / spark / flink / databricks). Goes into asset kinds + metadata.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), csv=('partition_values',), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        full_name = f"{self.catalog_name}.{self.namespace}.{self.table_name}"
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

        kinds = {"iceberg", "lakehouse"}
        if self.owner_engine:
            kinds.add(self.owner_engine.lower())

        metadata: Dict[str, Any] = {
            "catalog_name": self.catalog_name,
            "namespace": self.namespace,
            "table_name": self.table_name,
            "full_table_name": full_name,
            "dagster.observability_type": "external",
        }
        if self.warehouse:
            metadata["warehouse"] = self.warehouse
        if self.catalog_type:
            metadata["catalog_type"] = self.catalog_type
        if self.owner_engine:
            metadata["owner_engine"] = self.owner_engine

        spec = dg.AssetSpec(
            key=dg.AssetKey.from_user_string(self.asset_key),
            group_name=self.group_name,
            description=self.description or f"External Iceberg table {full_name}",
            kinds=kinds,
            owners=self.owners or [],
            tags=self.tags or {},
            metadata=metadata,
            partitions_def=partitions_def,
        )
        return dg.Definitions(assets=[spec])
