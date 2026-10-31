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
    partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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


class ExternalDeltaTableAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare an external Delta Lake table as a Dagster observable external asset.

    Example — Delta on S3 owned by Spark:

        ```yaml
        type: dagster_community_components.ExternalDeltaTableAsset
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
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), csv=('partition_values',), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
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
            key=dg.AssetKey.from_user_string(self.asset_key),
            group_name=self.group_name,
            description=self.description or f"External Delta table {self.table_uri}",
            kinds=kinds,
            owners=self.owners or [],
            tags=self.tags or {},
            metadata=metadata,
            partitions_def=partitions_def,
        )
        return dg.Definitions(assets=[spec])
