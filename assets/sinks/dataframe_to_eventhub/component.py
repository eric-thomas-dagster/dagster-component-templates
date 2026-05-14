"""DataFrame to Azure Event Hubs Component.

Sends each DataFrame row as a JSON event to an Azure Event Hub. Composes
with synthetic_data_generator (or any DataFrame-producing asset) to inject
test events into a queue without writing custom asset code.
"""

import json
import os
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class DataframeToEventHubComponent(Component, Model, Resolvable):
    """Send each DataFrame row to Azure Event Hubs as a JSON event.

    Useful for injecting synthetic events to drive a downstream consumer
    (e.g. eventhubs_to_database_asset) without needing a separate producer
    script. Each row is serialized as a JSON object and sent as one event;
    multiple rows are batched into EventDataBatch where possible.

    Example:
        ```yaml
        type: dagster_component_templates.DataframeToEventHubComponent
        attributes:
          asset_name: orders_to_eventhub
          upstream_asset_key: orders_raw
          connection_string_env_var: EVENTHUB_CONNECTION_STRING
          eventhub_name: demo-events
          partition_key_column: customer_id   # optional: pin events to a partition
          group_name: producers
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")
    connection_string_env_var: str = Field(
        description="Env var holding the Event Hubs namespace or hub-scoped connection string"
    )
    eventhub_name: str = Field(description="Event Hub name (the entity inside the namespace)")
    partition_key_column: Optional[str] = Field(
        default=None,
        description=(
            "Optional column to use as the partition key. Events with the same "
            "partition key land in the same partition (preserves ordering)."
        ),
    )
    batch_size: int = Field(
        default=100,
        ge=1,
        le=10000,
        description=(
            "Max events per send batch. EventDataBatch has a 1MB byte cap which "
            "the SDK enforces independently; this is just an upper bound."
        ),
    )
    group_name: Optional[str] = Field(default=None, description="Asset group")
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Auto-includes 'azure' + 'eventhubs' if unset.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        conn_env_var = self.connection_string_env_var
        eventhub_name = self.eventhub_name
        partition_key_column = self.partition_key_column
        batch_size = self.batch_size
        group_name = self.group_name
        description = self.description or (
            f"Send DataFrame rows to Azure Event Hub '{eventhub_name}' as JSON events."
        )

        kinds = self.kinds or ["azure", "eventhubs"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=group_name,
            description=description,
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def producer_asset(
            context: AssetExecutionContext, upstream: pd.DataFrame
        ) -> MaterializeResult:
            try:
                from azure.eventhub import EventData, EventHubProducerClient
            except ImportError as e:
                raise ImportError(
                    "azure-eventhub required: pip install azure-eventhub"
                ) from e

            conn = os.environ[conn_env_var]
            client = EventHubProducerClient.from_connection_string(
                conn, eventhub_name=eventhub_name
            )

            records = upstream.to_dict(orient="records")
            context.log.info(
                f"Sending {len(records)} events to Event Hub '{eventhub_name}'"
            )

            sent = 0
            try:
                for chunk_start in range(0, len(records), batch_size):
                    chunk = records[chunk_start : chunk_start + batch_size]
                    pk = (
                        str(chunk[0].get(partition_key_column, ""))
                        if partition_key_column
                        else None
                    )
                    batch = client.create_batch(partition_key=pk) if pk else client.create_batch()
                    for row in chunk:
                        evt = EventData(json.dumps(row, default=str))
                        try:
                            batch.add(evt)
                        except ValueError:
                            client.send_batch(batch)
                            sent += len(batch)
                            batch = (
                                client.create_batch(partition_key=pk)
                                if pk
                                else client.create_batch()
                            )
                            batch.add(evt)
                    if len(batch) > 0:
                        client.send_batch(batch)
                        sent += len(batch)
            finally:
                client.close()

            context.log.info(f"Sent {sent}/{len(records)} events")
            return MaterializeResult(
                metadata={
                    "events_sent": MetadataValue.int(sent),
                    "eventhub_name": MetadataValue.text(eventhub_name),
                    "partition_key_column": MetadataValue.text(
                        partition_key_column or "(none — round-robin partitioning)"
                    ),
                }
            )

        return Definitions(assets=[producer_asset])
