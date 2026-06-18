"""DataFrame to Azure Service Bus.

Sends each DataFrame row as a JSON message to an Azure Service Bus queue
or topic. Mirrors `dataframe_to_eventhub` for the SB workload — completes
the producer side of the registry's existing servicebus_monitor +
servicebus_to_database_asset (consumer-side) components.

Service Bus vs Event Hubs:
  - SB: enterprise messaging — ordered queues, topics + subscriptions,
    DLQ, transactions, sessions. Lower throughput, higher per-msg semantics.
  - EH: event streaming — partitioned, high throughput, no DLQ, no
    transactions. Higher throughput, lower per-msg semantics.

Use this for SB queues / topics; use dataframe_to_eventhub for EH.
"""

import json
import os
from typing import Dict, List, Optional, Union

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


class DataframeToServiceBusComponent(Component, Model, Resolvable):
    """Send each DataFrame row to Azure Service Bus as a JSON message."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    connection_string_env_var: str = Field(
        description="Env var holding the SB namespace or queue/topic-scoped connection string"
    )
    destination_name: str = Field(
        description="Queue or topic name (the SB entity to send to)"
    )
    destination_type: str = Field(
        default="queue",
        description="'queue' (point-to-point) | 'topic' (pub-sub)",
    )

    session_id_column: Optional[Union[str, int]] = Field(
        default=None,
        description=(
            "Optional column to use as Session ID. Required if the queue/topic has "
            "sessions enabled — guarantees per-session ordering."
        ),
    )
    message_id_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional column to use as Message ID for idempotency / dedup.",
    )
    correlation_id_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional column to use as Correlation ID (for request/reply patterns).",
    )

    batch_size: int = Field(
        default=100,
        ge=1,
        le=4500,
        description=(
            "Max messages per ServiceBusMessageBatch. SB enforces a 256KB batch byte cap "
            "(or 1MB on Premium); the SDK splits batches automatically."
        ),
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

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
        upstream = self.upstream_asset_key
        conn_env = self.connection_string_env_var
        destination = self.destination_name
        is_topic = (self.destination_type or "queue").lower() == "topic"
        session_col = self.session_id_column
        message_col = self.message_id_column
        correlation_col = self.correlation_id_column
        batch_size = self.batch_size

        kinds = self.kinds or ["azure", "servicebus"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream))},
            group_name=self.group_name,
            description=self.description or (
                f"Send DataFrame rows to Azure Service Bus {self.destination_type} '{destination}' as JSON messages."
            ),
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def producer_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            try:
                from azure.servicebus import ServiceBusClient, ServiceBusMessage
            except ImportError as e:
                raise ImportError("azure-servicebus required: pip install azure-servicebus") from e

            conn = os.environ[conn_env]
            client = ServiceBusClient.from_connection_string(conn)

            records = upstream.to_dict(orient="records")
            context.log.info(
                f"Sending {len(records)} messages to SB {self.destination_type} '{destination}'"
            )

            sent = 0
            try:
                with client:
                    sender = (
                        client.get_topic_sender(topic_name=destination)
                        if is_topic
                        else client.get_queue_sender(queue_name=destination)
                    )
                    with sender:
                        for chunk_start in range(0, len(records), batch_size):
                            chunk = records[chunk_start : chunk_start + batch_size]
                            batch = sender.create_message_batch()
                            for row in chunk:
                                msg = ServiceBusMessage(json.dumps(row, default=str))
                                if session_col and session_col in row:
                                    msg.session_id = str(row[session_col])
                                if message_col and message_col in row:
                                    msg.message_id = str(row[message_col])
                                if correlation_col and correlation_col in row:
                                    msg.correlation_id = str(row[correlation_col])
                                try:
                                    batch.add_message(msg)
                                except ValueError:
                                    sender.send_messages(batch)
                                    sent += len(batch)
                                    batch = sender.create_message_batch()
                                    batch.add_message(msg)
                            if len(batch) > 0:
                                sender.send_messages(batch)
                                sent += len(batch)
            finally:
                pass

            context.log.info(f"Sent {sent}/{len(records)} messages")
            return MaterializeResult(
                metadata={
                    "messages_sent": MetadataValue.int(sent),
                    "destination": MetadataValue.text(f"{self.destination_type}://{destination}"),
                    "session_id_column": MetadataValue.text(session_col or "(none)"),
                }
            )

        return Definitions(assets=[producer_asset])
