"""Azure Event Hubs to Database Asset Component.

Consumes a batch of events from an Azure Event Hub and writes them to a database
table via SQLAlchemy. Designed to be triggered by eventhubs_monitor.

Each event body is expected to be JSON. The batch size is configurable.
"""
from typing import Dict, List, Optional, Union
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class EventHubsToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Consume events from an Azure Event Hub and write them to a database table.

    Triggered by eventhubs_monitor passing partition_id/sequence_number via run_config,
    or run on a schedule to drain a hub batch.

    Example:
        ```yaml
        type: dagster_component_templates.EventHubsToDatabaseAssetComponent
        attributes:
          asset_name: eventhubs_events_ingest
          connection_string_env_var: EVENTHUB_CONNECTION_STRING
          eventhub_name: my-hub
          database_url_env_var: DATABASE_URL
          table_name: raw_events
          max_events: 10000
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    connection_string_env_var: str = Field(description="Env var with Event Hubs connection string")
    eventhub_name: str = Field(description="Event Hub name")
    consumer_group: str = Field(default="$Default", description="Consumer group")
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_events: int = Field(default=10000, description="Max events to consume per run")
    max_wait_seconds: float = Field(default=5.0, description="Max seconds to wait for events")
    column_mapping: Optional[dict] = Field(default=None, description="Rename columns: {old: new}")
    group_name: Optional[str] = Field(default="ingestion", description="Asset group name")
    description: Optional[str] = Field(default=None)
    partition_type: str = Field(default="none", description="none, daily, weekly, or monthly")
    partition_start_date: Optional[str] = Field(default=None, description="Partition start date YYYY-MM-DD (required if partition_type != none)")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        partitions_def = None
        if _self.partition_type == "daily":
            partitions_def = dg.DailyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")
        elif _self.partition_type == "weekly":
            partitions_def = dg.WeeklyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")
        elif _self.partition_type == "monthly":
            partitions_def = dg.MonthlyPartitionsDefinition(start_date=_self.partition_start_date or "2020-01-01")

        class EventHubsRunConfig(Config):
            partition_id: Optional[str] = None    # consume specific partition (from sensor)
            sequence_number: Optional[int] = None  # start from offset (from sensor)
            max_events: Optional[int] = None       # override at runtime

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).

        _retry_policy = None

        if self.retry_policy_max_retries is not None:

            from dagster import Backoff, RetryPolicy

            _retry_policy = RetryPolicy(

                max_retries=self.retry_policy_max_retries,

                delay=self.retry_policy_delay_seconds or 1,

                backoff=Backoff[self.retry_policy_backoff.upper()],

            )


        @dg.asset(retry_policy=_retry_policy, 
            key=dg.AssetKey.from_user_string(_self.asset_name),
            description=_self.description or f"EventHubs:{_self.eventhub_name} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"eventhubs", "sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
        )
        def eventhubs_to_database_asset(context: AssetExecutionContext, config: EventHubsRunConfig):
            import os, json
            import pandas as pd
            from azure.eventhub import EventHubConsumerClient
            from sqlalchemy import create_engine

            conn_str = os.environ[_self.connection_string_env_var]
            db_url = os.environ[_self.database_url_env_var]
            max_evts = config.max_events or _self.max_events

            records = []
            done_flag = {"stop": False}

            client = EventHubConsumerClient.from_connection_string(
                conn_str,
                consumer_group=_self.consumer_group,
                eventhub_name=_self.eventhub_name,
            )

            def on_event_batch(partition_context, events):
                if done_flag["stop"]:
                    return
                for event in events:
                    if len(records) >= max_evts:
                        done_flag["stop"] = True
                        break
                    try:
                        body = json.loads(event.body_as_str())
                        if isinstance(body, dict):
                            records.append(body)
                        elif isinstance(body, list):
                            records.extend(body)
                    except Exception as e:
                        context.log.warning(f"Skipping unparseable event: {e}")
                partition_context.update_checkpoint()
                if done_flag["stop"] or len(records) >= max_evts:
                    # receive_batch loops forever otherwise — close from callback to break out
                    client.close()

            context.log.info(f"Consuming up to {max_evts} events from {_self.eventhub_name}")

            try:
                with client:
                    client.receive_batch(
                        on_event_batch=on_event_batch,
                        max_batch_size=min(300, max_evts),
                        max_wait_time=_self.max_wait_seconds,
                        partition_id=config.partition_id,
                        starting_position=config.sequence_number if config.sequence_number is not None else "-1",
                    )
            except Exception as e:
                # Closing the client mid-receive can raise; treat as clean shutdown if we got events
                if not records:
                    raise
                context.log.info(f"Receiver shut down after collecting {len(records)} events: {e}")

            if not records:
                context.log.info("No events consumed.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "eventhub": _self.eventhub_name})

            df = pd.DataFrame(records)
            context.log.info(f"Consumed {len(records)} events → {len(df)} rows, {len(df.columns)} columns")

            if _self.column_mapping:
                df = df.rename(columns=_self.column_mapping)

            if context.has_partition_key:
                df["_partition_key"] = context.partition_key

            table_name = _self.table_name
            if context.has_partition_key:
                table_name = table_name.replace("{partition_key}", context.partition_key)

            engine = create_engine(db_url)
            df.to_sql(table_name, con=engine, schema=_self.schema_name,
                      if_exists=_self.if_exists, index=False, method="multi", chunksize=1000)

            context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{table_name}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "eventhub": _self.eventhub_name,
                "events_consumed": len(records),
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[eventhubs_to_database_asset])
