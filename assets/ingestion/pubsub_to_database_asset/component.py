"""Google Cloud Pub/Sub to Database Asset Component.

Pulls messages from a Google Cloud Pub/Sub subscription and writes them to a
database table via SQLAlchemy. Designed to be triggered by pubsub_monitor.

Each message data is expected to be JSON. Messages are acknowledged after write.
"""
from typing import Dict, List, Optional, Union
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field


class PubSubToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pull messages from a GCP Pub/Sub subscription and write to a database table.

    Triggered by pubsub_monitor, or run on a schedule to drain a subscription batch.
    Messages are acknowledged after successful write.

    Example:
        ```yaml
        type: dagster_component_templates.PubSubToDatabaseAssetComponent
        attributes:
          asset_name: pubsub_events_ingest
          project_id: my-gcp-project
          subscription_id: events-sub
          database_url_env_var: DATABASE_URL
          table_name: raw_events
          max_messages: 10000
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    project_id: str = Field(description="GCP project ID")
    subscription_id: str = Field(description="Pub/Sub subscription ID")
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Env var pointing to GCP service account JSON path. If unset, uses Application Default Credentials."
    )
    database_url_env_var: str = Field(description="Env var with SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_messages: int = Field(default=10000, description="Max messages to pull per run")
    pull_batch_size: int = Field(default=1000, description="Messages per pull request (max 1000)")
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

        class PubSubRunConfig(Config):
            max_messages: Optional[int] = None  # override at runtime

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
            description=_self.description or f"PubSub:{_self.subscription_id} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"pubsub", "sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
        )
        def pubsub_to_database_asset(context: AssetExecutionContext, config: PubSubRunConfig):
            import os, json
            import pandas as pd
            from google.cloud import pubsub_v1
            from sqlalchemy import create_engine

            if _self.credentials_env_var:
                os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", os.environ[_self.credentials_env_var])

            db_url = os.environ[_self.database_url_env_var]
            max_msgs = config.max_messages or _self.max_messages
            subscription_path = f"projects/{_self.project_id}/subscriptions/{_self.subscription_id}"

            context.log.info(f"Pulling up to {max_msgs} messages from {subscription_path}")

            subscriber = pubsub_v1.SubscriberClient()
            records = []
            ack_ids = []

            with subscriber:
                while len(records) < max_msgs:
                    pull_size = min(_self.pull_batch_size, max_msgs - len(records), 1000)
                    response = subscriber.pull(
                        request={"subscription": subscription_path, "max_messages": pull_size},
                        timeout=10,
                    )
                    if not response.received_messages:
                        break
                    for msg in response.received_messages:
                        try:
                            parsed = json.loads(msg.message.data.decode("utf-8"))
                            if isinstance(parsed, dict):
                                records.append(parsed)
                            elif isinstance(parsed, list):
                                records.extend(parsed)
                            ack_ids.append(msg.ack_id)
                        except Exception as e:
                            context.log.warning(f"Skipping unparseable message: {e}")

                if not records:
                    context.log.info("No messages in subscription.")
                    return dg.MaterializeResult(metadata={"num_rows": 0, "subscription": _self.subscription_id})

                df = pd.DataFrame(records)
                context.log.info(f"Pulled {len(records)} messages → {len(df)} rows, {len(df.columns)} columns")

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

                # Acknowledge after successful write
                for i in range(0, len(ack_ids), 1000):
                    subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": ack_ids[i:i+1000]}
                    )

            context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{table_name}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "messages_consumed": len(ack_ids),
                "subscription": _self.subscription_id,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[pubsub_to_database_asset])
