"""Google Cloud Pub/Sub to Database Asset Component.

Pulls messages from a Google Cloud Pub/Sub subscription and writes them to a
database table via SQLAlchemy. Designed to be triggered by pubsub_monitor.

Each message data is expected to be JSON. Messages are acknowledged after write.
"""
from typing import Any, Dict, List, Optional, Union
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field



def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
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
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
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
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")



def _resolve_db_url(context, resource_key, database_url, database_url_env_var):
    """Resolve a SQLAlchemy-compatible database URL.

    Precedence: a registered resource (WHERE to connect, e.g. postgres_resource)
    if resource_key is set and the resource exposes a 'connection_string'
    property, otherwise a literal database_url, otherwise database_url_env_var.
    The write mechanics (HOW) are unchanged either way -- this only resolves
    the URL that sqlalchemy.create_engine() then connects with.
    """
    import os

    if resource_key:
        resource = getattr(context.resources, resource_key, None)
        if resource is None:
            raise ValueError(f"Resource {resource_key!r} is not registered in this project's Definitions.")
        url = getattr(resource, "connection_string", None)
        if not url:
            raise ValueError(
                f"Resource {resource_key!r} ({type(resource).__name__}) does not expose a "
                "'connection_string' property, so it can't be used to determine the database "
                "URL automatically. Use database_url / database_url_env_var instead."
            )
        return url
    if database_url:
        return database_url
    if database_url_env_var:
        if database_url_env_var not in os.environ:
            raise KeyError(f"Env var '{database_url_env_var}' is not set")
        return os.environ[database_url_env_var]
    raise ValueError("Set 'resource_key', 'database_url', or 'database_url_env_var'.")


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
    database_url: Optional[str] = Field(default=None, description="SQLAlchemy database URL. Set this OR database_url_env_var OR resource_key.")
    database_url_env_var: Optional[str] = Field(default=None, description="Env var with SQLAlchemy database URL. Set this OR database_url OR resource_key.")
    resource_key: Optional[str] = Field(
        default=None,
        description="Key of a registered SQL resource (e.g. postgres_resource) to use for the destination connection instead of database_url/database_url_env_var. The resource must expose a connection_string property (e.g. PostgresResource, MongoDBResource); not all resource types do.",
    )
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_messages: int = Field(default=10000, description="Max messages to pull per run")
    pull_batch_size: int = Field(default=1000, description="Messages per pull request (max 1000)")
    column_mapping: Optional[dict] = Field(default=None, description="Rename columns: {old: new}")
    group_name: Optional[str] = Field(default="ingestion", description="Asset group name")
    description: Optional[str] = Field(default=None)
    partition_type: str = Field(default="none", description="none, daily, weekly, monthly, hourly, static, multi, or dynamic")
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

    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a markdown preview of the written rows in the materialization metadata.",
    )

    preview_rows: int = Field(
        default=25,
        description="Max rows to include in the preview when include_preview_metadata is True.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        partitions_def = _build_partitions_def(
            _self.partition_type if _self.partition_type != "none" else None,
            _self.partition_start or _self.partition_start_date,
            _self.partition_values,
            _self.dynamic_partition_name,
            _self.partition_dimensions,
        )

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
            required_resource_keys={_self.resource_key} if _self.resource_key else set(),
        )
        def pubsub_to_database_asset(context: AssetExecutionContext, config: PubSubRunConfig):
            import os, json
            import pandas as pd
            from google.cloud import pubsub_v1
            from sqlalchemy import create_engine

            if _self.credentials_env_var:
                os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", os.environ[_self.credentials_env_var])

            db_url = _resolve_db_url(context, _self.resource_key, _self.database_url, _self.database_url_env_var)
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
            _preview_metadata = {}

            if _self.include_preview_metadata:

                _prev_df = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)

                _cols = list(_prev_df.columns)

                _preview_metadata["preview"] = dg.MetadataValue.md(

                    "| " + " | ".join(_cols) + " |\n"

                    "| " + " | ".join(["---"] * len(_cols)) + " |\n" +

                    "\n".join("| " + " | ".join(str(v) for v in row) + " |" for row in _prev_df.itertuples(index=False))

                )


            context.log.info(f"Wrote {len(df)} rows to {_self.schema_name + '.' if _self.schema_name else ''}{table_name}")
            return dg.MaterializeResult(metadata={**_preview_metadata, 
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "messages_consumed": len(ack_ids),
                "subscription": _self.subscription_id,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[pubsub_to_database_asset])
