"""NATS to Database Asset Component.

Subscribes to a NATS subject or JetStream consumer and writes messages to a
database table via SQLAlchemy. Designed to be triggered by nats_monitor.

Each message payload is expected to be JSON.
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

    # Both shapes set: ambiguous. Pick one.
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


class NATSToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Subscribe to a NATS subject/JetStream and write messages to a database table.

    Connects to NATS, fetches a batch of messages, disconnects.
    Triggered by nats_monitor or run on a schedule.

    Example:
        ```yaml
        type: dagster_component_templates.NATSToDatabaseAssetComponent
        attributes:
          asset_name: nats_events_ingest
          nats_url_env_var: NATS_URL
          subject: events.>
          database_url_env_var: DATABASE_URL
          table_name: raw_events
          max_messages: 10000
          use_jetstream: true
          stream_name: EVENTS
          consumer_name: dagster-ingest
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    nats_url: Optional[str] = Field(default=None, description="NATS server URL (nats://host:4222). Set this OR nats_url_env_var.")
    nats_url_env_var: Optional[str] = Field(default=None, description="Env var with NATS server URL. Set this OR nats_url.")
    subject: str = Field(description="NATS subject to subscribe to (supports wildcards)")
    use_jetstream: bool = Field(default=False, description="Use JetStream for durable consumption")
    stream_name: Optional[str] = Field(default=None, description="JetStream stream name (required if use_jetstream=true)")
    consumer_name: Optional[str] = Field(default="dagster-ingest", description="JetStream durable consumer name")
    credentials_env_var: Optional[str] = Field(default=None, description="Env var with path to NATS credentials file")
    database_url: Optional[str] = Field(default=None, description="SQLAlchemy database URL. Set this OR database_url_env_var.")
    database_url_env_var: Optional[str] = Field(default=None, description="Env var with SQLAlchemy database URL. Set this OR database_url.")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_messages: int = Field(default=10000, description="Max messages to fetch per run")
    fetch_timeout_seconds: float = Field(default=5.0, description="Seconds to wait for each fetch batch")
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

        class NATSRunConfig(Config):
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
            description=_self.description or f"NATS:{_self.subject} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"nats", "sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
        )
        def nats_to_database_asset(context: AssetExecutionContext, config: NATSRunConfig):
            import os, json, asyncio
            import nats
            import pandas as pd
            from sqlalchemy import create_engine

            def _resolve(literal, env_var, name):
                if literal:
                    return literal
                if env_var:
                    if env_var not in os.environ:
                        raise KeyError(f"Env var '{env_var}' (for {name}) is not set")
                    return os.environ[env_var]
                raise ValueError(f"Set either '{name}' or '{name}_env_var'")
            nats_url = _resolve(_self.nats_url, _self.nats_url_env_var, "nats_url")
            db_url = _resolve(_self.database_url, _self.database_url_env_var, "database_url")
            max_msgs = config.max_messages or _self.max_messages

            async def fetch_messages():
                connect_kwargs: dict = {"servers": nats_url}
                if _self.credentials_env_var:
                    connect_kwargs["user_credentials"] = os.environ[_self.credentials_env_var]

                nc = await nats.connect(**connect_kwargs)
                records = []

                try:
                    if _self.use_jetstream:
                        js = nc.jetstream()
                        consumer = await js.pull_subscribe(
                            _self.subject,
                            stream=_self.stream_name,
                            durable=_self.consumer_name,
                        )
                        empty_count = 0
                        while len(records) < max_msgs and empty_count < 3:
                            try:
                                batch = await consumer.fetch(
                                    batch=min(500, max_msgs - len(records)),
                                    timeout=_self.fetch_timeout_seconds,
                                )
                                if not batch:
                                    empty_count += 1
                                    continue
                                empty_count = 0
                                for msg in batch:
                                    try:
                                        parsed = json.loads(msg.data.decode("utf-8"))
                                        if isinstance(parsed, dict):
                                            records.append(parsed)
                                        elif isinstance(parsed, list):
                                            records.extend(parsed)
                                        await msg.ack()
                                    except Exception as e:
                                        context.log.warning(f"Skipping unparseable message: {e}")
                            except nats.errors.TimeoutError:
                                break
                        await consumer.unsubscribe()
                    else:
                        # Core NATS: subscribe and collect for a timeout
                        collected = []
                        sub = await nc.subscribe(_self.subject)
                        try:
                            while len(collected) < max_msgs:
                                try:
                                    msg = await asyncio.wait_for(
                                        sub.next_msg(), timeout=_self.fetch_timeout_seconds
                                    )
                                    collected.append(msg)
                                except asyncio.TimeoutError:
                                    break
                        finally:
                            await sub.unsubscribe()

                        for msg in collected:
                            try:
                                parsed = json.loads(msg.data.decode("utf-8"))
                                if isinstance(parsed, dict):
                                    records.append(parsed)
                                elif isinstance(parsed, list):
                                    records.extend(parsed)
                            except Exception as e:
                                context.log.warning(f"Skipping unparseable message: {e}")
                finally:
                    await nc.close()

                return records

            context.log.info(f"Fetching up to {max_msgs} messages from NATS subject {_self.subject}")
            records = asyncio.run(fetch_messages())

            if not records:
                context.log.info("No messages received.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "subject": _self.subject})

            df = pd.DataFrame(records)
            context.log.info(f"Fetched {len(records)} messages → {len(df)} rows, {len(df.columns)} columns")

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
                "messages_consumed": len(records),
                "subject": _self.subject,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[nats_to_database_asset])
