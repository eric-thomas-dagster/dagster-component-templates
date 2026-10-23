"""Kafka to Database Asset Component.

Consumes a batch of messages from a Kafka topic and writes them to a database
table via SQLAlchemy. Designed to be triggered by kafka_monitor.

Each message body is expected to be JSON. The batch size is configurable.
"""
from typing import Any, Dict, List, Optional, Union
import dagster as dg
from dagster import AssetExecutionContext, Config
from pydantic import Field



def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value



def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
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
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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
        return _require_env(database_url_env_var)
    raise ValueError("Set 'resource_key', 'database_url', or 'database_url_env_var'.")



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


class KafkaToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Consume messages from a Kafka topic and write them to a database table.

    Triggered by kafka_monitor passing topic/partition/offset via run_config,
    or run on a schedule to drain a topic batch.

    Example:
        ```yaml
        type: dagster_component_templates.KafkaToDatabaseAssetComponent
        attributes:
          asset_name: kafka_events_ingest
          bootstrap_servers_env_var: KAFKA_BOOTSTRAP_SERVERS
          database_url_env_var: DATABASE_URL
          topic: events
          table_name: raw_events
          max_messages: 10000
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    bootstrap_servers: Optional[str] = Field(
        default=None,
        description="Kafka bootstrap servers (comma-separated). Set this OR bootstrap_servers_env_var.",
    )
    bootstrap_servers_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Kafka bootstrap servers. Set this OR bootstrap_servers.",
    )
    database_url: Optional[str] = Field(
        default=None,
        description="SQLAlchemy database URL. Set this OR database_url_env_var.",
    )
    database_url_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the SQLAlchemy database URL. Set this OR database_url.",
    )
    topic: str = Field(description="Kafka topic to consume from")
    consumer_group: str = Field(default="dagster-ingestion", description="Kafka consumer group ID")
    resource_key: Optional[str] = Field(
        default=None,
        description="Key of a registered SQL resource (e.g. postgres_resource) to use for the destination connection instead of database_url/database_url_env_var. The resource must expose a connection_string property (e.g. PostgresResource, MongoDBResource); not all resource types do.",
    )
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    max_messages: int = Field(default=10000, description="Max messages to consume per run")
    poll_timeout_seconds: float = Field(default=5.0, description="Seconds to wait for messages before stopping")
    security_protocol: str = Field(default="PLAINTEXT", description="PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")
    sasl_mechanism: Optional[str] = Field(default=None, description="PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")
    sasl_username_env_var: Optional[str] = Field(default=None, description="Env var with SASL username")
    sasl_password_env_var: Optional[str] = Field(default=None, description="Env var with SASL password")
    column_mapping: Optional[dict] = Field(default=None, description="Rename columns: {old: new}")
    group_name: Optional[str] = Field(default="ingestion", description="Asset group name")
    description: Optional[str] = Field(default=None)
    partition_type: str = Field(default="none", description="none, daily, weekly, monthly, hourly, static, multi, or dynamic")

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

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        _column_lineage_md = _column_lineage_metadata(self)
        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        _self = self

        partitions_def = _build_partitions_def(
            _self.partition_type if _self.partition_type != "none" else None,
            _self.partition_start or _self.partition_start_date,
            _self.partition_values,
            _self.dynamic_partition_name,
            _self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

        class KafkaRunConfig(Config):
            topic: Optional[str] = None          # override topic at runtime
            max_messages: Optional[int] = None   # override max_messages at runtime
            partition: Optional[int] = None      # consume specific partition (from sensor)
            offset: Optional[int] = None         # start from specific offset (from sensor)

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).

        _retry_policy = None

        if self.retry_policy_max_retries is not None:

            from dagster import Backoff, RetryPolicy

            _retry_policy = RetryPolicy(

                max_retries=self.retry_policy_max_retries,

                delay=self.retry_policy_delay_seconds or 1,

                backoff=Backoff[self.retry_policy_backoff.upper()],

            )


        @dg.asset(freshness_policy=_freshness_policy_obj, retry_policy=_retry_policy, 
            key=dg.AssetKey.from_user_string(_self.asset_name),
            description=_self.description or f"Kafka:{_self.topic} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"kafka", "sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
            required_resource_keys={_self.resource_key} if _self.resource_key else set(),
        )
        def kafka_to_database_asset(context: AssetExecutionContext, config: KafkaRunConfig):
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            import os, json
            import pandas as pd
            from confluent_kafka import Consumer, KafkaError, TopicPartition
            from sqlalchemy import create_engine

            def _resolve(literal, env_var, name):
                if literal:
                    return literal
                if env_var:
                    if env_var not in os.environ:
                        raise KeyError(f"Env var '{env_var}' (for {name}) is not set")
                    return _require_env(env_var)
                raise ValueError(f"Set either '{name}' or '{name}_env_var'")
            bootstrap = _resolve(_self.bootstrap_servers, _self.bootstrap_servers_env_var, "bootstrap_servers")
            db_url = _resolve_db_url(context, _self.resource_key, _self.database_url, _self.database_url_env_var)
            topic = config.topic or _self.topic
            max_msgs = config.max_messages or _self.max_messages

            consumer_config = {
                "bootstrap.servers": bootstrap,
                "group.id": _self.consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "security.protocol": _self.security_protocol,
            }
            if _self.sasl_mechanism:
                consumer_config["sasl.mechanisms"] = _self.sasl_mechanism
                consumer_config["sasl.username"] = os.environ.get(_self.sasl_username_env_var or "", "")
                consumer_config["sasl.password"] = os.environ.get(_self.sasl_password_env_var or "", "")

            consumer = Consumer(consumer_config)

            if config.partition is not None and config.offset is not None:
                consumer.assign([TopicPartition(topic, config.partition, config.offset)])
            else:
                consumer.subscribe([topic])

            context.log.info(f"Consuming up to {max_msgs} messages from {topic}")

            records = []
            consumed = 0
            empty_polls = 0

            while consumed < max_msgs and empty_polls < 3:
                msg = consumer.poll(timeout=_self.poll_timeout_seconds)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    raise Exception(f"Kafka error: {msg.error()}")
                empty_polls = 0
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    if isinstance(record, dict):
                        records.append(record)
                    elif isinstance(record, list):
                        records.extend(record)
                except Exception as e:
                    context.log.warning(f"Skipping unparseable message: {e}")
                consumed += 1

            consumer.commit()
            consumer.close()

            if not records:
                context.log.info("No messages consumed.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "topic": topic})

            df = pd.DataFrame(records)
            context.log.info(f"Consumed {len(records)} messages → {len(df)} rows, {len(df.columns)} columns")

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
                "topic": topic,
                "messages_consumed": consumed,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[kafka_to_database_asset])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
