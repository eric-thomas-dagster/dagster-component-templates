"""MQTT to Database Asset Component.

Subscribes to an MQTT topic, collects messages for a configured duration, and
writes them to a database table via SQLAlchemy. Designed to be triggered by
mqtt_monitor or run on a schedule.

Each message payload is expected to be JSON.
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


class MQTTToDatabaseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Subscribe to an MQTT topic and write messages to a database table.

    Connects, subscribes, collects messages for collect_seconds, then disconnects.
    Triggered by mqtt_monitor or run on a schedule.

    Example:
        ```yaml
        type: dagster_component_templates.MQTTToDatabaseAssetComponent
        attributes:
          asset_name: mqtt_sensors_ingest
          broker_host_env_var: MQTT_BROKER_HOST
          topic: sensors/#
          database_url_env_var: DATABASE_URL
          table_name: raw_sensor_readings
          collect_seconds: 30
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    broker_host: Optional[str] = Field(default=None, description="MQTT broker hostname. Set this OR broker_host_env_var.")
    broker_host_env_var: Optional[str] = Field(default=None, description="Env var with MQTT broker hostname. Set this OR broker_host.")
    broker_port: int = Field(default=1883, description="MQTT broker port (1883 or 8883 for TLS)")
    topic: str = Field(description="MQTT topic to subscribe to (supports wildcards # and +)")
    username_env_var: Optional[str] = Field(default=None, description="Env var with MQTT username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with MQTT password")
    use_tls: bool = Field(default=False, description="Enable TLS/SSL connection")
    database_url: Optional[str] = Field(default=None, description="SQLAlchemy database URL. Set this OR database_url_env_var.")
    database_url_env_var: Optional[str] = Field(default=None, description="Env var with SQLAlchemy database URL. Set this OR database_url.")
    resource_key: Optional[str] = Field(
        default=None,
        description="Key of a registered SQL resource (e.g. postgres_resource) to use for the destination connection instead of database_url/database_url_env_var. The resource must expose a connection_string property (e.g. PostgresResource, MongoDBResource); not all resource types do.",
    )
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="append", description="fail, replace, or append")
    collect_seconds: float = Field(default=30.0, description="Seconds to collect messages before writing")
    max_messages: int = Field(default=10000, description="Max messages to collect per run")
    qos: int = Field(default=1, description="MQTT QoS level (0, 1, or 2)")
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
        )

        class MQTTRunConfig(Config):
            collect_seconds: Optional[float] = None  # override at runtime
            max_messages: Optional[int] = None        # override at runtime

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
            description=_self.description or f"MQTT:{_self.topic} → {_self.table_name}",
            group_name=_self.group_name,
            kinds={"mqtt", "sql"},
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
            partitions_def=partitions_def,
            required_resource_keys={_self.resource_key} if _self.resource_key else set(),
        )
        def mqtt_to_database_asset(context: AssetExecutionContext, config: MQTTRunConfig):
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            import os, json, time
            import paho.mqtt.client as mqtt
            import pandas as pd
            from sqlalchemy import create_engine

            def _resolve(literal, env_var, name):
                if literal:
                    return literal
                if env_var:
                    if env_var not in os.environ:
                        raise KeyError(f"Env var '{env_var}' (for {name}) is not set")
                    return _require_env(env_var)
                raise ValueError(f"Set either '{name}' or '{name}_env_var'")
            broker_host = _resolve(_self.broker_host, _self.broker_host_env_var, "broker_host")
            db_url = _resolve_db_url(context, _self.resource_key, _self.database_url, _self.database_url_env_var)
            collect_secs = config.collect_seconds or _self.collect_seconds
            max_msgs = config.max_messages or _self.max_messages

            records = []
            done = False

            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    client.subscribe(_self.topic, qos=_self.qos)
                    context.log.info(f"Connected to {broker_host}, subscribed to {_self.topic}")
                else:
                    raise Exception(f"MQTT connect failed with code {rc}")

            def on_message(client, userdata, msg):
                if len(records) >= max_msgs:
                    return
                try:
                    payload = json.loads(msg.payload.decode("utf-8"))
                    record = payload if isinstance(payload, dict) else {"payload": payload}
                    record["_topic"] = msg.topic
                    records.append(record)
                except Exception as e:
                    context.log.warning(f"Skipping unparseable message on {msg.topic}: {e}")

            client = mqtt.Client()
            client.on_connect = on_connect
            client.on_message = on_message

            if _self.use_tls:
                client.tls_set()
            if _self.username_env_var:
                username = _require_env(_self.username_env_var)
                password = os.environ.get(_self.password_env_var or "", "") if _self.password_env_var else None
                client.username_pw_set(username, password)

            context.log.info(f"Collecting messages for {collect_secs}s from {broker_host} (max {max_msgs})")
            client.connect(broker_host, _self.broker_port, keepalive=60)
            client.loop_start()
            time.sleep(collect_secs)
            client.loop_stop()
            client.disconnect()

            if not records:
                context.log.info("No messages received.")
                return dg.MaterializeResult(metadata={"num_rows": 0, "topic": _self.topic})

            df = pd.DataFrame(records)
            context.log.info(f"Collected {len(records)} messages → {len(df)} rows, {len(df.columns)} columns")

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
                "topic": _self.topic,
                "collect_seconds": collect_secs,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{table_name}",
            })

        return dg.Definitions(assets=[mqtt_to_database_asset])


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
