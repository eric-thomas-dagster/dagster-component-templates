"""DataFrame to Kafka Component.

Sends each DataFrame row as a JSON message to a Kafka topic. Composes with
synthetic_data_generator (or any DataFrame-producing asset) to publish test
events into a topic without writing custom producer code, and is the
destination-side counterpart to kafka_monitor / kafka_to_database_asset
(consumer-only today).
"""

import json
import os
from typing import Any, Dict, List, Optional, Union

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
from pydantic import Field, model_validator


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


# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


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


class DataframeToKafkaComponent(Component, Model, Resolvable):
    """Send each DataFrame row to a Kafka topic as a JSON message.

    Destination-side counterpart to kafka_monitor and kafka_to_database_asset,
    which only consume from Kafka. Useful for publishing synthetic or
    transformed events for a downstream consumer to pick up, or as the Kafka
    sink a migrated pipeline (e.g. a StreamSets Kafka Producer destination)
    needs.

    Example:
        ```yaml
        type: dagster_community_components.DataframeToKafkaComponent
        attributes:
          asset_name: orders_to_kafka
          upstream_asset_key: orders_raw
          bootstrap_servers: localhost:9092
          topic: orders
          key_column: order_id   # optional: pins same-key rows to the same partition
          batch_size: 500
          group_name: producers
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Asset key of upstream DataFrame")
    topic: str = Field(description="Kafka topic to publish to")

    bootstrap_servers: Optional[str] = Field(
        default=None,
        description="Comma-separated Kafka broker addresses (e.g. 'broker1:9092,broker2:9092'). Set this OR bootstrap_servers_env_var OR resource_key.",
    )
    bootstrap_servers_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Kafka bootstrap servers. Set this OR bootstrap_servers OR resource_key.",
    )
    security_protocol: str = Field(
        default="PLAINTEXT",
        description="Kafka security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL). Ignored when resource_key is set.",
    )
    sasl_mechanism: Optional[str] = Field(
        default=None,
        description="SASL mechanism when using SASL_PLAINTEXT or SASL_SSL (e.g. PLAIN, SCRAM-SHA-256). Ignored when resource_key is set.",
    )
    sasl_username_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the SASL username. Ignored when resource_key is set.",
    )
    sasl_password_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the SASL password. Ignored when resource_key is set.",
    )
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster resource key providing a pre-configured Kafka client. "
            "When set, context.resources.<resource_key>.get_producer() is called "
            "instead of building a KafkaProducer from the other fields. Compatible "
            "with KafkaResourceComponent (resources/kafka_resource)."
        ),
    )

    key_column: Optional[Union[str, int]] = Field(
        default=None,
        description=(
            "Optional column whose value becomes each message's Kafka key "
            "(UTF-8 encoded). Messages with the same key land on the same "
            "partition, preserving per-key ordering. Unset: no key, round-robin "
            "partitioning."
        ),
    )
    batch_size: int = Field(
        default=500,
        ge=1,
        le=50000,
        description="Rows sent per flush() call. Does not cap total rows sent — only how often the producer's buffer is flushed.",
    )

    group_name: Optional[str] = Field(default=None, description="Asset group")
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Auto-includes 'kafka' if unset.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like broker unavailability.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dataframe To Kafka', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if not self.bootstrap_servers and not self.bootstrap_servers_env_var and not self.resource_key:
            raise ValueError(
                "Set one of 'bootstrap_servers', 'bootstrap_servers_env_var', or 'resource_key'."
            )

        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = (
                [str(v).strip() for v in self.partition_values if str(v).strip()]
                if isinstance(self.partition_values, (list, tuple))
                else [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            )
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
            else:
                raise ValueError(f"unknown partition_type: {_pt!r}")

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

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
        topic = self.topic
        bootstrap_servers = self.bootstrap_servers
        bootstrap_servers_env_var = self.bootstrap_servers_env_var
        security_protocol = self.security_protocol
        sasl_mechanism = self.sasl_mechanism
        sasl_username_env_var = self.sasl_username_env_var
        sasl_password_env_var = self.sasl_password_env_var
        resource_key = self.resource_key
        key_column = self.key_column
        batch_size = self.batch_size
        group_name = self.group_name
        description = self.description or f"Send DataFrame rows to Kafka topic '{topic}' as JSON messages."

        kinds = self.kinds or ["kafka"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        required_resource_keys = {resource_key} if resource_key else set()

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=group_name,
            description=description,
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
            required_resource_keys=required_resource_keys,
        )
        def kafka_producer_asset(
            context: AssetExecutionContext, upstream: pd.DataFrame
        ) -> MaterializeResult:
            if resource_key:
                resource = getattr(context.resources, resource_key, None)
                if resource is None:
                    raise ValueError(f"Resource {resource_key!r} is not registered in this project's Definitions.")
                producer = resource.get_producer()
            else:
                from kafka import KafkaProducer

                bootstrap = bootstrap_servers
                if not bootstrap:
                    if not bootstrap_servers_env_var:
                        raise ValueError("Set either 'bootstrap_servers' or 'bootstrap_servers_env_var'.")
                    bootstrap = _require_env(bootstrap_servers_env_var)

                producer_config: Dict[str, Any] = {
                    "bootstrap_servers": bootstrap,
                    "security_protocol": security_protocol,
                }
                if sasl_mechanism:
                    producer_config["sasl_mechanism"] = sasl_mechanism
                    if sasl_username_env_var:
                        producer_config["sasl_plain_username"] = _require_env(sasl_username_env_var)
                    if sasl_password_env_var:
                        producer_config["sasl_plain_password"] = _require_env(sasl_password_env_var)
                producer = KafkaProducer(**producer_config)

            records = upstream.to_dict(orient="records")
            context.log.info(f"Sending {len(records)} message(s) to Kafka topic '{topic}'")

            sent = 0
            try:
                for i, row in enumerate(records):
                    key = None
                    if key_column is not None and key_column in row:
                        key = str(row[key_column]).encode("utf-8")
                    producer.send(topic, value=json.dumps(row, default=str).encode("utf-8"), key=key)
                    sent += 1
                    if (i + 1) % batch_size == 0:
                        producer.flush()
                producer.flush()
            finally:
                if hasattr(producer, "close"):
                    producer.close()

            context.log.info(f"Sent {sent}/{len(records)} message(s) to '{topic}'")
            return MaterializeResult(
                metadata={
                    "messages_sent": MetadataValue.int(sent),
                    "topic": MetadataValue.text(topic),
                    "key_column": MetadataValue.text(
                        str(key_column) if key_column is not None else "(none — round-robin partitioning)"
                    ),
                }
            )

        return Definitions(assets=[kafka_producer_asset])


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
