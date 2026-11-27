"""PubsubSubscriptionAssetComponent.

Declaratively ensures a Google Cloud Pub/Sub subscription on a topic is
present or absent, as a Dagster asset: `create_subscription`/
`delete_subscription`, idempotent against `AlreadyExists`/`NotFound` (the real
`google-cloud-pubsub` client raises on both rather than silently no-op'ing).

Distinct from `GooglePubSubComponent` (`integrations/google_pubsub`), which only
*observes* pre-existing topics/subscriptions as read-only assets -- this
component manages subscription lifecycle (the Airflow
`PubSubCreateSubscriptionOperator` / `PubSubDeleteSubscriptionOperator`
equivalent).

Request/response shapes verified directly against the `google-cloud-pubsub`
client reference (not from memory): `SubscriberClient.create_subscription(
request={'name': subscription_path, 'topic': topic_path,
'ack_deadline_seconds': int, 'push_config': {'push_endpoint': str}})` -> a
`Subscription`; `get_subscription(request={'subscription': subscription_path})`;
`delete_subscription(request={'subscription': subscription_path})`.
`AlreadyExists`/`NotFound` come from `google.api_core.exceptions`. Omitting
`push_config` creates a pull subscription (the default).
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

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


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise ValueError(f"Environment variable {name!r} is not set.")
    return value


def _make_subscriber_client(credentials_env_var: Optional[str]):
    import os

    from google.cloud import pubsub_v1

    if credentials_env_var:
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", _require_env(credentials_env_var))
    return pubsub_v1.SubscriberClient()


def _ensure_subscription_state(
    client,
    subscription_path: str,
    topic_path: str,
    desired_state: str,
    ack_deadline_seconds: int,
    push_endpoint: Optional[str],
    log,
) -> Dict[str, Any]:
    from google.api_core import exceptions as gax_exceptions

    if desired_state == "present":
        request: Dict[str, Any] = {
            "name": subscription_path,
            "topic": topic_path,
            "ack_deadline_seconds": ack_deadline_seconds,
        }
        if push_endpoint:
            request["push_config"] = {"push_endpoint": push_endpoint}
        try:
            subscription = client.create_subscription(request=request)
            log.info(f"pubsub_subscription_asset: created subscription {subscription_path!r} on topic {topic_path!r}.")
            return {"subscription": subscription, "action": "created"}
        except gax_exceptions.AlreadyExists:
            subscription = client.get_subscription(request={"subscription": subscription_path})
            log.info(f"pubsub_subscription_asset: subscription {subscription_path!r} already exists.")
            return {"subscription": subscription, "action": "none"}

    if desired_state == "absent":
        try:
            client.delete_subscription(request={"subscription": subscription_path})
            log.info(f"pubsub_subscription_asset: deleted subscription {subscription_path!r}.")
            return {"subscription": None, "action": "deleted"}
        except gax_exceptions.NotFound:
            log.info(f"pubsub_subscription_asset: subscription {subscription_path!r} already absent.")
            return {"subscription": None, "action": "none"}

    raise ValueError(f"desired_state must be 'present' or 'absent', got {desired_state!r}")


class PubsubSubscriptionAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ensure a Google Cloud Pub/Sub subscription on a topic is present or absent.

    Idempotent against the real client's `AlreadyExists`/`NotFound` errors --
    materializing this asset with `desired_state='present'` a second time is a
    no-op (not an error), and so is `desired_state='absent'` against an
    already-deleted subscription. Omitting `push_endpoint` creates a pull
    subscription; setting it creates a push subscription.
    """

    asset_name: str = Field(description="Dagster asset key.")
    project_id: str = Field(description="GCP project ID.")
    topic_id: str = Field(description="The topic ID this subscription attaches to.")
    subscription_id: str = Field(description="Pub/Sub subscription ID (not the fully-qualified path).")
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Env var pointing to a GCP service account JSON path. If unset, uses Application Default Credentials.",
    )

    desired_state: str = Field(default="present", description="'present' or 'absent'. Idempotent: a no-op if the subscription is already in this state.")
    ack_deadline_seconds: int = Field(default=10, description="Seconds a subscriber has to ack a message before Pub/Sub redelivers it (10-600 per the API). Only applied on create.")
    push_endpoint: Optional[str] = Field(
        default=None,
        description="HTTPS endpoint Pub/Sub pushes messages to. Omit for a pull subscription (the default) -- the common case when a downstream Dagster sensor/asset pulls messages itself.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['gcp', 'pubsub'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys. The owning topic (e.g. from pubsub_topic_asset) is a natural dependency to list here.")

    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'cron' | None.")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description="Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only.",
    )
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(default=0, description="Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the underlying partitions definition (defaults to UTC when unset).")

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps"), csv=("partition_values",), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Pub/Sub Subscription Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.desired_state not in ("present", "absent"):
            raise ValueError(f"desired_state must be 'present' or 'absent', got {self.desired_state!r}")

        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["gcp", "pubsub"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(int(self.freshness_max_lag_minutes), self.freshness_cron)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Ensure Pub/Sub subscription {self.subscription_id!r} on topic {self.topic_id!r} is {self.desired_state}.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _pubsub_subscription_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            client = _make_subscriber_client(self.credentials_env_var)
            topic_path = client.topic_path(self.project_id, self.topic_id)
            subscription_path = client.subscription_path(self.project_id, self.subscription_id)
            result = _ensure_subscription_state(
                client, subscription_path, topic_path, self.desired_state,
                self.ack_deadline_seconds, self.push_endpoint, context.log,
            )

            metadata: Dict[str, Any] = {
                "project_id": dg.MetadataValue.text(self.project_id),
                "topic_id": dg.MetadataValue.text(self.topic_id),
                "subscription_id": dg.MetadataValue.text(self.subscription_id),
                "subscription_path": dg.MetadataValue.text(subscription_path),
                "action": dg.MetadataValue.text(result["action"]),
                "type": dg.MetadataValue.text("push" if self.push_endpoint else "pull"),
            }

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_pubsub_subscription_asset])
