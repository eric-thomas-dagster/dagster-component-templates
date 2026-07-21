"""PubSubPublishAssetComponent — publish DataFrame rows as Pub/Sub messages.

Each row of the upstream DataFrame becomes one Pub/Sub message. By
default the entire row is JSON-serialized as the message body. Use
`message_column` to publish a single column instead, and
`attribute_columns` to map columns into Pub/Sub message attributes
(useful for downstream filtering / routing on subscribers).
"""

import json
import os
import time
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class PubSubPublishAssetComponent(Component, Model, Resolvable):
    """Publish DataFrame rows to a Pub/Sub topic — one message per row."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None, description="GCP project. Defaults to the SA's project.")
    topic: str = Field(description="Pub/Sub topic name (no `projects/.../topics/` prefix).")
    auto_create_topic: bool = Field(
        default=False,
        description="If True, create the topic if it doesn't exist (requires roles/pubsub.editor).",
    )

    message_column: Optional[Union[str, int]] = Field(
        default=None,
        description=(
            "If set, publish only this column's value per row. Default: JSON-serialize "
            "the whole row (drop attribute_columns + ordering_key_column from the body)."
        ),
    )
    attribute_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to attach as Pub/Sub message attributes (string-coerced). Useful for downstream filtering.",
    )
    ordering_key_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional column to use as Pub/Sub ordering_key (requires the topic to have message ordering enabled).",
    )

    flush_batch_size: int = Field(default=100, description="Flush publishes every N messages.")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

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

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        project_id = self.project_id or creds_dict.get("project_id")
        topic = self.topic
        auto_create_topic = self.auto_create_topic
        message_column = self.message_column
        attribute_columns = list(self.attribute_columns or [])
        ordering_key_column = self.ordering_key_column
        flush_batch_size = self.flush_batch_size

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Publish to Pub/Sub topic {project_id}/{topic}.",
            group_name=self.group_name,
            kinds={"google", "pubsub"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            try:
                from google.cloud import pubsub_v1
                from google.oauth2 import service_account
                from google.api_core.exceptions import AlreadyExists, NotFound
            except ImportError:
                raise ImportError("pip install google-cloud-pubsub google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            # If ordering_key_column is set, the publisher client MUST be created
            # with message-ordering enabled, AND the subscription must have
            # `enableMessageOrdering=true` (set at sub creation, immutable).
            if ordering_key_column:
                publisher = pubsub_v1.PublisherClient(
                    credentials=sa_creds,
                    publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True),
                )
            else:
                publisher = pubsub_v1.PublisherClient(credentials=sa_creds)
            topic_path = publisher.topic_path(project_id, topic)

            # Optional auto-create.
            if auto_create_topic:
                try:
                    publisher.create_topic(request={"name": topic_path})
                    context.log.info(f"created Pub/Sub topic {topic_path}")
                except AlreadyExists:
                    pass
                except Exception as e:
                    context.log.warning(f"auto_create_topic: {e}")

            df = upstream.copy().reset_index(drop=True)
            if df.empty:
                return Output(
                    value=pd.DataFrame(columns=["pubsub_message_id", "_error"]),
                    metadata={
                        "topic":         MetadataValue.text(topic_path),
                        "messages_sent": MetadataValue.int(0),
                    },
                )

            # Validate referenced columns.
            for col in [c for c in [message_column, ordering_key_column] if c]:
                if col not in df.columns:
                    raise ValueError(f"column {col!r} not in upstream: {list(df.columns)}")
            missing_attrs = [c for c in attribute_columns if c not in df.columns]
            if missing_attrs:
                raise ValueError(f"attribute_columns missing from upstream: {missing_attrs}")

            futures = []
            errors: List[Optional[str]] = []
            message_ids: List[Optional[str]] = []

            for i, row in df.iterrows():
                # Build message body.
                if message_column:
                    body = row[message_column]
                    if isinstance(body, (dict, list)):
                        data = json.dumps(body, default=str).encode("utf-8")
                    elif isinstance(body, bytes):
                        data = body
                    else:
                        data = str(body).encode("utf-8")
                else:
                    drop_cols = set(attribute_columns + ([ordering_key_column] if ordering_key_column else []))
                    payload = {k: v for k, v in row.items() if k not in drop_cols}
                    # Make pandas/numpy types JSON-friendly.
                    data = json.dumps(payload, default=str).encode("utf-8")

                attrs = {c: str(row[c]) for c in attribute_columns}
                pub_kwargs: Dict[str, Any] = {}
                if ordering_key_column:
                    pub_kwargs["ordering_key"] = str(row[ordering_key_column])

                try:
                    fut = publisher.publish(topic_path, data, **attrs, **pub_kwargs)
                    futures.append(fut)
                except Exception as e:
                    errors.append(str(e))
                    message_ids.append(None)
                    continue

                # Periodic flush.
                if len(futures) >= flush_batch_size:
                    for f in futures:
                        try:
                            mid = f.result(timeout=60)
                            message_ids.append(mid)
                            errors.append(None)
                        except Exception as e:
                            message_ids.append(None)
                            errors.append(str(e))
                    futures = []

            # Final flush.
            for f in futures:
                try:
                    mid = f.result(timeout=60)
                    message_ids.append(mid)
                    errors.append(None)
                except Exception as e:
                    message_ids.append(None)
                    errors.append(str(e))

            # Detect topic-not-found early so users know to flip auto_create_topic.
            sample_err = next((e for e in errors if e), None)
            if sample_err and ("NotFound" in sample_err or "404" in sample_err):
                context.log.error(
                    f"Pub/Sub topic {topic_path!r} not found. Set "
                    f"`auto_create_topic: true` in YAML, or run: "
                    f"`gcloud pubsub topics create {topic} --project={project_id}`."
                )

            df["pubsub_message_id"] = message_ids
            if any(errors):
                df["_error"] = errors

            sent = sum(1 for m in message_ids if m is not None)
            return Output(
                value=df,
                metadata={
                    "topic":         MetadataValue.text(topic_path),
                    "messages_sent": MetadataValue.int(sent),
                    "messages_failed": MetadataValue.int(len(message_ids) - sent),
                    "preview":       MetadataValue.md(df.head(5).to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
