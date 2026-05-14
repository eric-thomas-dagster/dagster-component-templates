"""DataFrame → OpenTelemetry Traces (OTLP/HTTP).

Push DataFrame rows as OpenTelemetry spans via the OTLP HTTP exporter.
Use case: emit synthetic spans for batch jobs / ETL stages so they
show up in your tracing backend (Honeycomb, Lightstep, Tempo, Jaeger,
Datadog APM via OTLP intake) alongside live application traces.

Each row → one span. Configure trace_id (groups spans into a trace),
parent_span_id (builds the tree), name, start_time, end_time,
attributes.
"""

import os
import time
import uuid
from typing import Dict, List, Optional

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


class DataframeToOtlpTracesComponent(Component, Model, Resolvable):
    """Push DataFrame rows as OpenTelemetry spans via OTLP/HTTP."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    endpoint: str = Field(description="OTLP/HTTP endpoint base URL")
    service_name: str = Field(default="dagster")

    span_name_column: str = Field(description="Column whose value becomes the span name")
    start_time_column: Optional[str] = Field(
        default=None,
        description="Column with span start (epoch seconds or datetime). Default: now()",
    )
    end_time_column: Optional[str] = Field(
        default=None,
        description="Column with span end. Default: start + duration_ms_column or 1ms after start",
    )
    duration_ms_column: Optional[str] = Field(
        default=None,
        description="Column with duration in ms (used if end_time_column is unset)",
    )
    trace_id_column: Optional[str] = Field(
        default=None,
        description="Column to derive trace IDs (rows with same value share a trace). Default: one trace per push.",
    )
    parent_span_id_column: Optional[str] = Field(
        default=None,
        description="Column with parent span IDs to build the span tree",
    )
    attribute_columns: Optional[List[str]] = Field(default=None)
    bearer_token_env_var: Optional[str] = Field(default=None)
    extra_headers: Optional[Dict[str, str]] = Field(default=None)

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

        cfg = self
        kinds = self.kinds or ["opentelemetry", "traces", "observability"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or "Push DataFrame rows as OTLP spans",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def otlp_traces(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            try:
                from opentelemetry.sdk.trace import TracerProvider
                from opentelemetry.sdk.trace.export import BatchSpanProcessor
                from opentelemetry.sdk.resources import Resource
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
                from opentelemetry import trace
                from opentelemetry.trace import SpanContext, TraceFlags
            except ImportError as e:
                raise ImportError(
                    "OTel SDK required: pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http"
                ) from e

            headers = {}
            if cfg.bearer_token_env_var:
                token = os.environ.get(cfg.bearer_token_env_var)
                if token:
                    headers["Authorization"] = f"Bearer {token}"
            if cfg.extra_headers:
                headers.update(cfg.extra_headers)

            exp_kwargs = {"endpoint": f"{cfg.endpoint.rstrip('/')}/v1/traces"}
            if headers:
                exp_kwargs["headers"] = headers

            exporter = OTLPSpanExporter(**exp_kwargs)
            resource = Resource.create({"service.name": cfg.service_name})
            provider = TracerProvider(resource=resource)
            provider.add_span_processor(BatchSpanProcessor(exporter))
            trace.set_tracer_provider(provider)
            tracer = trace.get_tracer("dagster_otlp_sink")

            attribute_cols = cfg.attribute_columns or []
            now = time.time_ns()
            single_trace_id = int(uuid.uuid4().int >> 64)

            sent = 0
            for _, row in upstream.iterrows():
                # Determine trace_id (consistent across rows with same trace_id_column value)
                if cfg.trace_id_column and cfg.trace_id_column in row:
                    seed = str(row[cfg.trace_id_column])
                    trace_id = int.from_bytes(uuid.uuid5(uuid.NAMESPACE_OID, seed).bytes[:16], "big")
                else:
                    trace_id = single_trace_id

                start = now
                if cfg.start_time_column and cfg.start_time_column in row:
                    val = row[cfg.start_time_column]
                    start = int(val * 1e9) if isinstance(val, (int, float)) else int(pd.to_datetime(val).timestamp() * 1e9)
                if cfg.end_time_column and cfg.end_time_column in row:
                    val = row[cfg.end_time_column]
                    end = int(val * 1e9) if isinstance(val, (int, float)) else int(pd.to_datetime(val).timestamp() * 1e9)
                elif cfg.duration_ms_column and cfg.duration_ms_column in row:
                    end = start + int(float(row[cfg.duration_ms_column]) * 1e6)
                else:
                    end = start + 1_000_000  # 1ms

                span_name = str(row[cfg.span_name_column])
                attrs = {c: str(row[c]) for c in attribute_cols if c in row}

                span = tracer.start_span(span_name, start_time=start, attributes=attrs)
                try:
                    pass
                finally:
                    span.end(end_time=end)
                sent += 1

            try:
                provider.force_flush(timeout_millis=10_000)
            except Exception as e:
                context.log.warning(f"force_flush warning: {e}")

            context.log.info(f"Exported {sent} OTLP spans to {cfg.endpoint}")
            return MaterializeResult(
                metadata={
                    "spans_exported": MetadataValue.int(sent),
                    "endpoint": MetadataValue.text(cfg.endpoint),
                    "service_name": MetadataValue.text(cfg.service_name),
                }
            )

        return Definitions(assets=[otlp_traces])
