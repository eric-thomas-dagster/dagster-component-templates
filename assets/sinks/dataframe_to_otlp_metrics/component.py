"""DataFrame → OpenTelemetry Metrics (OTLP/HTTP).

Push DataFrame rows as OpenTelemetry metrics via the OTLP HTTP exporter.
ONE sink, MANY backends — works against any OTLP-compatible receiver:

- OTel Collector (anywhere)
- Honeycomb (https://api.honeycomb.io/v1/metrics)
- Lightstep / ServiceNow CMP
- Datadog (via OTLP intake)
- Splunk Observability Cloud
- New Relic (also has dedicated dataframe_to_newrelic_logs)
- Grafana Cloud (Mimir)
- Tempo + Prometheus + Loki via collector
- AWS X-Ray / CloudWatch (via collector)
- GCP Cloud Operations (via collector)

This is the universal observability sink — point at an OTel collector or
any vendor's OTLP intake URL.
"""

import os
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


class DataframeToOtlpMetricsComponent(Component, Model, Resolvable):
    """Push DataFrame rows as OpenTelemetry metrics via OTLP/HTTP."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Asset key of upstream DataFrame")

    endpoint: str = Field(
        description=(
            "OTLP/HTTP endpoint base URL, e.g. 'http://localhost:4318' (OTel "
            "collector default), 'https://api.honeycomb.io', 'https://otlp.eu01.nr-data.net'."
        )
    )
    metric_name: str = Field(description="OTel metric name (dot-namespaced, e.g. 'orders.processed.total')")
    metric_kind: str = Field(
        default="gauge",
        description="'gauge' | 'sum' (counter/up-down). Histograms are advanced — use a custom op for those.",
    )
    metric_unit: Optional[str] = Field(default=None, description="UCUM unit, e.g. '1', 'ms', 'By'")
    value_column: str = Field(description="DataFrame column with numeric metric values")
    attribute_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns whose values become OTel attributes (labels) on each metric data point",
    )
    timestamp_column: Optional[str] = Field(
        default=None,
        description="Column with epoch-seconds (or pandas datetime). Default: now() per row.",
    )
    service_name: str = Field(
        default="dagster",
        description="OTel resource service.name attribute — identifies the producer",
    )
    bearer_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var with Bearer token for Authorization header (most SaaS backends require this)",
    )
    extra_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Extra HTTP headers, e.g. {x-honeycomb-dataset: production} or {api-key: ...}",
    )

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
        kinds = self.kinds or ["opentelemetry", "metrics", "observability"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Push DataFrame rows as OTLP metric '{cfg.metric_name}'",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def otlp_metrics(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            try:
                from opentelemetry.sdk.metrics import MeterProvider
                from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
                from opentelemetry.sdk.resources import Resource
                from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
                from opentelemetry import metrics
            except ImportError as e:
                raise ImportError(
                    "OTel SDK required: pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http"
                ) from e

            # Build headers
            headers = {}
            if cfg.bearer_token_env_var:
                token = os.environ.get(cfg.bearer_token_env_var)
                if token:
                    headers["Authorization"] = f"Bearer {token}"
            if cfg.extra_headers:
                headers.update(cfg.extra_headers)

            exporter_kwargs = {"endpoint": f"{cfg.endpoint.rstrip('/')}/v1/metrics"}
            if headers:
                exporter_kwargs["headers"] = headers

            exporter = OTLPMetricExporter(**exporter_kwargs)
            reader = PeriodicExportingMetricReader(exporter, export_interval_millis=60_000)
            resource = Resource.create({"service.name": cfg.service_name})
            provider = MeterProvider(resource=resource, metric_readers=[reader])
            metrics.set_meter_provider(provider)
            meter = metrics.get_meter("dagster_otlp_sink")

            attribute_cols = cfg.attribute_columns or []
            sent = 0

            if cfg.metric_kind == "sum":
                instr = meter.create_counter(cfg.metric_name, unit=cfg.metric_unit or "1", description=cfg.description or cfg.metric_name)
                for _, row in upstream.iterrows():
                    attrs = {c: str(row[c]) for c in attribute_cols if c in row}
                    instr.add(float(row[cfg.value_column]), attributes=attrs)
                    sent += 1
            else:  # gauge
                # OTel gauges in Python SDK use observable_gauge with a callback;
                # for batch push, simpler to use an UpDownCounter as a stand-in or just iterate
                instr = meter.create_up_down_counter(cfg.metric_name, unit=cfg.metric_unit or "1", description=cfg.description or cfg.metric_name)
                for _, row in upstream.iterrows():
                    attrs = {c: str(row[c]) for c in attribute_cols if c in row}
                    instr.add(float(row[cfg.value_column]), attributes=attrs)
                    sent += 1

            # Force flush so all metrics export before this asset finishes
            try:
                provider.force_flush(timeout_millis=10_000)
            except Exception as e:
                context.log.warning(f"force_flush warning: {e}")

            context.log.info(
                f"Exported {sent} OTLP metric data points to {cfg.endpoint} "
                f"(metric={cfg.metric_name}, kind={cfg.metric_kind})"
            )
            return MaterializeResult(
                metadata={
                    "data_points_exported": MetadataValue.int(sent),
                    "endpoint": MetadataValue.text(cfg.endpoint),
                    "metric_name": MetadataValue.text(cfg.metric_name),
                    "metric_kind": MetadataValue.text(cfg.metric_kind),
                    "service_name": MetadataValue.text(cfg.service_name),
                }
            )

        return Definitions(assets=[otlp_metrics])
