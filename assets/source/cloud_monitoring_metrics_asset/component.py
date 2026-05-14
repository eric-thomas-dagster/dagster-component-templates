"""CloudMonitoringMetricsAssetComponent — pull GCP time-series metrics into a DataFrame.

Wraps Cloud Monitoring's ListTimeSeries API. Useful for:
  - Cost / usage analytics (cross-checking GCP bills against actual usage)
  - SLO compliance roll-ups outside the Monitoring console
  - Driving alerting / anomaly-detection assets downstream
  - Building dashboards in non-GCP tools (Superset, Looker Studio, etc.)

Metric type discovery:
  https://cloud.google.com/monitoring/api/metrics_gcp

Filter syntax (MQL-equivalent ListTimeSeries filter):
  metric.type="compute.googleapis.com/instance/cpu/utilization"
  resource.type="cloud_run_revision" AND metric.type="run.googleapis.com/request_count"
"""

import datetime as dt
import json
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
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


_ALIGNERS = {
    "NONE":           "ALIGN_NONE",
    "MEAN":           "ALIGN_MEAN",
    "MIN":            "ALIGN_MIN",
    "MAX":            "ALIGN_MAX",
    "SUM":            "ALIGN_SUM",
    "COUNT":          "ALIGN_COUNT",
    "RATE":           "ALIGN_RATE",
    "DELTA":          "ALIGN_DELTA",
    "PERCENT_CHANGE": "ALIGN_PERCENT_CHANGE",
    "PERCENTILE_99":  "ALIGN_PERCENTILE_99",
    "PERCENTILE_95":  "ALIGN_PERCENTILE_95",
    "PERCENTILE_50":  "ALIGN_PERCENTILE_50",
}

# MetricDescriptor.ValueType enum (from google.api.metric_pb2)
_VALUE_TYPE_NAMES = {
    0: "VALUE_TYPE_UNSPECIFIED",
    1: "BOOL",
    2: "INT64",
    3: "DOUBLE",
    4: "STRING",
    5: "DISTRIBUTION",
    6: "MONEY",
}

def _ts_to_datetime(ts) -> Any:
    """Cloud Monitoring may return either a Timestamp proto (with ToDatetime)
    or a DatetimeWithNanoseconds (already a datetime). Normalize."""
    if hasattr(ts, "ToDatetime"):
        return ts.ToDatetime()
    return ts


_REDUCERS = {
    "NONE":           "REDUCE_NONE",
    "MEAN":           "REDUCE_MEAN",
    "MIN":            "REDUCE_MIN",
    "MAX":            "REDUCE_MAX",
    "SUM":            "REDUCE_SUM",
    "COUNT":          "REDUCE_COUNT",
    "PERCENTILE_99":  "REDUCE_PERCENTILE_99",
    "PERCENTILE_95":  "REDUCE_PERCENTILE_95",
    "PERCENTILE_50":  "REDUCE_PERCENTILE_50",
}


class CloudMonitoringMetricsAssetComponent(Component, Model, Resolvable):
    """Query Cloud Monitoring time-series and return as a DataFrame."""

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)

    project_id: Optional[str] = Field(default=None)

    filter_: str = Field(
        alias="filter",
        description=(
            "Cloud Monitoring filter. Must include a `metric.type=...` clause. "
            'Example: \'metric.type="compute.googleapis.com/instance/cpu/utilization"\'.'
        ),
    )

    lookback_minutes: int = Field(default=60, description="Time window (minutes from now backwards).")

    alignment_period_seconds: int = Field(default=60, description="Bucket size for per-series alignment.")
    aligner: Literal[
        "NONE", "MEAN", "MIN", "MAX", "SUM", "COUNT", "RATE", "DELTA",
        "PERCENT_CHANGE", "PERCENTILE_99", "PERCENTILE_95", "PERCENTILE_50",
    ] = Field(default="MEAN")

    cross_series_reducer: Optional[Literal[
        "NONE", "MEAN", "MIN", "MAX", "SUM", "COUNT",
        "PERCENTILE_99", "PERCENTILE_95", "PERCENTILE_50",
    ]] = Field(
        default=None,
        description="Optional cross-series reducer (combines all series matching the filter).",
    )
    group_by_fields: Optional[List[str]] = Field(
        default=None,
        description="Resource labels to retain when reducing (e.g. ['resource.labels.zone']).",
    )

    view: Literal["FULL", "HEADERS"] = Field(
        default="FULL",
        description="FULL returns data points; HEADERS returns series metadata only.",
    )

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
        project_id = self.project_id or creds_dict.get("project_id")
        filter_text = self.filter_
        lookback_minutes = self.lookback_minutes
        alignment_period_seconds = self.alignment_period_seconds
        aligner_key = self.aligner
        cross_reducer_key = self.cross_series_reducer
        group_by_fields = self.group_by_fields or []
        view = self.view

        @asset(
            name=asset_name,
            description=self.description or f"Cloud Monitoring time-series in {project_id}.",
            group_name=self.group_name,
            kinds={"google", "cloud-monitoring"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            try:
                from google.cloud import monitoring_v3
                from google.oauth2 import service_account
                from google.protobuf import timestamp_pb2
            except ImportError:
                raise ImportError("pip install google-cloud-monitoring google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = monitoring_v3.MetricServiceClient(credentials=sa_creds)

            now = dt.datetime.now(dt.timezone.utc)
            start = now - dt.timedelta(minutes=lookback_minutes)

            def _to_ts(dt_):
                ts = timestamp_pb2.Timestamp()
                ts.FromDatetime(dt_)
                return ts

            interval = monitoring_v3.TimeInterval({
                "start_time": _to_ts(start),
                "end_time":   _to_ts(now),
            })

            aggregation = monitoring_v3.Aggregation({
                "alignment_period": {"seconds": alignment_period_seconds},
                "per_series_aligner": getattr(monitoring_v3.Aggregation.Aligner, _ALIGNERS[aligner_key]),
                **({
                    "cross_series_reducer": getattr(monitoring_v3.Aggregation.Reducer, _REDUCERS[cross_reducer_key]),
                    "group_by_fields": group_by_fields,
                } if cross_reducer_key else {}),
            })

            context.log.info(f"Cloud Monitoring filter: {filter_text}")
            context.log.info(f"Window: {start.isoformat()} → {now.isoformat()}")

            view_enum = (
                monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
                if view == "FULL" else
                monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.HEADERS
            )

            rows: List[Dict[str, Any]] = []
            series_count = 0
            for ts_series in client.list_time_series(
                request={
                    "name":        f"projects/{project_id}",
                    "filter":      filter_text,
                    "interval":    interval,
                    "aggregation": aggregation,
                    "view":        view_enum,
                }
            ):
                series_count += 1
                metric_type = ts_series.metric.type
                metric_labels = dict(ts_series.metric.labels) if ts_series.metric.labels else {}
                resource_type = ts_series.resource.type
                resource_labels = dict(ts_series.resource.labels) if ts_series.resource.labels else {}
                unit = ts_series.unit
                value_type = _VALUE_TYPE_NAMES.get(int(ts_series.value_type), str(ts_series.value_type))
                for point in ts_series.points or []:
                    interval_ = point.interval
                    val = point.value
                    if val.double_value:
                        v = float(val.double_value)
                    elif val.int64_value:
                        v = int(val.int64_value)
                    elif val.bool_value:
                        v = bool(val.bool_value)
                    elif val.string_value:
                        v = val.string_value
                    else:
                        v = None
                    rows.append({
                        "metric_type":     metric_type,
                        "metric_labels":   metric_labels,
                        "resource_type":   resource_type,
                        "resource_labels": resource_labels,
                        "value":           v,
                        "value_type":      value_type,
                        "unit":            unit,
                        "start_time":      _ts_to_datetime(interval_.start_time) if interval_.start_time else None,
                        "end_time":        _ts_to_datetime(interval_.end_time) if interval_.end_time else None,
                    })

            df = pd.DataFrame(rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no points)"
            return Output(
                value=df,
                metadata={
                    "filter":         MetadataValue.text(filter_text),
                    "series_count":   MetadataValue.int(series_count),
                    "point_count":    MetadataValue.int(len(df)),
                    "window_minutes": MetadataValue.int(lookback_minutes),
                    "aligner":        MetadataValue.text(aligner_key),
                    "preview":        MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
