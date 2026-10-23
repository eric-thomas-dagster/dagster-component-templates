"""AWS CloudWatch Metrics → DataFrame.

Run a CloudWatch GetMetricData query and materialize the result as a
DataFrame asset. Counterpart to the Azure azure_log_analytics_query
component, but for AWS CloudWatch Metrics.
"""
import os
from datetime import datetime, timedelta
from typing import Dict, List, Literal, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext, AssetKey, Component, ComponentLoadContext,
    Definitions, MaterializeResult, MetadataValue, Model, Resolvable, asset,
)
from pydantic import Field



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


class AwsCloudwatchMetricsQueryComponent(Component, Model, Resolvable):
    """Query CloudWatch Metrics → DataFrame."""

    asset_name: str = Field(description="Output Dagster asset name")
    region: str = Field(description="AWS region, e.g. 'us-east-1'")
    namespace: str = Field(description="CloudWatch namespace, e.g. 'AWS/Lambda', 'AWS/RDS'")
    metric_name: str = Field(description="Metric name, e.g. 'Invocations', 'CPUUtilization'")
    statistic: str = Field(default="Average", description="'Average' | 'Sum' | 'Maximum' | 'Minimum' | 'SampleCount'")
    period_seconds: int = Field(default=300, description="Aggregation period (must be >= 60)")
    range_minutes: int = Field(default=60, description="How far back to query Ignored on a partitioned time run, which queries exactly its partition's window.")
    dimensions: Optional[Dict[str, str]] = Field(default=None, description="Dimension filters as {Name: Value}")
    aws_access_key_id_env_var: Optional[str] = Field(default=None, description="If unset, uses default boto3 chain (IAM role / env vars / aws config)")
    aws_secret_access_key_env_var: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
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

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "fires one CloudWatch GetMetricStatistics call covering the whole "
            "backfill range (the queried time window spans the full range instead "
            "of one partition's window) rather than one call per partition. "
            "'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with "
            "partition_expr set) -- a generic filesystem/default IO manager "
            "cannot persist a single_run output spanning multiple partitions "
            "and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
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
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

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

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        cfg = self
        kinds = self.kinds or ["aws", "cloudwatch", "metrics"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=self.description or f"CloudWatch {cfg.namespace}/{cfg.metric_name} ({cfg.statistic})",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
        )
        def cw_metrics(context: AssetExecutionContext) -> MaterializeResult:
            try:
                import boto3
            except ImportError as e:
                raise ImportError("boto3 required: pip install boto3") from e

            kwargs = {"region_name": cfg.region}
            if cfg.aws_access_key_id_env_var and cfg.aws_secret_access_key_env_var:
                kwargs["aws_access_key_id"] = _require_env(cfg.aws_access_key_id_env_var)
                kwargs["aws_secret_access_key"] = _require_env(cfg.aws_secret_access_key_env_var)
            client = boto3.client("cloudwatch", **kwargs)

            end = datetime.utcnow()
            start = end - timedelta(minutes=cfg.range_minutes)
            if context.has_partition_key or context.has_partition_key_range:
                # a time partition queries exactly its own window (range_minutes only applies to unpartitioned runs)
                try:
                    _window = context.partition_time_window
                except Exception:
                    _window = None
                if _window is not None:
                    from datetime import timezone as _tz

                    start = _window.start.astimezone(_tz.utc).replace(tzinfo=None)
                    end = _window.end.astimezone(_tz.utc).replace(tzinfo=None)
            dims = [{"Name": k, "Value": v} for k, v in (cfg.dimensions or {}).items()]

            resp = client.get_metric_data(
                MetricDataQueries=[{
                    "Id": "q1",
                    "MetricStat": {
                        "Metric": {"Namespace": cfg.namespace, "MetricName": cfg.metric_name, "Dimensions": dims},
                        "Period": cfg.period_seconds,
                        "Stat": cfg.statistic,
                    },
                    "ReturnData": True,
                }],
                StartTime=start,
                EndTime=end,
                ScanBy="TimestampAscending",
            )
            results = resp.get("MetricDataResults", [])
            rows = []
            for r in results:
                for ts, val in zip(r.get("Timestamps", []), r.get("Values", [])):
                    rows.append({"timestamp": ts, "metric": r.get("Label", cfg.metric_name), "value": val})
            df = pd.DataFrame(rows)
            context.log.info(f"CloudWatch returned {len(df)} samples")

            return MaterializeResult(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "namespace": MetadataValue.text(cfg.namespace),
                    "metric_name": MetadataValue.text(cfg.metric_name),
                    "statistic": MetadataValue.text(cfg.statistic),
                    "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                },
            )

        return Definitions(assets=[cw_metrics])


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
