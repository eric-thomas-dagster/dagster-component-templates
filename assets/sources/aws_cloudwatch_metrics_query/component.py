"""AWS CloudWatch Metrics → DataFrame.

Run a CloudWatch GetMetricData query and materialize the result as a
DataFrame asset. Counterpart to the Azure azure_log_analytics_query
component, but for AWS CloudWatch Metrics.
"""
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext, AssetKey, Component, ComponentLoadContext,
    Definitions, MaterializeResult, MetadataValue, Model, Resolvable, asset,
)
from pydantic import Field


class AwsCloudwatchMetricsQueryComponent(Component, Model, Resolvable):
    """Query CloudWatch Metrics → DataFrame."""

    asset_name: str = Field(description="Output Dagster asset name")
    region: str = Field(description="AWS region, e.g. 'us-east-1'")
    namespace: str = Field(description="CloudWatch namespace, e.g. 'AWS/Lambda', 'AWS/RDS'")
    metric_name: str = Field(description="Metric name, e.g. 'Invocations', 'CPUUtilization'")
    statistic: str = Field(default="Average", description="'Average' | 'Sum' | 'Maximum' | 'Minimum' | 'SampleCount'")
    period_seconds: int = Field(default=300, description="Aggregation period (must be >= 60)")
    range_minutes: int = Field(default=60, description="How far back to query")
    dimensions: Optional[Dict[str, str]] = Field(default=None, description="Dimension filters as {Name: Value}")
    aws_access_key_id_env_var: Optional[str] = Field(default=None, description="If unset, uses default boto3 chain (IAM role / env vars / aws config)")
    aws_secret_access_key_env_var: Optional[str] = Field(default=None)
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
        kinds = self.kinds or ["aws", "cloudwatch", "metrics"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"CloudWatch {cfg.namespace}/{cfg.metric_name} ({cfg.statistic})",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def cw_metrics(context: AssetExecutionContext) -> MaterializeResult:
            try:
                import boto3
            except ImportError as e:
                raise ImportError("boto3 required: pip install boto3") from e

            kwargs = {"region_name": cfg.region}
            if cfg.aws_access_key_id_env_var and cfg.aws_secret_access_key_env_var:
                kwargs["aws_access_key_id"] = os.environ[cfg.aws_access_key_id_env_var]
                kwargs["aws_secret_access_key"] = os.environ[cfg.aws_secret_access_key_env_var]
            client = boto3.client("cloudwatch", **kwargs)

            end = datetime.utcnow()
            start = end - timedelta(minutes=cfg.range_minutes)
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
