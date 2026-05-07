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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
