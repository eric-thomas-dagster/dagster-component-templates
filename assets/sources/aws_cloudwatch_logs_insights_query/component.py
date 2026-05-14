"""AWS CloudWatch Logs Insights → DataFrame.

Run a Logs Insights query against CloudWatch Logs and materialize the
result as a DataFrame asset. Logs Insights uses its own SQL-like query
language; works with structured + unstructured logs across log groups.
"""
import os, time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext, AssetKey, Component, ComponentLoadContext,
    Definitions, MaterializeResult, MetadataValue, Model, Resolvable, asset,
)
from pydantic import Field


class AwsCloudwatchLogsInsightsQueryComponent(Component, Model, Resolvable):
    """Run a Logs Insights query → DataFrame."""

    asset_name: str = Field(description="Output Dagster asset name")
    region: str = Field(description="AWS region")
    log_group_names: List[str] = Field(description="One or more log group names to query")
    query_string: str = Field(description="Logs Insights query (e.g. 'fields @timestamp, @message | filter @message like /ERROR/ | limit 100')")
    range_minutes: int = Field(default=60)
    aws_access_key_id_env_var: Optional[str] = Field(default=None)
    aws_secret_access_key_env_var: Optional[str] = Field(default=None)
    max_wait_seconds: int = Field(default=60, description="Max wait for query to complete")
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
        kinds = self.kinds or ["aws", "cloudwatch", "logs"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Logs Insights query against {len(cfg.log_group_names)} log group(s)",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def cw_logs_insights(context: AssetExecutionContext) -> MaterializeResult:
            try:
                import boto3
            except ImportError as e:
                raise ImportError("boto3 required: pip install boto3") from e

            kwargs = {"region_name": cfg.region}
            if cfg.aws_access_key_id_env_var and cfg.aws_secret_access_key_env_var:
                kwargs["aws_access_key_id"] = os.environ[cfg.aws_access_key_id_env_var]
                kwargs["aws_secret_access_key"] = os.environ[cfg.aws_secret_access_key_env_var]
            client = boto3.client("logs", **kwargs)

            end = int(time.time())
            start = int((datetime.utcnow() - timedelta(minutes=cfg.range_minutes)).timestamp())

            start_resp = client.start_query(
                logGroupNames=cfg.log_group_names,
                startTime=start, endTime=end,
                queryString=cfg.query_string,
            )
            query_id = start_resp["queryId"]

            elapsed = 0
            while elapsed < cfg.max_wait_seconds:
                status_resp = client.get_query_results(queryId=query_id)
                status = status_resp["status"]
                if status == "Complete":
                    rows = []
                    for record in status_resp.get("results", []):
                        rows.append({field["field"]: field["value"] for field in record})
                    df = pd.DataFrame(rows)
                    context.log.info(f"Logs Insights returned {len(df)} rows")
                    return MaterializeResult(
                        value=df,
                        metadata={
                            "row_count": MetadataValue.int(len(df)),
                            "log_group_names": MetadataValue.text(", ".join(cfg.log_group_names)),
                            "query_string": MetadataValue.text(cfg.query_string[:500]),
                            "preview": MetadataValue.md(df.head(20).to_markdown(index=False) if len(df) > 0 else "(empty)"),
                        },
                    )
                elif status in ("Failed", "Cancelled"):
                    raise Exception(f"Query {status}: {status_resp}")
                time.sleep(2)
                elapsed += 2
            raise Exception(f"Query timed out after {cfg.max_wait_seconds}s")

        return Definitions(assets=[cw_logs_insights])
