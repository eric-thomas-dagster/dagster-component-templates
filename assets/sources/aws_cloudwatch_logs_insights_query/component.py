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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
