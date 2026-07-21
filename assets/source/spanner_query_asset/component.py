"""SpannerQueryAssetComponent — run a SQL query against Cloud Spanner.

Returns a pandas DataFrame. Useful for:
  - Reading transactional / operational data into a warehouse pipeline
  - Periodic snapshots / CDC-style polling against Spanner
  - Cross-database joins downstream (Spanner + BigQuery side-by-side)

Spanner is GCP's globally-distributed strongly-consistent RDBMS — distinct
from BigQuery (analytic) and Firestore (document). If you're not sure why
you'd query it, you probably want BigQuery instead.
"""

import json
import os
from typing import Any, Dict, List, Optional

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


class SpannerQueryAssetComponent(Component, Model, Resolvable):
    """Run a Spanner SQL query and return rows as a DataFrame."""

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)

    project_id: Optional[str] = Field(default=None)
    instance_id: str = Field(description="Spanner instance id.")
    database_id: str = Field(description="Spanner database id.")

    sql: str = Field(description="The SQL query to run. Spanner GoogleSQL dialect by default.")

    params: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional named parameters for the query (use `@name` in SQL).",
    )
    param_types: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Optional explicit type hints per param (string names: STRING, INT64, FLOAT64, "
            "BOOL, BYTES, TIMESTAMP, DATE, NUMERIC). Default: inferred."
        ),
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
        instance_id = self.instance_id
        database_id = self.database_id
        sql = self.sql
        params = self.params or None
        param_types_in = self.param_types or {}

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Spanner query: {project_id}/{instance_id}/{database_id}.",
            group_name=self.group_name,
            kinds={"google", "spanner"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext):
            try:
                from google.cloud import spanner
                from google.cloud.spanner_v1 import param_types as spanner_param_types
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-spanner google-auth")

            type_map = {
                "STRING":    spanner_param_types.STRING,
                "INT64":     spanner_param_types.INT64,
                "FLOAT64":   spanner_param_types.FLOAT64,
                "BOOL":      spanner_param_types.BOOL,
                "BYTES":     spanner_param_types.BYTES,
                "TIMESTAMP": spanner_param_types.TIMESTAMP,
                "DATE":      spanner_param_types.DATE,
                "NUMERIC":   spanner_param_types.NUMERIC,
            }
            spanner_param_types_dict = {k: type_map[v] for k, v in param_types_in.items() if v in type_map}

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = spanner.Client(project=project_id, credentials=sa_creds)
            instance = client.instance(instance_id)
            database = instance.database(database_id)

            context.log.info(f"Spanner query → {project_id}/{instance_id}/{database_id}")

            with database.snapshot() as snapshot:
                results = snapshot.execute_sql(
                    sql,
                    params=params,
                    param_types=spanner_param_types_dict or None,
                )
                results.fields  # force metadata fetch
                columns = [f.name for f in results.fields]
                rows = list(results)

            df = pd.DataFrame(rows, columns=columns)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no rows)"
            return Output(
                value=df,
                metadata={
                    "instance":  MetadataValue.text(instance_id),
                    "database":  MetadataValue.text(database_id),
                    "row_count": MetadataValue.int(len(df)),
                    "columns":   MetadataValue.json(columns),
                    "preview":   MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
