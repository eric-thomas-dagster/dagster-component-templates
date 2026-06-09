"""BigtableWriterAssetComponent — write DataFrame rows to a Cloud Bigtable table.

For each row of an upstream DataFrame, write one Bigtable row. The row key
comes from `row_key_column`. Other columns are written as cells under their
configured column family + qualifier (qualifier defaults to the column name).

Bigtable cells are bytes; values are utf-8-encoded by default. Use
`json_columns` to JSON-serialize dict/list cells before encoding.
"""

import json
import os
from typing import Any, Dict, List, Optional

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


class BigtableWriterAssetComponent(Component, Model, Resolvable):
    """Write rows of an upstream DataFrame into a Bigtable table."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)

    project_id: Optional[str] = Field(default=None)
    instance_id: str = Field(description="Bigtable instance id.")
    table_id: str = Field(description="Bigtable table id.")

    row_key_column: str = Field(description="DataFrame column to use as the Bigtable row key.")

    column_family: str = Field(
        description="Default column family for all non-row-key columns (unless overridden in column_map).",
    )

    column_map: Optional[Dict[str, Dict[str, str]]] = Field(
        default=None,
        description=(
            "Optional per-column override: {<df_column>: {family: '...', qualifier: '...'}}. "
            "Default: family=column_family, qualifier=df_column."
        ),
    )

    json_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns whose values should be json.dumps()'d before encoding.",
    )

    batch_size: int = Field(default=500, description="Rows per Bigtable mutate batch.")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
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
        instance_id = self.instance_id
        table_id = self.table_id
        row_key_column = self.row_key_column
        default_family = self.column_family
        column_map = self.column_map or {}
        json_columns = set(self.json_columns or [])
        batch_size = self.batch_size

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Bigtable write: {project_id}/{instance_id}/{table_id}.",
            group_name=self.group_name,
            kinds={"google", "bigtable"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            try:
                from google.cloud import bigtable
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-bigtable google-auth")

            if row_key_column not in upstream.columns:
                raise ValueError(f"row_key_column={row_key_column!r} not in upstream: {list(upstream.columns)}")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigtable.Client(project=project_id, credentials=sa_creds, admin=False)
            instance = client.instance(instance_id)
            table = instance.table(table_id)

            data_cols = [c for c in upstream.columns if c != row_key_column]

            def _encode(v: Any, col: str) -> bytes:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return b""
                if col in json_columns or isinstance(v, (dict, list)):
                    return json.dumps(v, default=str).encode("utf-8")
                return str(v).encode("utf-8")

            written = 0
            batch_rows = []
            for _, row in upstream.iterrows():
                rk = str(row[row_key_column]).encode("utf-8")
                bt_row = table.direct_row(rk)
                for col in data_cols:
                    mapping = column_map.get(col, {})
                    family = str(mapping.get("family", default_family))
                    qualifier = str(mapping.get("qualifier", col))
                    bt_row.set_cell(family, qualifier.encode("utf-8"), _encode(row[col], col))
                batch_rows.append(bt_row)
                if len(batch_rows) >= batch_size:
                    table.mutate_rows(batch_rows)
                    written += len(batch_rows)
                    batch_rows = []
            if batch_rows:
                table.mutate_rows(batch_rows)
                written += len(batch_rows)

            context.log.info(f"Bigtable wrote {written} rows to {project_id}/{instance_id}/{table_id}")
            return Output(
                value=pd.DataFrame({"metric": ["rows_written"], "value": [written]}),
                metadata={
                    "instance":     MetadataValue.text(instance_id),
                    "table":        MetadataValue.text(table_id),
                    "rows_written": MetadataValue.int(written),
                    "families":     MetadataValue.json(sorted({m.get("family", default_family) for m in column_map.values()} | {default_family})),
                },
            )

        return Definitions(assets=[_asset])
