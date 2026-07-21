"""BigtableReaderAssetComponent — read rows from a Cloud Bigtable table into a DataFrame.

Bigtable is GCP's wide-column NoSQL. This component pulls a row range (or
prefix scan) into a pandas DataFrame, one Bigtable row per output row,
each column-family/qualifier as a column (`<family>:<qualifier>`).

Supports:
  - row-key prefix scan (`row_key_prefix`)
  - row-key range scan (`start_key` / `end_key`)
  - column-family filter (`column_families`)
  - row limit (`limit`)

For richer filters (cell-version, regex, timestamp range), build a custom
asset on top of `google-cloud-bigtable` — this component covers the common
batch-read patterns.
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


class BigtableReaderAssetComponent(Component, Model, Resolvable):
    """Read rows from a Bigtable table into a DataFrame."""

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)

    project_id: Optional[str] = Field(default=None)
    instance_id: str = Field(description="Bigtable instance id.")
    table_id: str = Field(description="Bigtable table id.")

    row_key_prefix: Optional[str] = Field(
        default=None,
        description="Row-key prefix scan. Mutually exclusive with start_key/end_key.",
    )
    start_key: Optional[str] = Field(default=None, description="Start of row-key range (inclusive).")
    end_key: Optional[str] = Field(default=None, description="End of row-key range (exclusive).")

    column_families: Optional[List[str]] = Field(
        default=None,
        description="Optional list of column families to read. Default: all.",
    )

    limit: Optional[int] = Field(default=None, description="Max rows to return.")

    decode_values_as: str = Field(
        default="utf-8",
        description="How to decode cell values. 'utf-8' (default), 'bytes' (keep raw), or 'json' (parse JSON cells).",
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
        table_id = self.table_id
        row_key_prefix = self.row_key_prefix
        start_key = self.start_key
        end_key = self.end_key
        column_families = self.column_families
        limit = self.limit
        decode_as = self.decode_values_as

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Bigtable read: {project_id}/{instance_id}/{table_id}.",
            group_name=self.group_name,
            kinds={"google", "bigtable"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext):
            try:
                from google.cloud import bigtable
                from google.cloud.bigtable.row_set import RowSet, RowRange
                from google.cloud.bigtable.row_filters import (
                    FamilyNameRegexFilter, RowFilterUnion,
                )
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-bigtable google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigtable.Client(project=project_id, credentials=sa_creds, admin=False)
            instance = client.instance(instance_id)
            table = instance.table(table_id)

            row_set = RowSet()
            if row_key_prefix:
                row_set.add_row_range_with_prefix(row_key_prefix)
            elif start_key or end_key:
                row_set.add_row_range(RowRange(
                    start_key=start_key.encode() if start_key else None,
                    end_key=end_key.encode() if end_key else None,
                ))

            row_filter = None
            if column_families:
                filters = [FamilyNameRegexFilter(f"^{cf}$") for cf in column_families]
                row_filter = filters[0] if len(filters) == 1 else RowFilterUnion(filters)

            context.log.info(f"Bigtable scan → {project_id}/{instance_id}/{table_id}")

            rows = table.read_rows(row_set=row_set, filter_=row_filter, limit=limit)
            out_rows: List[Dict[str, Any]] = []
            for r in rows:
                out: Dict[str, Any] = {"_row_key": r.row_key.decode("utf-8", errors="replace")}
                for family_name, cols in (r.cells or {}).items():
                    for col_qual, cells in cols.items():
                        if not cells:
                            continue
                        cell = cells[0]  # latest version only
                        col_name = f"{family_name}:{col_qual.decode('utf-8', errors='replace')}"
                        raw = cell.value
                        if decode_as == "bytes":
                            out[col_name] = raw
                        elif decode_as == "json":
                            try:
                                out[col_name] = json.loads(raw.decode("utf-8"))
                            except Exception:
                                out[col_name] = raw.decode("utf-8", errors="replace")
                        else:
                            out[col_name] = raw.decode(decode_as, errors="replace")
                out_rows.append(out)

            df = pd.DataFrame(out_rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no rows)"
            return Output(
                value=df,
                metadata={
                    "instance":  MetadataValue.text(instance_id),
                    "table":     MetadataValue.text(table_id),
                    "row_count": MetadataValue.int(len(df)),
                    "columns":   MetadataValue.json(list(df.columns) if not df.empty else []),
                    "preview":   MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
