"""Dataframe To GCS.

Write a DataFrame to Google Cloud Storage as Parquet, CSV, or JSON.
"""
import os
from dataclasses import dataclass
from typing import Optional

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


@dataclass
class DataframeToGcsComponent(Component, Model, Resolvable):
    """Write a DataFrame to Google Cloud Storage as Parquet, CSV, or JSON."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    bucket_env_var: str = Field(default="GCS_BUCKET", description="Env var containing the GCS bucket name")
    blob_path: str = Field(description="Path within bucket e.g. data/output/results.parquet")
    format: str = Field(default="parquet", description="Output format: 'parquet', 'csv', or 'json'")
    credentials_env_var: Optional[str] = Field(
        default=None,
        description="Env var with path to service account JSON. None = application default credentials.",
    )
    compression: Optional[str] = Field(
        default=None,
        description="Compression codec. For parquet: 'snappy', 'gzip'. For csv: 'gzip'. None = default.",
    )
    project_env_var: Optional[str] = Field(
        default=None,
        description="Env var containing the GCP project ID",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Write a DataFrame to Google Cloud Storage as Parquet, CSV, or JSON."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        bucket_env_var = self.bucket_env_var
        blob_path = self.blob_path
        format = self.format
        credentials_env_var = self.credentials_env_var
        compression = self.compression
        project_env_var = self.project_env_var
        group_name = self.group_name

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
            description=DataframeToGcsComponent.get_description(),
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            try:
                import gcsfs
            except ImportError:
                raise ImportError("gcsfs required: pip install gcsfs")

            bucket = os.environ[bucket_env_var]
            full_path = f"gs://{bucket}/{blob_path}"

            storage_options: dict = {}
            if credentials_env_var:
                storage_options["token"] = os.environ[credentials_env_var]
            if project_env_var:
                storage_options["project"] = os.environ[project_env_var]

            if format == "parquet":
                upstream.to_parquet(
                    full_path,
                    compression=compression,
                    storage_options=storage_options or None,
                )
            elif format == "csv":
                upstream.to_csv(
                    full_path,
                    index=False,
                    compression=compression,
                    storage_options=storage_options or None,
                )
            elif format == "json":
                upstream.to_json(
                    full_path,
                    orient="records",
                    lines=True,
                    storage_options=storage_options or None,
                )
            else:
                raise ValueError(f"Unsupported format '{format}'. Must be 'parquet', 'csv', or 'json'.")

            context.log.info(f"Wrote {len(upstream)} rows to GCS path {full_path} as {format}")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "column_count": MetadataValue.int(len(upstream.columns)),
                    "gcs_path": MetadataValue.text(full_path),
                    "format": MetadataValue.text(format),
                }
            )

        return Definitions(assets=[_asset])
