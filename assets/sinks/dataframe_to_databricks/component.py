"""Dataframe To Databricks.

Write a DataFrame to a Databricks Delta Lake table.
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
class DataframeToDatabricksComponent(Component, Model, Resolvable):
    """Write a DataFrame to a Databricks Delta Lake table."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    catalog: str = Field(default="main", description="Unity Catalog name")
    schema: str = Field(description="Target schema/database name")
    table: str = Field(description="Target table name")
    host_env_var: str = Field(default="DATABRICKS_HOST", description="Env var containing Databricks workspace URL")
    token_env_var: str = Field(default="DATABRICKS_TOKEN", description="Env var containing Databricks personal access token")
    mode: str = Field(default="overwrite", description="Write mode: 'overwrite', 'append', 'ignore', 'error'")
    merge_schema: bool = Field(default=True, description="Allow schema evolution when appending")
    cluster_id: Optional[str] = Field(default=None, description="SQL warehouse or cluster ID (None = auto)")
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
        return "Write a DataFrame to a Databricks Delta Lake table."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        catalog = self.catalog
        schema = self.schema
        table = self.table
        host_env_var = self.host_env_var
        token_env_var = self.token_env_var
        mode = self.mode
        merge_schema = self.merge_schema
        cluster_id = self.cluster_id
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
            description=DataframeToDatabricksComponent.get_description(),
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
                from databricks import sql as databricks_sql
            except ImportError:
                raise ImportError(
                    "databricks-sql-connector required: pip install databricks-sql-connector"
                )

            host = os.environ[host_env_var]
            token = os.environ[token_env_var]
            full_table = f"{catalog}.{schema}.{table}"
            http_path = f"/sql/1.0/warehouses/{cluster_id or 'auto'}"

            with databricks_sql.connect(
                server_hostname=host,
                http_path=http_path,
                access_token=token,
            ) as conn:
                with conn.cursor() as cursor:
                    if mode == "overwrite":
                        cols = ", ".join([f"`{c}` STRING" for c in upstream.columns])
                        cursor.execute(f"CREATE OR REPLACE TABLE {full_table} ({cols})")
                    elif mode == "error":
                        cursor.execute(
                            f"SELECT COUNT(*) FROM information_schema.tables "
                            f"WHERE table_catalog='{catalog}' AND table_schema='{schema}' AND table_name='{table}'"
                        )
                        if cursor.fetchone()[0] > 0:
                            raise RuntimeError(
                                f"Table {full_table} already exists and mode='error'"
                            )

                    # Insert rows in batches of 1000
                    batch_size = 1000
                    total_rows = 0
                    for i in range(0, len(upstream), batch_size):
                        batch = upstream.iloc[i : i + batch_size]
                        placeholders = ", ".join(["?" for _ in range(len(batch.columns))])
                        values = [tuple(row) for row in batch.itertuples(index=False)]
                        cursor.executemany(
                            f"INSERT INTO {full_table} VALUES ({placeholders})", values
                        )
                        total_rows += len(batch)

            context.log.info(f"Wrote {total_rows} rows to Databricks table {full_table}")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(total_rows),
                    "column_count": MetadataValue.int(len(upstream.columns)),
                    "table": MetadataValue.text(full_table),
                    "mode": MetadataValue.text(mode),
                }
            )

        return Definitions(assets=[_asset])
