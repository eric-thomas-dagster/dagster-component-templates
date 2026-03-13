"""DataframeFromSql Component.

Run a SQL query against a database and return the results as a DataFrame.
"""
import os
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class DataframeFromSqlComponent(Component, Model, Resolvable):
    """Run a SQL query and return results as a DataFrame."""

    asset_name: str = Field(description="Output Dagster asset name")
    query: str = Field(description="SQL query to execute")
    database_url_env_var: str = Field(
        default="DATABASE_URL",
        description="Environment variable containing the database connection URL",
    )
    parse_dates: Optional[List[str]] = Field(
        default=None, description="Columns to parse as dates"
    )
    deps: Optional[List[str]] = Field(
        default=None, description="Upstream asset keys for lineage"
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
        return "Run a SQL query and return results as a DataFrame."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        query = self.query
        database_url_env_var = self.database_url_env_var
        parse_dates = self.parse_dates
        deps = self.deps
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
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
            partitions_def=partitions_def,
            group_name=group_name,
            description=DataframeFromSqlComponent.get_description(),
        )
        def _asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            try:
                import sqlalchemy
            except ImportError:
                raise ImportError("sqlalchemy required: pip install sqlalchemy")

            engine = sqlalchemy.create_engine(os.environ[database_url_env_var])
            context.log.info(f"Executing query: {query[:200]}{'...' if len(query) > 200 else ''}")

            df = pd.read_sql(
                query,
                engine,
                parse_dates=parse_dates,
            )

            context.log.info(f"Query returned {len(df)} rows and {len(df.columns)} columns")
            return df

        return Definitions(assets=[_asset])
