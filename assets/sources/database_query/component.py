"""Database Query Asset Component.

Execute SQL queries against databases and materialize results as Dagster assets.
"""

from typing import Optional
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    AssetKey,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


class DatabaseQueryComponent(Component, Model, Resolvable):
    """Component for executing SQL queries and materializing results.

    Execute SQL queries against any SQLAlchemy-compatible database and
    materialize the results as Dagster assets.

    Example:
        ```yaml
        type: dagster_component_templates.DatabaseQueryComponent
        attributes:
          asset_name: daily_sales
          database_url: ${DATABASE_URL}
          query: "SELECT * FROM sales WHERE date = CURRENT_DATE"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset"
    )

    database_url: str = Field(
        description="Database connection URL (use ${DB_URL} for env var)"
    )

    query: str = Field(
        description="SQL query to execute"
    )

    cache_to_parquet: bool = Field(
        default=False,
        description="Whether to cache results to parquet file"
    )

    cache_path: Optional[str] = Field(
        default=None,
        description="Path to parquet cache file (required if cache_to_parquet is True)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
    )
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

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    include_sample_metadata: bool = Field(
        default=False,
        description="Include sample data preview in metadata (first 5 rows as markdown table and interactive preview)"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        database_url = self.database_url
        query = self.query
        cache_to_parquet = self.cache_to_parquet
        cache_path = self.cache_path
        description = self.description or f"Query: {query[:50]}..."
        group_name = self.group_name
        include_sample = self.include_sample_metadata

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
            description=description,
            partitions_def=partitions_def,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def database_query_asset(context: AssetExecutionContext):
            """Asset that executes SQL query and returns results."""

            # Check if running in partitioned mode
            query_params = {}
            if context.has_partition_key:
                # Parse partition key as date (format: YYYY-MM-DD)
                try:
                    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
                    query_params["partition_date"] = partition_date
                    context.log.info(
                        f"Running partitioned query for date: {context.partition_key}"
                    )
                except ValueError:
                    context.log.warning(
                        f"Could not parse partition key '{context.partition_key}' as date"
                    )
            else:
                context.log.info("Running non-partitioned query")

            context.log.info(f"Connecting to database...")
            engine = create_engine(database_url)

            context.log.info(f"Executing query: {query[:100]}...")

            try:
                # Execute query and load results into DataFrame
                # Query params are available as :partition_date in SQL
                df = pd.read_sql(text(query), engine, params=query_params)

                context.log.info(f"Query returned {len(df)} rows")

                # Cache to parquet if requested
                if cache_to_parquet and cache_path:
                    context.log.info(f"Caching results to {cache_path}")
                    df.to_parquet(cache_path, index=False)

                # Add metadata
                context.add_output_metadata({
                    "num_rows": len(df),
                    "num_columns": len(df.columns),
                    "columns": list(df.columns),
                    "query": query,
                })

                if include_sample and len(df) > 0:
                    # Return with sample metadata
                    return Output(
                        value=df,
                        metadata={
                            "row_count": len(df),
                            "columns": df.columns.tolist(),
                            "sample": MetadataValue.md(df.head().to_markdown()),
                            "preview": MetadataValue.dataframe(df.head())
                        }
                    )
                else:
                    return df

            except Exception as e:
                context.log.error(f"Query failed: {e}")
                raise

            finally:
                engine.dispose()

        return Definitions(assets=[database_query_asset])
