"""Database Query Asset Component.

Execute SQL queries against databases and materialize results as Dagster assets.
"""

from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    AssetSpec,
    multi_asset,
)
from pydantic import BaseModel, Field


class DatabaseQueryComponent(Component, BaseModel):
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        database_url = self.database_url
        query = self.query
        cache_to_parquet = self.cache_to_parquet
        cache_path = self.cache_path
        description = self.description or f"Query: {query[:50]}..."
        group_name = self.group_name

        @multi_asset(
            name=f"{asset_name}_asset",
            specs=[
                AssetSpec(
                    key=asset_name,
                    description=description,
                    group_name=group_name,
                )
            ],
        )
        def database_query_asset(context: AssetExecutionContext):
            """Asset that executes SQL query and returns results."""

            context.log.info(f"Connecting to database...")
            engine = create_engine(database_url)

            context.log.info(f"Executing query: {query[:100]}...")

            try:
                # Execute query and load results into DataFrame
                df = pd.read_sql(text(query), engine)

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

                return df

            except Exception as e:
                context.log.error(f"Query failed: {e}")
                raise

            finally:
                engine.dispose()

        return Definitions(assets=[database_query_asset])
