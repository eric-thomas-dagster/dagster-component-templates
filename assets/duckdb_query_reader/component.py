"""DuckDB Query Reader Asset Component."""

from typing import Optional
import pandas as pd
from pathlib import Path
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetSpec,
    AssetExecutionContext,
    ComponentLoadContext,
    multi_asset,
)
from pydantic import Field


class DuckDBQueryReaderComponent(Component, Model, Resolvable):
    """
    Component for reading data from DuckDB tables using SQL queries.

    This component executes SQL queries against a DuckDB database and returns
    the results as a pandas DataFrame. Perfect for filtering, aggregating, or
    joining data from DuckDB tables written by the DuckDB Table Writer component.

    Features:
    - Full SQL support (SELECT, JOIN, WHERE, GROUP BY, etc.)
    - Query multiple tables in the same database
    - Aggregate and transform data with SQL
    - Create derived assets from queries
    - No need to load entire tables into memory
    """

    asset_name: str = Field(description="Name of the asset to create")

    database_path: str = Field(
        default="data.duckdb",
        description="Path to the DuckDB database file"
    )

    query: str = Field(
        description="SQL query to execute (SELECT statement)"
    )

    description: Optional[str] = Field(
        default="",
        description="Description of the asset"
    )

    group_name: Optional[str] = Field(
        default="",
        description="Asset group name for organization"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the DuckDB query reader."""

        # Capture fields for closure
        asset_name = self.asset_name
        database_path = self.database_path
        query = self.query
        description = self.description or f"Query results from DuckDB"
        group_name = self.group_name or None

        @multi_asset(
            name=f"{asset_name}_reader",
            specs=[
                AssetSpec(
                    key=asset_name,
                    description=description,
                    group_name=group_name,
                )
            ],
        )
        def duckdb_reader_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Execute SQL query and return results as DataFrame."""
            import duckdb

            # Check if database exists
            db_path = Path(database_path)
            if not db_path.exists():
                raise FileNotFoundError(
                    f"DuckDB database not found at {database_path}. "
                    f"Make sure a DuckDB Table Writer has run first."
                )

            context.log.info(f"Connecting to DuckDB at {database_path}")

            # Connect to DuckDB
            con = duckdb.connect(str(db_path), read_only=True)

            try:
                context.log.info(f"Executing query: {query[:100]}...")

                # Execute query and get DataFrame
                df = con.execute(query).df()

                context.log.info(
                    f"Query returned {len(df)} rows and {len(df.columns)} columns"
                )

                if len(df) > 0:
                    context.log.info(f"Columns: {', '.join(df.columns.tolist())}")
                else:
                    context.log.warning("Query returned no rows")

                return df

            except Exception as e:
                context.log.error(f"Query execution failed: {str(e)}")
                raise

            finally:
                con.close()

        return Definitions(assets=[duckdb_reader_asset])
