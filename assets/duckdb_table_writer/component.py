"""DuckDB Table Writer Asset Component."""

from typing import Optional, Literal
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
    AssetKey,
    AssetDep,
)
from pydantic import Field


class DuckDBTableWriterComponent(Component, Model, Resolvable):
    """
    Component for writing pandas DataFrames to DuckDB tables.

    This component reads data from an upstream asset (as a DataFrame) and writes
    it to a DuckDB database table. Perfect for persisting synthetic data, API
    responses, or transformed data to a local database.

    Features:
    - Automatic table creation from DataFrame schema
    - Support for create/replace/append modes
    - Local DuckDB file storage (no server setup needed)
    - Efficient columnar storage
    - Full SQL query support on written data
    """

    asset_name: str = Field(description="Name of the asset to create")

    source_asset: str = Field(
        description="Name of the upstream asset that provides the DataFrame"
    )

    database_path: str = Field(
        description="Path to the DuckDB database file (will be created if doesn't exist)"
    )

    table_name: str = Field(
        description="Name of the table to write data to"
    )

    write_mode: Literal["create", "replace", "append"] = Field(
        default="replace",
        description="Write mode: 'create' (fail if exists), 'replace' (drop and recreate), 'append' (add rows)"
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
        """Build Dagster definitions for the DuckDB table writer."""

        # Capture fields for closure
        asset_name = self.asset_name
        source_asset = self.source_asset
        database_path = self.database_path
        table_name = self.table_name
        write_mode = self.write_mode
        description = self.description or f"Write data to DuckDB table {table_name}"
        group_name = self.group_name or None

        @multi_asset(
            name=f"{asset_name}_writer",
            specs=[
                AssetSpec(
                    key=asset_name,
                    description=description,
                    group_name=group_name,
                    deps=[AssetDep(AssetKey(source_asset))],
                )
            ],
        )
        def duckdb_writer_asset(context: AssetExecutionContext, **kwargs) -> None:
            """Write DataFrame to DuckDB table."""
            import duckdb

            # Get the DataFrame from the upstream asset
            df = kwargs[source_asset]

            if not isinstance(df, pd.DataFrame):
                raise ValueError(
                    f"Expected DataFrame from {source_asset}, got {type(df)}"
                )

            context.log.info(
                f"Writing {len(df)} rows to DuckDB table '{table_name}' "
                f"at {database_path}"
            )

            # Ensure database directory exists
            db_path = Path(database_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            # Connect to DuckDB
            con = duckdb.connect(str(db_path))

            try:
                # Check if table exists
                table_exists = con.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables "
                    f"WHERE table_name = '{table_name}'"
                ).fetchone()[0] > 0

                if write_mode == "create":
                    if table_exists:
                        raise ValueError(
                            f"Table {table_name} already exists and write_mode is 'create'"
                        )
                    # Create new table
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                    context.log.info(f"Created new table {table_name}")

                elif write_mode == "replace":
                    if table_exists:
                        con.execute(f"DROP TABLE {table_name}")
                        context.log.info(f"Dropped existing table {table_name}")
                    # Create table with data
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                    context.log.info(f"Created table {table_name}")

                elif write_mode == "append":
                    if not table_exists:
                        # Create table if it doesn't exist
                        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                        context.log.info(
                            f"Created new table {table_name} (table didn't exist)"
                        )
                    else:
                        # Append to existing table
                        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
                        context.log.info(f"Appended {len(df)} rows to {table_name}")

                # Log final row count
                row_count = con.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                context.log.info(
                    f"Table {table_name} now has {row_count} total rows"
                )

            finally:
                con.close()

        return Definitions(assets=[duckdb_writer_asset])
