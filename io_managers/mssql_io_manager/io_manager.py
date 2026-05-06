"""MSSQLIOManager.

ConfigurableIOManager that writes pandas DataFrames to SQL Server via SQLAlchemy + pyodbc. Asset key becomes the table name; partitioned assets land in one table per partition.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field

from sqlalchemy import create_engine


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


class MSSQLIOManager(dg.ConfigurableIOManager):
    """Persist DataFrames to a Microsoft SQL Server schema, one table per asset."""

    connection_url: str = Field(
        description=(
            "SQLAlchemy URL. Two driver options: "
            "pymssql (pure Python, no ODBC needed): mssql+pymssql://user:pwd@host:1433/db; "
            "pyodbc (requires Microsoft ODBC Driver 18 installed): "
            "mssql+pyodbc://user:pwd@host/db?driver=ODBC+Driver+18+for+SQL+Server"
        )
    )
    schema_name: str = Field(default="dbo", description="SQL Server schema.")
    if_exists: str = Field(default="replace", description="'replace', 'append', or 'fail'.")

    def _table_name(self, context) -> str:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return "_".join(_sanitize(str(p)) for p in parts)

    def handle_output(self, context, obj) -> None:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"MSSQLIOManager only handles DataFrames; got {type(obj)}")
        engine = create_engine(self.connection_url)
        try:
            obj.to_sql(self._table_name(context), engine, schema=self.schema_name, if_exists=self.if_exists, index=False)
        finally:
            engine.dispose()
        context.add_output_metadata({"table": dg.MetadataValue.text(f"{self.schema_name}.{self._table_name(context)}"), "row_count": dg.MetadataValue.int(len(obj))})

    def load_input(self, context):
        engine = create_engine(self.connection_url)
        try:
            return pd.read_sql_table(self._table_name(context.upstream_output), engine, schema=self.schema_name)
        finally:
            engine.dispose()
