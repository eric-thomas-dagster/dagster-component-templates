"""MySQLIOManager.

ConfigurableIOManager that writes pandas DataFrames to MySQL via SQLAlchemy. Each asset gets its own table named from the asset key. Supports partitioned assets (one table per partition) and customer schema selection.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field

from sqlalchemy import create_engine


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


class MySQLIOManager(dg.ConfigurableIOManager):
    """Persist DataFrames to a MySQL/MariaDB schema, one table per asset."""

    connection_url: str = Field(description="SQLAlchemy URL, e.g. mysql+pymysql://user:pwd@host/db")
    schema_name: Optional[str] = Field(default=None, description="MySQL schema (uses connection default if None).")
    if_exists: str = Field(default="replace", description="'replace', 'append', or 'fail' (pandas.to_sql arg).")

    def _table_name(self, context) -> str:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return "_".join(_sanitize(str(p)) for p in parts)

    def handle_output(self, context, obj) -> None:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"MySQLIOManager only handles DataFrames; got {type(obj)}")
        engine = create_engine(self.connection_url)
        try:
            obj.to_sql(self._table_name(context), engine, schema=self.schema_name, if_exists=self.if_exists, index=False)
        finally:
            engine.dispose()
        context.add_output_metadata({"table": dg.MetadataValue.text(self._table_name(context)), "row_count": dg.MetadataValue.int(len(obj))})

    def load_input(self, context):
        engine = create_engine(self.connection_url)
        try:
            return pd.read_sql_table(self._table_name(context.upstream_output), engine, schema=self.schema_name)
        finally:
            engine.dispose()
