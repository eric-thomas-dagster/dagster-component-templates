"""ClickHouseIOManager.

ConfigurableIOManager that writes pandas DataFrames to ClickHouse via clickhouse-connect. Asset key becomes the table name; default engine is MergeTree.
"""
from typing import Any, Optional

import dagster as dg
import pandas as pd
from pydantic import Field

import clickhouse_connect


def _sanitize(s: str) -> str:
    return s.replace("[", "--").replace("]", "--").replace(" ", "_").replace("/", "__")


class ClickHouseIOManager(dg.ConfigurableIOManager):
    """Persist DataFrames to ClickHouse, one table per asset (columnar OLAP)."""

    host: str = Field(description="ClickHouse host.")
    port: int = Field(default=8443, description="ClickHouse HTTPS port (8443) or HTTP (8123).")
    username: str = Field(default="default", description="ClickHouse username.")
    password: str = Field(default="", description="ClickHouse password.")
    database: str = Field(default="default", description="ClickHouse database name.")
    secure: bool = Field(default=True, description="Use HTTPS.")
    table_engine: str = Field(default="MergeTree() ORDER BY tuple()", description="ClickHouse table engine clause.")

    def _table_name(self, context) -> str:
        parts = list(context.asset_key.path)
        if context.has_asset_partitions:
            parts.append(context.asset_partition_key)
        return "_".join(_sanitize(str(p)) for p in parts)

    def handle_output(self, context, obj) -> None:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"ClickHouseIOManager only handles DataFrames; got {type(obj)}")
        client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username, password=self.password, database=self.database, secure=self.secure)
        table = self._table_name(context)
        client.command(f"DROP TABLE IF EXISTS {table}")
        client.insert_df(table, obj, settings={"insert_distributed_sync": 1}) if False else client.insert_df(table, obj)
        context.add_output_metadata({"table": dg.MetadataValue.text(f"{self.database}.{table}"), "row_count": dg.MetadataValue.int(len(obj))})

    def load_input(self, context):
        client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username, password=self.password, database=self.database, secure=self.secure)
        return client.query_df(f"SELECT * FROM {self._table_name(context.upstream_output)}")
