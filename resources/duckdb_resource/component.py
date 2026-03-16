from dataclasses import dataclass

import dagster as dg
import duckdb
from dagster import ConfigurableResource


class DuckDBResource(ConfigurableResource):
    database: str = ":memory:"
    read_only: bool = False

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.database, read_only=self.read_only)


@dataclass
class DuckDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dagster component that provides a DuckDB resource."""

    resource_key: str = "duckdb_resource"
    database: str = ":memory:"
    read_only: bool = False

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = DuckDBResource(
            database=self.database,
            read_only=self.read_only,
        )
        return dg.Definitions(resources={self.resource_key: resource})
