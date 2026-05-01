"""DuckDB Resource component."""
import dagster as dg
from pydantic import Field


class DuckDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-duckdb DuckDBResource for use by other components."""

    resource_key: str = Field(default="duckdb_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    database: str = Field(default=":memory:", description="DuckDB database path. Use ':memory:' for an ephemeral in-process DB or a file path for persistence.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_duckdb import DuckDBResource
        resource = DuckDBResource(database=self.database)
        return dg.Definitions(resources={self.resource_key: resource})
