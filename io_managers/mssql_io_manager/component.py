"""MSSQLIOManagerComponent.

YAML/Component wrapper around `MSSQLIOManager`.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import MSSQLIOManager


class MSSQLIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Persist DataFrames to a Microsoft SQL Server schema, one table per asset."""

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key. Use 'io_manager' to make this the default.",
    )
    connection_url_env_var: str = Field(default="MSSQL_URL", description="Env var holding the SQLAlchemy URL.")
    schema_name: str = Field(default="dbo", description="SQL Server schema name.")
    if_exists: str = Field(default="replace", description="'replace', 'append', or 'fail'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = MSSQLIOManager(
            connection_url=dg.EnvVar(self.connection_url_env_var).get_value() or "",
            schema_name=self.schema_name,
            if_exists=self.if_exists,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
