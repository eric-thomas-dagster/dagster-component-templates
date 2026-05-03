"""ClickHouseIOManagerComponent.

YAML/Component wrapper around `ClickHouseIOManager`.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import ClickHouseIOManager


class ClickHouseIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Persist DataFrames to ClickHouse, one table per asset (columnar OLAP)."""

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key. Use 'io_manager' to make this the default.",
    )
    host: str = Field(description="ClickHouse host (no protocol).")
    port: int = Field(default=8443, description="ClickHouse port.")
    username_env_var: str = Field(default="CLICKHOUSE_USER", description="Env var with username.")
    password_env_var: str = Field(default="CLICKHOUSE_PASSWORD", description="Env var with password.")
    database: str = Field(default="default", description="ClickHouse database name.")
    secure: bool = Field(default=True, description="Use HTTPS.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = ClickHouseIOManager(
            host=self.host,
            port=self.port,
            username=dg.EnvVar(self.username_env_var).get_value() or "default",
            password=dg.EnvVar(self.password_env_var).get_value() or "",
            database=self.database,
            secure=self.secure,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
