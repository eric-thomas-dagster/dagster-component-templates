"""ClickhousePolarsIOManagerComponent.

Polars variant of clickhouse_io_manager. Same backend (ClickHouse) but with the Polars query engine — much faster on wide rows. Wraps the official `dagster-clickhouse-polars` package.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class ClickhousePolarsIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wrap dagster-clickhouse-polars's ClickhousePolarsIOManager so polars.DataFrame assets persist to ClickHouse."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key.")
    host: str = Field(description="ClickHouse host.")
    port: int = Field(default=8443, description="ClickHouse port.")
    username_env_var: str = Field(default="CLICKHOUSE_USER", description="Env var with username.")
    password_env_var: str = Field(default="CLICKHOUSE_PASSWORD", description="Env var with password.")
    database: str = Field(default="default", description="ClickHouse database.")
    secure: bool = Field(default=True, description="Use HTTPS.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_clickhouse_polars import ClickhousePolarsIOManager
        io_manager = ClickhousePolarsIOManager(
            host=self.host,
            port=self.port,
            user=dg.EnvVar(self.username_env_var),
            password=dg.EnvVar(self.password_env_var),
            database=self.database,
            secure=self.secure,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})

