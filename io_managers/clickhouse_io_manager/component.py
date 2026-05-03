"""ClickhouseIOManagerComponent.

ConfigurableIOManager wrapping the official `dagster-clickhouse-pandas` package. Each asset becomes a ClickHouse table; partitioned assets get partitioned tables. Uses the maintained upstream package for type handling, partitions, and schema evolution.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class ClickhouseIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wrap dagster-clickhouse-pandas's ClickhousePandasIOManager so DataFrame assets persist to ClickHouse."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key.")
    host: str = Field(description="ClickHouse host (no protocol).")
    port: int = Field(default=8443, description="ClickHouse port (8443 HTTPS, 8123 HTTP).")
    username_env_var: str = Field(default="CLICKHOUSE_USER", description="Env var with username.")
    password_env_var: str = Field(default="CLICKHOUSE_PASSWORD", description="Env var with password.")
    database: str = Field(default="default", description="ClickHouse database.")
    secure: bool = Field(default=True, description="Use HTTPS.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_clickhouse_pandas import ClickhousePandasIOManager
        io_manager = ClickhousePandasIOManager(
            host=self.host,
            port=self.port,
            username=dg.EnvVar(self.username_env_var),
            password=dg.EnvVar(self.password_env_var),
            database=self.database,
            secure=self.secure,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})

