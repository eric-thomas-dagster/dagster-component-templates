"""Snowflake IO Manager component — reads and writes Pandas DataFrames via Snowflake."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class SnowflakeIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SnowflakePandasIOManager so assets are automatically stored in and loaded from Snowflake."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    account: str = Field(description="Snowflake account identifier, e.g. 'xy12345.us-east-1'")
    user: str = Field(description="Snowflake username")
    password_env_var: str = Field(description="Environment variable holding the Snowflake password")
    database: str = Field(description="Snowflake database to store assets in")
    warehouse: Optional[str] = Field(default=None, description="Snowflake virtual warehouse")
    schema_name: Optional[str] = Field(default="PUBLIC", description="Default schema for asset tables")
    role: Optional[str] = Field(default=None, description="Snowflake role to use")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_snowflake_pandas import SnowflakePandasIOManager
        io_manager = SnowflakePandasIOManager(
            account=self.account,
            user=self.user,
            password=dg.EnvVar(self.password_env_var),
            database=self.database,
            warehouse=self.warehouse,
            schema=self.schema_name,
            role=self.role,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
