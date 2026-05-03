"""SnowflakePySparkIOManagerComponent.

PySpark variant of snowflake_io_manager. Wraps the official `dagster-snowflake-pyspark` package.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class SnowflakePySparkIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wrap dagster-snowflake-pyspark so PySpark DataFrames persist to Snowflake."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key.")
    account: str = Field(description="Snowflake account.")
    user_env_var: str = Field(default="SNOWFLAKE_USER", description="Env var with username.")
    password_env_var: str = Field(default="SNOWFLAKE_PASSWORD", description="Env var with password.")
    database: str = Field(description="Snowflake database.")
    schema_name: str = Field(default="PUBLIC", description="Snowflake schema.")
    warehouse: Optional[str] = Field(default=None, description="Snowflake warehouse.")
    role: Optional[str] = Field(default=None, description="Snowflake role.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_snowflake_pyspark import SnowflakePySparkIOManager
        io_manager = SnowflakePySparkIOManager(
            account=self.account,
            user=dg.EnvVar(self.user_env_var),
            password=dg.EnvVar(self.password_env_var),
            database=self.database,
            schema=self.schema_name,
            warehouse=self.warehouse,
            role=self.role,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})

