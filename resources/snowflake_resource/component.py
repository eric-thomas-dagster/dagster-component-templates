"""Snowflake Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class SnowflakeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-snowflake SnowflakeResource for use by other components."""

    resource_key: str = Field(default="snowflake_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    account: str = Field(description="Snowflake account identifier, e.g. 'xy12345.us-east-1'")
    user: str = Field(description="Snowflake username")
    password_env_var: str = Field(description="Environment variable holding the Snowflake password")
    database: Optional[str] = Field(default=None, description="Default database")
    warehouse: Optional[str] = Field(default=None, description="Default warehouse")
    role: Optional[str] = Field(default=None, description="Default role")
    schema_name: Optional[str] = Field(default=None, description="Default schema")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_snowflake import SnowflakeResource
        resource = SnowflakeResource(
            account=self.account,
            user=self.user,
            password=os.environ.get(self.password_env_var, ""),
            database=self.database,
            warehouse=self.warehouse,
            role=self.role,
            schema=self.schema_name,
        )
        return dg.Definitions(resources={self.resource_key: resource})
