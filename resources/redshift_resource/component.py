"""AWS Redshift Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class RedshiftResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-aws RedshiftClientResource for use by other components."""

    resource_key: str = Field(default="redshift_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(description="Redshift cluster endpoint")
    port: int = Field(default=5439, description="Redshift port")
    database: str = Field(description="Database name")
    user: str = Field(description="Database user")
    password_env_var: str = Field(description="Environment variable holding the Redshift password")
    schema_name: Optional[str] = Field(default=None, description="Default schema")
    autocommit: bool = Field(default=False, description="Enable autocommit mode")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_aws.redshift import RedshiftClientResource
        resource = RedshiftClientResource(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=os.environ.get(self.password_env_var, ""),
            schema=self.schema_name,
            autocommit=self.autocommit,
        )
        return dg.Definitions(resources={self.resource_key: resource})
