"""MySQL Resource component."""
from typing import Optional

import dagster as dg
import mysql.connector
from dagster import ConfigurableResource
from pydantic import Field


class MySQLResource(ConfigurableResource):
    host: str
    port: int = 3306
    database: str
    username: str
    password: str = ""
    ssl_disabled: bool = False

    def get_connection(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password,
            ssl_disabled=self.ssl_disabled,
        )


class MySQLResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a MySQL resource for use by other components."""

    resource_key: str = Field(default="mysql_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(description="MySQL host")
    port: int = Field(default=3306, description="MySQL port")
    database: str = Field(description="Database name")
    username: str = Field(description="Database username")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the database password")
    ssl_disabled: bool = Field(default=False, description="Disable TLS. Default False (TLS required); set True only for trusted local networks.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        password = dg.EnvVar(self.password_env_var) if self.password_env_var else ""
        resource = MySQLResource(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=password,
            ssl_disabled=self.ssl_disabled,
        )
        return dg.Definitions(resources={self.resource_key: resource})
