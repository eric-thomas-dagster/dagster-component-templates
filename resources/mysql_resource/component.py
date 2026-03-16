import os
from dataclasses import dataclass

import dagster as dg
import mysql.connector
from dagster import ConfigurableResource


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


@dataclass
class MySQLResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dagster component that provides a MySQL resource."""

    resource_key: str = "mysql_resource"
    host: str = ""
    port: int = 3306
    database: str = ""
    username: str = ""
    password_env_var: str = ""
    ssl_disabled: bool = False

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        password = os.environ.get(self.password_env_var, "") if self.password_env_var else ""
        resource = MySQLResource(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=password,
            ssl_disabled=self.ssl_disabled,
        )
        return dg.Definitions(resources={self.resource_key: resource})
