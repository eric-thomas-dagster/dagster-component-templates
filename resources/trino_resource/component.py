"""Trino Resource component — distributed SQL query engine."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field
from contextlib import contextmanager


class TrinoResource(dg.ConfigurableResource):
    """Trino distributed SQL query engine resource."""
    host: str
    port: int = 8080
    user: str = "dagster"
    catalog: str = "iceberg"
    schema_name: Optional[str] = None
    password: Optional[str] = None

    @contextmanager
    def get_connection(self):
        import trino.dbapi as trino_dbapi
        conn = trino_dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema_name,
            auth=trino_dbapi.auth.BasicAuthentication(self.user, self.password) if self.password else None,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str):
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query)
            return cur.fetchall()


@dataclass
class TrinoResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Trino distributed SQL query engine resource for use by other components."""

    resource_key: str = Field(default="trino_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8080, description="Trino coordinator port")
    user: str = Field(default="dagster", description="Trino user name")
    catalog: str = Field(default="iceberg", description="Default catalog, e.g. 'iceberg', 'hive', 'delta'")
    schema_name: Optional[str] = Field(default=None, description="Default schema within the catalog")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Trino password (for basic auth)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = TrinoResource(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema_name=self.schema_name,
            password=os.environ.get(self.password_env_var, "") if self.password_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: resource})
