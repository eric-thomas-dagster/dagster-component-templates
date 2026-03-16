"""PostgreSQL Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


class PostgresResource(dg.ConfigurableResource):
    """Provides a PostgreSQL connection string and a psycopg2 connection factory."""

    host: str
    port: int = 5432
    database: str
    username: str
    password: str
    sslmode: str = "prefer"

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode={self.sslmode}"

    def get_connection(self):
        import psycopg2
        return psycopg2.connect(self.connection_string)


@dataclass
class PostgresResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PostgreSQL resource for use by other components."""

    resource_key: str = Field(default="postgres_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(description="Database name")
    username: str = Field(description="Database username")
    password_env_var: str = Field(description="Environment variable holding the database password")
    sslmode: Optional[str] = Field(default="prefer", description="SSL mode: disable, allow, prefer, require, verify-ca, verify-full")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = PostgresResource(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=os.environ.get(self.password_env_var, ""),
            sslmode=self.sslmode or "prefer",
        )
        return dg.Definitions(resources={self.resource_key: resource})
