"""StarRocks resource component.

StarRocks is the literal open-source fork of Apache Doris — same MySQL
wire protocol, same Stream Load HTTP ingestion path, same OLAP MPP
architecture. The wire-level surface this component speaks against is
identical to ``doris_resource``; the differences customers care about
are mostly upstream feature velocity + the StarRocks Enterprise / Celerdata
SaaS offering.

If you're picking between Doris and StarRocks for a new install, talk
to your team about feature velocity / CelerData support / community
size. Operationally for Dagster, ``starrocks_resource`` and
``doris_resource`` are interchangeable — same connection string, same
Stream Load endpoints.
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class StarRocksResource(dg.ConfigurableResource):
    """Provides a StarRocks connection string + connection factory.

    StarRocks speaks MySQL wire protocol on the query port (default
    9030 — same as Doris). The HTTP port (default 8030) handles Stream
    Load + admin REST.
    """

    host: str
    query_port: int = 9030
    http_port: int = 8030
    database: str
    username: str
    password: str
    ssl: bool = False

    @property
    def connection_string(self) -> str:
        """SQLAlchemy URL via PyMySQL — StarRocks uses the MySQL wire protocol."""
        pw = urllib.parse.quote_plus(self.password)
        url = f"mysql+pymysql://{self.username}:{pw}@{self.host}:{self.query_port}/{self.database}"
        if self.ssl:
            url += "?ssl=true"
        return url

    @property
    def http_endpoint(self) -> str:
        """HTTP frontend endpoint — Stream Load + admin REST."""
        scheme = "https" if self.ssl else "http"
        return f"{scheme}://{self.host}:{self.http_port}"

    def get_engine(self):
        from sqlalchemy import create_engine
        return create_engine(self.connection_string)

    def get_connection(self):
        import pymysql
        return pymysql.connect(
            host=self.host,
            port=self.query_port,
            user=self.username,
            password=self.password,
            database=self.database,
            ssl_disabled=not self.ssl,
        )


class StarRocksResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a StarRocks resource for use by other components.

    Example:

        ```yaml
        type: dagster_community_components.StarRocksResourceComponent
        attributes:
          resource_key: starrocks_resource
          host: starrocks-fe.mycompany.com
          query_port: 9030
          http_port: 8030
          database: analytics
          username: dagster_runner
          password_env_var: STARROCKS_PASSWORD
        ```
    """

    resource_key: str = Field(default="starrocks_resource", description="Resource key.")
    host: str = Field(description="StarRocks Frontend (FE) hostname.")
    query_port: int = Field(default=9030, description="MySQL-protocol query port.")
    http_port: int = Field(default=8030, description="HTTP frontend port.")
    database: str = Field(description="Database name.")
    username: str = Field(description="StarRocks username.")
    password: Optional[str] = Field(default=None, description="StarRocks password. Set this OR password_env_var.")
    password_env_var: Optional[str] = Field(default=None, description="Env var with password.")
    ssl: bool = Field(default=False, description="Enable TLS.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = StarRocksResource(
            host=self.host,
            query_port=self.query_port,
            http_port=self.http_port,
            database=self.database,
            username=self.username,
            password=self.password if self.password else dg.EnvVar(self.password_env_var or ""),
            ssl=self.ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
