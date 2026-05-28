"""Apache Doris resource component.

Provides a SQLAlchemy connection string + raw connection factory for
[Apache Doris](https://doris.apache.org/) — the real-time OLAP MPP
database (similar to ClickHouse / Druid / StarRocks). Doris speaks the
MySQL wire protocol on its query port (default 9030), so the
SQLAlchemy + PyMySQL stack works as-is. The HTTP frontend (default
8030) handles Stream Load / Broker Load / admin REST APIs — used by
``dataframe_to_doris``.

Customers pair this resource with the generic SQL components
(``sql_transform``, ``dataframe_to_table``, ``database_schema_inventory``)
+ the Doris-specific ``dataframe_to_doris`` sink that uses Stream Load
for efficient bulk ingestion.

Auth: username / password (Doris's own user model — separate from any
external IdP today; Doris-on-Kubernetes deployments often front this
with an OIDC proxy).

Connection string shape:
    mysql+pymysql://{user}:{pw}@{host}:{port}/{database}
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class DorisResource(dg.ConfigurableResource):
    """Provides an Apache Doris connection string + connection factory.

    Doris speaks MySQL wire protocol on the query port; the HTTP port
    is used for Stream Load (see dataframe_to_doris sink). This resource
    exposes both via separate URL builders.
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
        """SQLAlchemy URL via PyMySQL — Doris speaks MySQL wire protocol."""
        pw = urllib.parse.quote_plus(self.password)
        url = f"mysql+pymysql://{self.username}:{pw}@{self.host}:{self.query_port}/{self.database}"
        if self.ssl:
            url += "?ssl=true"
        return url

    @property
    def http_endpoint(self) -> str:
        """HTTP frontend endpoint — used by Stream Load + admin REST."""
        scheme = "https" if self.ssl else "http"
        return f"{scheme}://{self.host}:{self.http_port}"

    def get_engine(self):
        """Return a SQLAlchemy engine."""
        from sqlalchemy import create_engine
        return create_engine(self.connection_string)

    def get_connection(self):
        """Return a raw PyMySQL connection."""
        import pymysql
        return pymysql.connect(
            host=self.host,
            port=self.query_port,
            user=self.username,
            password=self.password,
            database=self.database,
            ssl_disabled=not self.ssl,
        )


class DorisResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Apache Doris resource for use by other components.

    Pairs with the generic SQL components (``sql_transform``,
    ``dataframe_to_table``, ``database_schema_inventory``) by exposing
    its SQLAlchemy URL, and with ``dataframe_to_doris`` for Stream-Load-
    based bulk ingestion via the HTTP frontend.

    Example:

        ```yaml
        type: dagster_community_components.DorisResourceComponent
        attributes:
          resource_key: doris_resource
          host: doris-fe.mycompany.com
          query_port: 9030
          http_port: 8030
          database: analytics
          username: dagster_runner
          password_env_var: DORIS_PASSWORD
        ```
    """

    resource_key: str = Field(
        default="doris_resource",
        description="Resource key. Other components reference it via this name.",
    )
    host: str = Field(description="Doris Frontend (FE) hostname.")
    query_port: int = Field(default=9030, description="MySQL-protocol query port.")
    http_port: int = Field(default=8030, description="HTTP frontend port (Stream Load + admin REST).")
    database: str = Field(description="Database name (Doris's term for catalog).")
    username: str = Field(description="Doris username.")
    password: Optional[str] = Field(default=None, description="Doris password (literal). Set this OR password_env_var.")
    password_env_var: Optional[str] = Field(default=None, description="Env var holding the password.")
    ssl: bool = Field(
        default=False,
        description="Enable TLS for query + HTTP endpoints (Doris 2.0+).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = DorisResource(
            host=self.host,
            query_port=self.query_port,
            http_port=self.http_port,
            database=self.database,
            username=self.username,
            password=self.password if self.password else dg.EnvVar(self.password_env_var or ""),
            ssl=self.ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
