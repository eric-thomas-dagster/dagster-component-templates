"""IBM Db2 / Db2 on Cloud Resource component.

Mirrors mssql_resource / postgres_resource — provides a SQLAlchemy connection
string + connection factory other components can share via the Dagster resource
system. Works against Db2 LUW (Linux/UNIX/Windows), Db2 Community Edition
(`icr.io/db2_community/db2`), Db2 on Cloud (IBM Cloud DBaaS), and Db2 Warehouse.

Uses `ibm_db_sa` (SQLAlchemy dialect) backed by IBM's `ibm_db` driver.
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class Db2Resource(dg.ConfigurableResource):
    """Provides an IBM Db2 connection string + connection factory."""

    host: str
    port: int = 50000
    database: str
    username: str
    password: str
    ssl: bool = False
    security_mechanism: Optional[str] = None

    @property
    def connection_string(self) -> str:
        """SQLAlchemy URL for `ibm_db_sa`."""
        pw = urllib.parse.quote_plus(self.password)
        url = (
            f"db2+ibm_db://{self.username}:{pw}@{self.host}:{self.port}/"
            f"{self.database}"
        )
        params = []
        if self.ssl:
            params.append("Security=SSL")
        if self.security_mechanism:
            params.append(f"SecurityMechanism={self.security_mechanism}")
        if params:
            url += "?" + "&".join(params)
        return url

    def get_engine(self):
        """Return a SQLAlchemy engine using the configured connection string."""
        from sqlalchemy import create_engine
        return create_engine(self.connection_string)

    def get_connection(self):
        """Return a raw ibm_db connection."""
        import ibm_db
        dsn = (
            f"DATABASE={self.database};HOSTNAME={self.host};PORT={self.port};"
            f"PROTOCOL=TCPIP;UID={self.username};PWD={self.password};"
        )
        if self.ssl:
            dsn += "Security=SSL;"
        if self.security_mechanism:
            dsn += f"SecurityMechanism={self.security_mechanism};"
        return ibm_db.connect(dsn, "", "")


class Db2ResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an IBM Db2 resource for use by other components.

    Pairs with the generic SQL components — `dataframe_to_table`,
    `sql_command_job`, `warehouse_maintenance_job` — by exporting its
    SQLAlchemy URL.

    Example (Docker Db2 Community Edition):

        ```yaml
        type: dagster_component_templates.Db2ResourceComponent
        attributes:
          resource_key: db2_resource
          host: localhost
          port: 50000
          database: testdb
          username: db2inst1
          password_env_var: DB2_PASSWORD
        ```

    Example (Db2 on Cloud):

        ```yaml
        type: dagster_component_templates.Db2ResourceComponent
        attributes:
          resource_key: db2_resource
          host: '<id>.databases.appdomain.cloud'
          port: 31198
          database: bludb
          username: bluadmin
          password_env_var: DB2_CLOUD_PASSWORD
          ssl: true
        ```
    """

    resource_key: str = Field(
        default="db2_resource",
        description="Resource key. Other components reference it via this name.",
    )
    host: str = Field(description="Db2 hostname")
    port: int = Field(default=50000, description="Db2 port (default 50000; 31xxx for Db2 on Cloud)")
    database: str = Field(description="Database name")
    username: str = Field(description="Login username")
    password: Optional[str] = Field(default=None, description="Db2 password (literal). Set this OR password_env_var.")
    password_env_var: Optional[str] = Field(default=None, description="Env var holding the password. Set this OR password.")
    ssl: bool = Field(
        default=False,
        description="Enable SSL connection. Required for Db2 on Cloud.",
    )
    security_mechanism: Optional[str] = Field(
        default=None,
        description="Optional SecurityMechanism override (e.g. 'PLAIN'). Leave unset for default.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = Db2Resource(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password if self.password else dg.EnvVar(self.password_env_var or ""),
            ssl=self.ssl,
            security_mechanism=self.security_mechanism,
        )
        return dg.Definitions(resources={self.resource_key: resource})
