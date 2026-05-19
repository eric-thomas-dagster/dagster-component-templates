"""Oracle Database Resource component.

Mirrors mssql_resource / postgres_resource — provides a SQLAlchemy connection
string + connection factory other components can share via the Dagster resource
system. Works against Oracle Database (XE / Free / Enterprise) on-prem,
container images (`container-registry.oracle.com/database/free`), and Oracle
Autonomous Database / Oracle Cloud Infrastructure DBaaS.

Uses python-oracledb (thin mode) by default — no Oracle Instant Client needed.
Set `thick_mode: true` to enable advanced features that require the client.
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class OracleResource(dg.ConfigurableResource):
    """Provides an Oracle Database connection string + connection factory."""

    host: str
    port: int = 1521
    service_name: Optional[str] = None
    sid: Optional[str] = None
    username: str
    password: str
    thick_mode: bool = False

    @property
    def connection_string(self) -> str:
        """SQLAlchemy URL for `oracledb`. Thin-mode by default; no Instant Client."""
        pw = urllib.parse.quote_plus(self.password)
        if self.service_name:
            return (
                f"oracle+oracledb://{self.username}:{pw}@{self.host}:{self.port}"
                f"/?service_name={self.service_name}"
            )
        if self.sid:
            return (
                f"oracle+oracledb://{self.username}:{pw}@{self.host}:{self.port}"
                f"/{self.sid}"
            )
        raise ValueError("Either service_name or sid must be set.")

    def get_engine(self):
        """Return a SQLAlchemy engine using the configured connection string."""
        from sqlalchemy import create_engine
        if self.thick_mode:
            import oracledb
            oracledb.init_oracle_client()
        return create_engine(self.connection_string)

    def get_connection(self):
        """Return a raw oracledb connection (thin-mode by default)."""
        import oracledb
        if self.thick_mode:
            oracledb.init_oracle_client()
        if self.service_name:
            dsn = oracledb.makedsn(self.host, self.port, service_name=self.service_name)
        else:
            dsn = oracledb.makedsn(self.host, self.port, sid=self.sid)
        return oracledb.connect(user=self.username, password=self.password, dsn=dsn)


class OracleResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Oracle Database resource for use by other components.

    Pairs with the generic SQL components — `dataframe_to_table`,
    `sql_command_job`, `warehouse_maintenance_job` — by sharing its
    SQLAlchemy URL via `connection_string_env_var` semantics.

    Example (Docker Oracle Free):

        ```yaml
        type: dagster_component_templates.OracleResourceComponent
        attributes:
          resource_key: oracle_resource
          host: localhost
          port: 1521
          service_name: FREEPDB1
          username: system
          password_env_var: ORACLE_PASSWORD
        ```

    Example (Oracle Autonomous Database):

        ```yaml
        type: dagster_component_templates.OracleResourceComponent
        attributes:
          resource_key: oracle_resource
          host: adb.us-ashburn-1.oraclecloud.com
          port: 1522
          service_name: g1234abcd_my_adb_medium.adb.oraclecloud.com
          username: ADMIN
          password_env_var: ADW_PASSWORD
        ```
    """

    resource_key: str = Field(
        default="oracle_resource",
        description="Resource key. Other components reference it via this name.",
    )
    host: str = Field(description="Oracle DB host (e.g. 'localhost' or 'adb.us-ashburn-1.oraclecloud.com')")
    port: int = Field(default=1521, description="Oracle DB listener port (default 1521 on-prem; 1522 for ADB)")
    service_name: Optional[str] = Field(
        default=None,
        description="Oracle service name (e.g. 'FREEPDB1', 'ORCLCDB'). Either this OR sid is required.",
    )
    sid: Optional[str] = Field(
        default=None,
        description="Oracle SID (legacy; use service_name when possible).",
    )
    username: str = Field(description="Database username")
    password: Optional[str] = Field(default=None, description="Oracle password (literal). Set this OR password_env_var.")
    password_env_var: Optional[str] = Field(default=None, description="Env var holding the password. Set this OR password.")
    thick_mode: bool = Field(
        default=False,
        description=(
            "Use python-oracledb thick mode (requires Oracle Instant Client on the host). "
            "Leave false for default thin-mode — pure Python, no client install."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = OracleResource(
            host=self.host,
            port=self.port,
            service_name=self.service_name,
            sid=self.sid,
            username=self.username,
            password=self.password if self.password else dg.EnvVar(self.password_env_var or ""),
            thick_mode=self.thick_mode,
        )
        return dg.Definitions(resources={self.resource_key: resource})
