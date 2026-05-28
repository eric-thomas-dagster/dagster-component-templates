"""ClickHouse resource component.

Provides connection access to a ClickHouse cluster — the dominant open-
source columnar OLAP database. Uses the official ``clickhouse-connect``
Python client (HTTP-based; also supports the native binary protocol via
``clickhouse-driver``, which this component does NOT enable by default
to keep the dependency surface small).

ClickHouse exposes:
  - HTTP interface (default 8123 plain / 8443 TLS) — used by clickhouse-connect
  - Native TCP protocol (default 9000) — faster but heavier dependency
  - MySQL wire protocol (default 9004) — interop only; not the production path

We default to HTTP via clickhouse-connect.

Pairs with the registry's existing ClickHouse components:
  - ``clickhouse_io_manager`` / ``clickhouse_polars_io_manager`` — asset
    persistence
  - ``external_clickhouse_table`` — declare-only catalog entry
  - ``clickhouse_table_observation_sensor`` — table-state observation
  - ``dataframe_to_clickhouse`` — bulk insert via HTTP

Docs: https://clickhouse.com/docs/en/integrations/python
"""
from typing import Optional

import dagster as dg
from pydantic import Field


class ClickHouseResource(dg.ConfigurableResource):
    """Provides a ClickHouse client + SQLAlchemy URL.

    Two access modes:
      - ``get_client()`` — clickhouse-connect.Client (preferred, HTTP-based)
      - ``connection_string`` — SQLAlchemy URL via clickhouse-sqlalchemy
        (for generic SQL components that want a SQLAlchemy URL)
    """

    host: str
    port: int = 8123  # 8443 for TLS
    database: str = "default"
    username: str = "default"
    password: str = ""
    secure: bool = False  # set True for TLS (port becomes 8443 conventionally)

    @property
    def connection_string(self) -> str:
        """SQLAlchemy URL via clickhouse-sqlalchemy."""
        scheme = "clickhouse+http"  # use clickhouse-sqlalchemy's http dialect
        secure_q = "?secure=true" if self.secure else ""
        from urllib.parse import quote_plus
        pw = quote_plus(self.password) if self.password else ""
        user_pw = f"{self.username}:{pw}" if pw else self.username
        return f"{scheme}://{user_pw}@{self.host}:{self.port}/{self.database}{secure_q}"

    def get_engine(self):
        from sqlalchemy import create_engine
        return create_engine(self.connection_string)

    def get_client(self):
        """Return a clickhouse-connect Client."""
        import clickhouse_connect
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            secure=self.secure,
        )


class ClickHouseResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ClickHouse resource for use by other components.

    Example:

        ```yaml
        type: dagster_community_components.ClickHouseResourceComponent
        attributes:
          resource_key: clickhouse_resource
          host: clickhouse.mycompany.com
          port: 8443
          database: analytics
          username: dagster_runner
          password_env_var: CLICKHOUSE_PASSWORD
          secure: true
        ```

    Pairs with:
      - ``clickhouse_io_manager`` — Pandas asset persistence
      - ``dataframe_to_clickhouse`` — bulk insert
      - ``external_clickhouse_table`` — catalog declaration
      - Generic SQL components via ``connection_string``
    """

    resource_key: str = Field(default="clickhouse_resource", description="Resource key.")
    host: str = Field(description="ClickHouse server hostname.")
    port: int = Field(
        default=8123,
        description="HTTP port (8123 plain, 8443 TLS). For native TCP use 9000 + a different client.",
    )
    database: str = Field(default="default", description="Database name.")
    username: str = Field(default="default", description="ClickHouse username.")
    password: Optional[str] = Field(default=None, description="Password. Set this OR password_env_var.")
    password_env_var: Optional[str] = Field(default=None, description="Env var with password.")
    secure: bool = Field(default=False, description="Enable TLS.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = ClickHouseResource(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password if self.password else (
                dg.EnvVar(self.password_env_var or "") if self.password_env_var else ""
            ),
            secure=self.secure,
        )
        return dg.Definitions(resources={self.resource_key: resource})
