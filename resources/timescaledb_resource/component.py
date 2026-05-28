"""TimescaleDB resource component.

[TimescaleDB](https://www.timescale.com/) is a time-series database built
as a Postgres extension — same Postgres wire protocol, same SQLAlchemy
dialect (``psycopg2``), with **hypertable** semantics layered on top of
regular tables. This resource is a thin Postgres-resource wrapper that
adds TimescaleDB-specific helpers:

  - ``create_hypertable(table, time_column)`` — convert a normal table
    into a TimescaleDB hypertable (partitioned by time_column).
  - ``add_compression_policy(table, after_interval)`` — Timescale's
    columnar compression for older chunks.
  - ``add_retention_policy(table, drop_after)`` — drop old chunks
    automatically.
  - ``add_continuous_aggregate(view, query, refresh_interval)`` —
    materialized-view-like incremental aggregates.

Wraps ``postgres_resource``-style auth; pair with the generic SQL
components (``sql_transform``, ``dataframe_to_table``,
``database_schema_inventory``).
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class TimescaleDBResource(dg.ConfigurableResource):
    """Provides a TimescaleDB (Postgres extension) connection + helpers.

    Connection: standard Postgres via psycopg2. The extension is loaded
    by Postgres at server start (CREATE EXTENSION timescaledb on the
    database side; nothing for the client to do).
    """

    host: str
    port: int = 5432
    database: str
    username: str
    password: str
    ssl: bool = False

    @property
    def connection_string(self) -> str:
        pw = urllib.parse.quote_plus(self.password)
        url = f"postgresql+psycopg2://{self.username}:{pw}@{self.host}:{self.port}/{self.database}"
        if self.ssl:
            url += "?sslmode=require"
        return url

    def get_engine(self):
        from sqlalchemy import create_engine
        return create_engine(self.connection_string)

    def get_connection(self):
        """Return a raw psycopg2 connection."""
        import psycopg2
        return psycopg2.connect(
            host=self.host, port=self.port, dbname=self.database,
            user=self.username, password=self.password,
            sslmode="require" if self.ssl else "prefer",
        )

    def create_hypertable(self, table: str, time_column: str = "timestamp") -> None:
        """Convert a regular table into a TimescaleDB hypertable."""
        from sqlalchemy import text
        with self.get_engine().begin() as conn:
            conn.execute(text(f"SELECT create_hypertable('{table}', '{time_column}', if_not_exists => TRUE)"))

    def add_compression_policy(self, table: str, after_interval: str = "7 days") -> None:
        """Add a compression policy — chunks older than `after_interval` get columnar-compressed."""
        from sqlalchemy import text
        with self.get_engine().begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} SET (timescaledb.compress)"))
            conn.execute(text(f"SELECT add_compression_policy('{table}', INTERVAL '{after_interval}')"))

    def add_retention_policy(self, table: str, drop_after: str = "90 days") -> None:
        """Auto-drop chunks older than `drop_after`."""
        from sqlalchemy import text
        with self.get_engine().begin() as conn:
            conn.execute(text(f"SELECT add_retention_policy('{table}', INTERVAL '{drop_after}')"))


class TimescaleDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a TimescaleDB resource.

    Example:

        ```yaml
        type: dagster_community_components.TimescaleDBResourceComponent
        attributes:
          resource_key: timescaledb_resource
          host: localhost
          port: 5432
          database: metrics
          username: postgres
          password_env_var: TIMESCALEDB_PASSWORD
        ```

    Use the resource's helpers from a regular @asset:

        ```python
        @asset(required_resource_keys={"timescaledb_resource"})
        def setup_metrics_hypertable(context):
            ctx.resources.timescaledb_resource.create_hypertable("events", "ts")
            ctx.resources.timescaledb_resource.add_compression_policy("events", "7 days")
            ctx.resources.timescaledb_resource.add_retention_policy("events", "90 days")
        ```
    """

    resource_key: str = Field(default="timescaledb_resource", description="Resource key.")
    host: str = Field(description="TimescaleDB / Postgres host.")
    port: int = Field(default=5432, description="Postgres port.")
    database: str = Field(description="Database name.")
    username: str = Field(description="Postgres username.")
    password: Optional[str] = Field(default=None, description="Password. Set this OR password_env_var.")
    password_env_var: Optional[str] = Field(default=None, description="Env var with password.")
    ssl: bool = Field(default=False, description="Use sslmode=require.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = TimescaleDBResource(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password if self.password else dg.EnvVar(self.password_env_var or ""),
            ssl=self.ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
