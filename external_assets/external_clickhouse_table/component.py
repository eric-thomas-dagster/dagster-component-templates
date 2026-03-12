"""External ClickHouse Table Component.

Declares a ClickHouse table as an observable external asset in Dagster.
Includes a ClickHouseResource for reuse across components.

Use alongside clickhouse_table_observation_sensor for continuous health monitoring.
"""
from typing import Optional
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class ClickHouseResource(ConfigurableResource):
    """Resource for connecting to ClickHouse.

    Example (dagster.yaml or Definitions):
        ```python
        ClickHouseResource(
            host=EnvVar("CLICKHOUSE_HOST"),
            password=EnvVar("CLICKHOUSE_PASSWORD"),
        )
        ```
    """

    host: str = Field(description="ClickHouse host")
    port: int = Field(default=8443, description="ClickHouse port (8443 for HTTPS, 8123 for HTTP)")
    username: str = Field(default="default", description="ClickHouse username")
    password: str = Field(default="", description="ClickHouse password")
    secure: bool = Field(default=True, description="Use HTTPS (recommended)")

    def get_client(self):
        """Return a clickhouse_connect client."""
        import clickhouse_connect
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            secure=self.secure,
        )

    def query_value(self, sql: str):
        """Execute a scalar query and return the result."""
        client = self.get_client()
        return client.command(sql)


class ExternalClickHouseTableComponent(dg.Component, dg.Model, dg.Resolvable):
    """Declare a ClickHouse table as an observable external asset.

    Example:
        ```yaml
        type: dagster_component_templates.ExternalClickHouseTableComponent
        attributes:
          asset_key: clickhouse/analytics/events
          database: analytics
          table: events
          host_env_var: CLICKHOUSE_HOST
          password_env_var: CLICKHOUSE_PASSWORD
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'clickhouse/analytics/events')")
    database: str = Field(description="ClickHouse database name")
    table: str = Field(description="ClickHouse table name")
    host_env_var: str = Field(description="Env var with ClickHouse host")
    port: int = Field(default=8443, description="ClickHouse port")
    username_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse password")
    description: Optional[str] = Field(default=None, description="Human-readable description")
    group_name: Optional[str] = Field(default="clickhouse", description="Dagster asset group name")
    owners: Optional[list] = Field(default=None, description="List of owner emails or team names")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=self.description or f"ClickHouse table {self.database}.{self.table}",
            group_name=self.group_name,
            owners=self.owners or [],
            kinds={"clickhouse", "sql"},
            metadata={
                "dagster/storage_kind": "clickhouse",
                "dagster/observability_type": "external",
                "database": self.database,
                "table": self.table,
                "full_table_name": f"{self.database}.{self.table}",
            },
        )
        return dg.Definitions(assets=[spec])
