"""External Snowflake Table Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalSnowflakeTableAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a Snowflake table as an observable external asset.

    Example:
        ```yaml
        type: dagster_component_templates.ExternalSnowflakeTableAsset
        attributes:
          asset_key: snowflake/raw/orders
          account: myorg-us-east-1
          database: RAW
          schema_name: PUBLIC
          table_name: ORDERS
          group_name: snowflake_sources
        ```
    """
    asset_key: str = Field(description="Dagster asset key")
    account: str = Field(description="Snowflake account identifier (e.g. myorg-us-east-1)")
    database: str = Field(description="Snowflake database name")
    schema_name: str = Field(description="Snowflake schema name")
    table_name: str = Field(description="Snowflake table name")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Snowflake {self.database}.{self.schema_name}.{self.table_name}",
            kinds={"snowflake", "sql", "table"},
            metadata={
                "account": self.account,
                "database": self.database,
                "schema": self.schema_name,
                "table": self.table_name,
                "dagster/uri": f"snowflake://{self.account}/{self.database}/{self.schema_name}/{self.table_name}",
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
