"""External Databricks Delta Table Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalDatabricksTableAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a Databricks Delta table (Unity Catalog or Hive Metastore) as an observable external asset."""
    asset_key: str = Field(description="Dagster asset key")
    workspace_url: str = Field(description="Databricks workspace URL (e.g. https://myorg.azuredatabricks.net)")
    catalog: Optional[str] = Field(default=None, description="Unity Catalog catalog name (leave blank for Hive Metastore)")
    schema_name: str = Field(description="Schema/database name")
    table_name: str = Field(description="Table name")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        full_name = f"{self.catalog}.{self.schema_name}.{self.table_name}" if self.catalog else f"{self.schema_name}.{self.table_name}"
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Databricks Delta table {full_name}",
            kinds={"databricks", "delta", "sql", "table"},
            metadata={
                "workspace_url": self.workspace_url,
                "catalog": self.catalog or "",
                "schema": self.schema_name,
                "table": self.table_name,
                "full_table_name": full_name,
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
