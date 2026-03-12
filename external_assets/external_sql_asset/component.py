"""External SQL Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalSqlAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    table_name: str = Field(description="Table name (use schema.table for non-default schemas)")
    connection_string_env_var: str = Field(description="Env var containing the SQLAlchemy connection string")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"SQL table {self.table_name}",
            kinds={"sql", "database"},
            metadata={
                "table_name": self.table_name,
                "connection_string_env_var": self.connection_string_env_var,
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
