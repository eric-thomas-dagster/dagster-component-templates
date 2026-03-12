"""External ADLS Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalAdlsAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    account_name: str = Field(description="Azure storage account name")
    container_name: str = Field(description="ADLS container / filesystem name")
    path_prefix: str = Field(default="", description="Path prefix within the container")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"ADLS {self.account_name}/{self.container_name}/{self.path_prefix}",
            kinds={"adls", "azure", "object_store"},
            metadata={
                "account_name": self.account_name,
                "container_name": self.container_name,
                "path_prefix": self.path_prefix,
                "dagster/uri": f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net/{self.path_prefix}",
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
