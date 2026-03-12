"""External SharePoint Library Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalSharePointLibraryAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a SharePoint document library as an observable external asset."""
    asset_key: str = Field(description="Dagster asset key")
    site_url: str = Field(description="SharePoint site URL")
    library_name: str = Field(description="Document library name")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"SharePoint {self.site_url}/{self.library_name}",
            kinds={"sharepoint", "microsoft", "files"},
            metadata={
                "site_url": self.site_url,
                "library_name": self.library_name,
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
