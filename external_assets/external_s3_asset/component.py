"""External asset component — ExternalS3Asset."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalS3Asset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key for this source")
    bucket_name: str = Field(description="S3 bucket name")
    prefix: str = Field(default="", description="Key prefix")
    region_name: Optional[str] = Field(default=None, description="AWS region")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or "S3 bucket source",
            kinds={"s3", "aws", "object_store"},
            metadata={
                "bucket_name": self.bucket_name,
                "prefix": self.prefix,
                "region_name": self.region_name or "",
                "dagster/uri": f"s3://{self.bucket_name}/{self.prefix}",
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
