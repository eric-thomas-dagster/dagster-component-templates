"""External GCS Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalGcsAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    bucket_name: str = Field(description="GCS bucket name")
    prefix: str = Field(default="", description="Object prefix")
    project: Optional[str] = Field(default=None, description="GCP project ID")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"GCS bucket {self.bucket_name}/{self.prefix}",
            kinds={"gcs", "gcp", "object_store"},
            metadata={
                "bucket_name": self.bucket_name,
                "prefix": self.prefix,
                "project": self.project or "",
                "dagster/uri": f"gs://{self.bucket_name}/{self.prefix}",
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
