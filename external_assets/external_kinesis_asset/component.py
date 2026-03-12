"""External Kinesis Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalKinesisAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    stream_name: str = Field(description="Kinesis stream name")
    region_name: Optional[str] = Field(default=None, description="AWS region")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Kinesis stream {self.stream_name}",
            kinds={"kinesis", "aws", "streaming"},
            metadata={
                "stream_name": self.stream_name,
                "region_name": self.region_name or "",
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
