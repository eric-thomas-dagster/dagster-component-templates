"""External SQS Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalSqsAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    queue_url: str = Field(description="Full SQS queue URL")
    region_name: Optional[str] = Field(default=None, description="AWS region")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"SQS queue {self.queue_url}",
            kinds={"sqs", "aws", "queue"},
            metadata={
                "queue_url": self.queue_url,
                "region_name": self.region_name or "",
                "dagster/uri": self.queue_url,
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
