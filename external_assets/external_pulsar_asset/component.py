"""External Pulsar Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalPulsarAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    service_url: str = Field(description="Pulsar service URL")
    topic: str = Field(description="Pulsar topic name")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Pulsar topic {self.topic}",
            kinds={"pulsar", "streaming"},
            metadata={
                "service_url": self.service_url,
                "topic": self.topic,
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
