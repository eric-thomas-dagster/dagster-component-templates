"""External Kafka Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalKafkaAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    bootstrap_servers: str = Field(description="Comma-separated Kafka broker addresses")
    topic: str = Field(description="Kafka topic name")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Kafka topic {self.topic}",
            kinds={"kafka", "streaming"},
            metadata={
                "bootstrap_servers": self.bootstrap_servers,
                "topic": self.topic,
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
