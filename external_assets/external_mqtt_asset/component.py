"""External MQTT Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalMqttAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    broker_host: str = Field(description="MQTT broker hostname")
    topic: str = Field(description="MQTT topic (supports + and # wildcards)")
    broker_port: int = Field(default=1883, description="MQTT broker port")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"MQTT topic {self.broker_host}/{self.topic}",
            kinds={"mqtt", "iot", "messaging"},
            metadata={
                "broker_host": self.broker_host,
                "broker_port": self.broker_port,
                "topic": self.topic,
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
