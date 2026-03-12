"""External Event Hubs Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalEventHubsAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    namespace: str = Field(description="Azure Event Hubs namespace (without .servicebus.windows.net)")
    eventhub_name: str = Field(description="Event Hub name")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Event Hub {self.namespace}/{self.eventhub_name}",
            kinds={"eventhubs", "azure", "streaming"},
            metadata={
                "namespace": self.namespace,
                "eventhub_name": self.eventhub_name,
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
