"""External Service Bus Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalServiceBusAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    namespace: str = Field(description="Azure Service Bus namespace")
    queue_name: Optional[str] = Field(default=None, description="Queue name (use queue_name OR topic_name)")
    topic_name: Optional[str] = Field(default=None, description="Topic name (use topic_name + subscription_name)")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        source = self.queue_name or self.topic_name or "unknown"
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Service Bus {self.namespace}/{source}",
            kinds={"servicebus", "azure", "queue"},
            metadata={
                "namespace": self.namespace,
                "queue_name": self.queue_name or "",
                "topic_name": self.topic_name or "",
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
