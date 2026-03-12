"""External RabbitMQ Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalRabbitmqAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    host: str = Field(description="RabbitMQ host")
    queue_name: str = Field(description="RabbitMQ queue name")
    virtual_host: str = Field(default="/", description="RabbitMQ virtual host")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"RabbitMQ queue {self.host}/{self.queue_name}",
            kinds={"rabbitmq", "queue", "messaging"},
            metadata={
                "host": self.host,
                "queue_name": self.queue_name,
                "virtual_host": self.virtual_host,
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
