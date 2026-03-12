"""External Redis Stream Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalRedisStreamAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    stream_name: str = Field(description="Redis stream name")
    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Redis stream {self.host}/{self.stream_name}",
            kinds={"redis", "streaming"},
            metadata={
                "host": self.host,
                "port": self.port,
                "stream_name": self.stream_name,
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
