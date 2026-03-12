"""External Pub/Sub Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalPubsubAsset(dg.Component, dg.Model, dg.Resolvable):
    asset_key: str = Field(description="Dagster asset key")
    project_id: str = Field(description="GCP project ID")
    topic_id: str = Field(description="Pub/Sub topic ID")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"Pub/Sub topic {self.project_id}/{self.topic_id}",
            kinds={"pubsub", "gcp", "streaming"},
            metadata={
                "project_id": self.project_id,
                "topic_id": self.topic_id,
                "topic_path": f"projects/{self.project_id}/topics/{self.topic_id}",
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
