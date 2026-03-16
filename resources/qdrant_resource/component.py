"""Qdrant Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class QdrantResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-qdrant QdrantResource for use by other components."""

    resource_key: str = Field(default="qdrant_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    url: Optional[str] = Field(default=None, description="Qdrant URL, e.g. 'https://xyz.qdrant.io' or 'http://localhost:6333'")
    host: Optional[str] = Field(default=None, description="Qdrant host (alternative to url)")
    port: int = Field(default=6333, description="Qdrant REST port")
    api_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Qdrant API key (for Qdrant Cloud)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_qdrant import QdrantResource
        api_key = os.environ.get(self.api_key_env_var) if self.api_key_env_var else None
        resource = QdrantResource(
            url=self.url,
            host=self.host,
            port=self.port,
            api_key=api_key,
        )
        return dg.Definitions(resources={self.resource_key: resource})
