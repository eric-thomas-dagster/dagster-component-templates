"""Weaviate Resource component."""
from dataclasses import dataclass
from typing import Optional, Dict
import os
import dagster as dg
from pydantic import Field


@dataclass
class WeaviateResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-weaviate WeaviateResource for use by other components."""

    resource_key: str = Field(default="weaviate_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    connection_params: Optional[Dict] = Field(default=None, description="Weaviate connection parameters dict passed to weaviate.connect_to_custom()")
    url: Optional[str] = Field(default=None, description="Weaviate instance URL for cloud or local, e.g. 'https://my-cluster.weaviate.network'")
    api_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Weaviate API key (for Weaviate Cloud)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_weaviate import WeaviateResource
        api_key = os.environ.get(self.api_key_env_var) if self.api_key_env_var else None
        resource = WeaviateResource(
            url=self.url,
            auth_client_secret=api_key,
        )
        return dg.Definitions(resources={self.resource_key: resource})
