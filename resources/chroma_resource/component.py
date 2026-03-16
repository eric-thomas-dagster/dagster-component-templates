"""Chroma Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class ChromaResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-chroma ChromaResource for use by other components."""

    resource_key: str = Field(default="chroma_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: Optional[str] = Field(default=None, description="Chroma server host for HTTP client mode, e.g. 'localhost'")
    port: int = Field(default=8000, description="Chroma server port")
    api_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Chroma API key")
    local_path: Optional[str] = Field(default=None, description="Local path for persistent client mode (omit host/port for local)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_chroma import ChromaResource
        api_key = os.environ.get(self.api_key_env_var) if self.api_key_env_var else None
        resource = ChromaResource(
            host=self.host,
            port=self.port,
            api_key=api_key,
            path=self.local_path,
        )
        return dg.Definitions(resources={self.resource_key: resource})
