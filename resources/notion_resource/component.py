"""Notion Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class NotionResource(ConfigurableResource):
    """Dagster resource wrapping the notion-client SDK."""

    token: str = Field(description="Notion integration token")

    def get_client(self):
        import notion_client
        return notion_client.Client(auth=self.token)


@dataclass
class NotionResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a NotionResource for use by other components."""

    resource_key: str = Field(
        default="notion_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    token_env_var: str = Field(
        description="Env var holding Notion integration token",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = NotionResource(
            token=os.environ.get(self.token_env_var, ""),
        )
        return dg.Definitions(resources={self.resource_key: resource})
