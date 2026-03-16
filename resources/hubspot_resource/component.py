"""HubSpot Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class HubSpotResource(ConfigurableResource):
    """Dagster resource wrapping the HubSpot API client."""

    access_token: str = Field(description="HubSpot private app access token")

    def get_client(self):
        import hubspot
        return hubspot.HubSpot(access_token=self.access_token)


@dataclass
class HubSpotResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a HubSpotResource for use by other components."""

    resource_key: str = Field(
        default="hubspot_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    access_token_env_var: str = Field(
        description="Env var holding HubSpot private app access token",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = HubSpotResource(
            access_token=os.environ.get(self.access_token_env_var, ""),
        )
        return dg.Definitions(resources={self.resource_key: resource})
