"""Zendesk Resource component."""
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class ZendeskResource(ConfigurableResource):
    """Dagster resource wrapping the Zenpy client."""

    subdomain: str = Field(description="Zendesk subdomain e.g. 'mycompany'")
    email: str = Field(description="Zendesk agent email address")
    api_token: str = Field(description="Zendesk API token")

    def get_client(self):
        from zenpy import Zenpy
        return Zenpy(
            subdomain=self.subdomain,
            email=self.email,
            token=self.api_token,
        )


class ZendeskResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ZendeskResource for use by other components."""

    resource_key: str = Field(
        default="zendesk_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    subdomain: str = Field(
        description="Zendesk subdomain e.g. 'mycompany'",
    )
    email: str = Field(
        description="Zendesk agent email address",
    )
    api_token_env_var: str = Field(
        description="Env var holding Zendesk API token",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = ZendeskResource(
            subdomain=self.subdomain,
            email=self.email,
            api_token=dg.EnvVar(self.api_token_env_var),
        )
        return dg.Definitions(resources={self.resource_key: resource})
