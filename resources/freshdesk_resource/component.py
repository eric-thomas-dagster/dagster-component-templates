"""Freshdesk Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class FreshdeskResource(ConfigurableResource):
    """Dagster resource for the Freshdesk REST API."""

    domain: str = Field(description="Freshdesk domain e.g. 'mycompany.freshdesk.com'")
    api_key_env_var: str = Field(description="Env var holding Freshdesk API key")

    def get_session(self):
        import requests

        api_key = os.environ.get(self.api_key_env_var, "")
        base_url = f"https://{self.domain}/api/v2"

        session = requests.Session()
        session.auth = (api_key, "X")
        session.headers.update({"Content-Type": "application/json"})
        session.base_url = base_url  # type: ignore[attr-defined]
        return session


@dataclass
class FreshdeskResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a FreshdeskResource for use by other components."""

    resource_key: str = Field(
        default="freshdesk_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    domain: str = Field(
        description="Freshdesk domain e.g. 'mycompany.freshdesk.com'",
    )
    api_key_env_var: str = Field(
        description="Env var holding Freshdesk API key",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = FreshdeskResource(
            domain=self.domain,
            api_key_env_var=self.api_key_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
