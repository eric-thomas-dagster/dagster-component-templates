"""Pipedrive Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class PipedriveResource(ConfigurableResource):
    """Dagster resource for the Pipedrive API."""

    api_token_env_var: str = Field(description="Env var holding Pipedrive API token")
    company_domain: Optional[str] = Field(
        default=None,
        description="Company domain e.g. 'mycompany'",
    )

    def get_client(self):
        import requests

        api_token = os.environ.get(self.api_token_env_var, "")
        domain = self.company_domain or "api"
        base_url = f"https://{domain}.pipedrive.com/api/v1"

        session = requests.Session()
        session.params = {"api_token": api_token}  # type: ignore[assignment]
        session.headers.update({"Content-Type": "application/json"})
        # Store base_url as a convenience attribute
        session.base_url = base_url  # type: ignore[attr-defined]
        return session


@dataclass
class PipedriveResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PipedriveResource for use by other components."""

    resource_key: str = Field(
        default="pipedrive_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_token_env_var: str = Field(
        description="Env var holding Pipedrive API token",
    )
    company_domain: Optional[str] = Field(
        default=None,
        description="Company domain e.g. 'mycompany'",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = PipedriveResource(
            api_token_env_var=self.api_token_env_var,
            company_domain=self.company_domain,
        )
        return dg.Definitions(resources={self.resource_key: resource})
