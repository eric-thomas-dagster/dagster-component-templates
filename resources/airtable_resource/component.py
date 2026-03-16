"""Airtable Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class AirtableResource(ConfigurableResource):
    """Dagster resource wrapping the pyairtable API client."""

    api_key: str = Field(description="Airtable personal access token")

    def get_client(self):
        import pyairtable
        return pyairtable.Api(self.api_key)


@dataclass
class AirtableResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an AirtableResource for use by other components."""

    resource_key: str = Field(
        default="airtable_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        description="Env var holding Airtable personal access token",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = AirtableResource(
            api_key=os.environ.get(self.api_key_env_var, ""),
        )
        return dg.Definitions(resources={self.resource_key: resource})
