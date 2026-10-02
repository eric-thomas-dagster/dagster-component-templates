"""Typesense Resource component."""
import os

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class TypesenseResource(ConfigurableResource):
    """Dagster resource for the Typesense REST API."""

    host: str = Field(
        description=(
            "Typesense node URL, e.g. 'https://xxx.a1.typesense.net:443' "
            "(Typesense Cloud) or 'http://localhost:8108' (self-hosted)."
        )
    )
    api_key_env_var: str = Field(description="Env var holding the Typesense API key.")

    def get_base_url(self) -> str:
        return self.host.rstrip("/")

    def get_headers(self) -> dict:
        api_key = os.environ.get(self.api_key_env_var, "")
        return {"X-TYPESENSE-API-KEY": api_key}


class TypesenseResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a TypesenseResource for use by other components."""

    resource_key: str = Field(
        default="typesense_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    host: str = Field(
        description="Typesense node URL, e.g. 'https://xxx.a1.typesense.net:443' or 'http://localhost:8108'."
    )
    api_key_env_var: str = Field(description="Env var holding the Typesense API key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = TypesenseResource(
            host=self.host,
            api_key_env_var=self.api_key_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
