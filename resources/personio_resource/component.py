"""Personio Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class PersonioResource(ConfigurableResource):
    """Dagster resource wrapping the personio-py client."""

    client_id_env_var: str = Field(description="Env var holding Personio client ID")
    client_secret_env_var: str = Field(description="Env var holding Personio client secret")

    def get_client(self):
        import personio_py

        client_id = os.environ.get(self.client_id_env_var, "")
        client_secret = os.environ.get(self.client_secret_env_var, "")

        client = personio_py.Personio(client_id=client_id, client_secret=client_secret)
        client.authenticate()
        return client


@dataclass
class PersonioResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PersonioResource for use by other components."""

    resource_key: str = Field(
        default="personio_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    client_id_env_var: str = Field(
        description="Env var holding Personio client ID",
    )
    client_secret_env_var: str = Field(
        description="Env var holding Personio client secret",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = PersonioResource(
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
