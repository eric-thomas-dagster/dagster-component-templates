"""Algolia Resource component."""
import os
from typing import Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class AlgoliaResource(ConfigurableResource):
    """Dagster resource for the Algolia Search REST API (write operations)."""

    app_id: str = Field(description="Algolia Application ID")
    api_key_env_var: str = Field(
        description=(
            "Env var holding an Algolia Admin API Key (needs write access to "
            "the target index -- the Search-only API Key will not work for "
            "batch/delete operations)."
        )
    )
    api_base_url: str = Field(
        default="",
        description=(
            "Override the Algolia REST host. Defaults to "
            "'https://{app_id}-dsn.algolia.net' (the standard write endpoint "
            "host for batch indexing operations)."
        ),
    )

    def get_base_url(self) -> str:
        return self.api_base_url or f"https://{self.app_id}-dsn.algolia.net"

    def get_headers(self) -> dict:
        api_key = os.environ.get(self.api_key_env_var, "")
        return {
            "X-Algolia-API-Key": api_key,
            "X-Algolia-Application-Id": self.app_id,
            "Content-Type": "application/json",
        }


class AlgoliaResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an AlgoliaResource for use by other components."""

    resource_key: str = Field(
        default="algolia_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    app_id: str = Field(description="Algolia Application ID")
    api_key_env_var: str = Field(
        description="Env var holding an Algolia Admin API Key (write access required).",
    )
    api_base_url: Optional[str] = Field(
        default="",
        description=(
            "Override the Algolia REST host. Defaults to "
            "'https://{app_id}-dsn.algolia.net'."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = AlgoliaResource(
            app_id=self.app_id,
            api_key_env_var=self.api_key_env_var,
            api_base_url=self.api_base_url or "",
        )
        return dg.Definitions(resources={self.resource_key: resource})
