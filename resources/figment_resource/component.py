"""Figment Resource component.

API-key wrapper over Figment's institutional staking infrastructure REST
API (Rewards, Validators, Networks, ...).

Base URL: https://api.figment.io
Auth header: x-api-key: <api_key>

Verified against docs.figment.io (2026-10):
  - Figment's API uses a static API key, sent in the `x-api-key` header
    (not `Authorization: Bearer`, and not OAuth2) -- confirmed via
    docs.figment.io/reference/authentication.
  - Keys are environment-scoped (test vs. production) and permission-scoped
    (Read/Write vs. Read-Only); a Read-Only key works for this repo's
    read-only Rewards-API use case.
  - All endpoints (per-network Rewards, Validators, ...) share this same
    `https://api.figment.io` base host and auth scheme.

Drop to `.get_client()` for anything not covered -- returns an
authenticated `requests.Session`. Per this repo's self-contained-component
convention, the one real external-call boundary is NOT wrapped here with
convenience methods -- the consuming component
(`figment_staking_rewards_ingestion`) isolates its own module-level
API-call function around `get_client()` so it stays independently
mockable in tests.
"""
import os

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class FigmentResource(ConfigurableResource):
    """Dagster resource wrapping Figment's institutional staking REST API
    (static `x-api-key` auth)."""

    api_key_env_var: str = Field(description="Env var holding the Figment API key.")
    base_url: str = Field(
        default="https://api.figment.io",
        description="Figment API base URL.",
    )

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch for anything
        not covered by a dedicated component -- every Figment network's Rewards
        API lives under this same `x-api-key` auth scheme."""
        import requests

        api_key = os.environ.get(self.api_key_env_var)
        if not api_key:
            raise ValueError(
                f"FigmentResource: env var {self.api_key_env_var!r} is unset or empty."
            )
        session = requests.Session()
        session.headers.update({
            "x-api-key": api_key,
            "Content-Type": "application/json",
        })
        return session


class FigmentResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a FigmentResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.FigmentResourceComponent
        attributes:
          resource_key: figment_resource
          api_key_env_var: FIGMENT_API_KEY
        ```
    """

    resource_key: str = Field(
        default="figment_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        description="Env var holding the Figment API key.",
    )
    base_url: str = Field(
        default="https://api.figment.io",
        description="Figment API base URL.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = FigmentResource(
            api_key_env_var=self.api_key_env_var,
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
