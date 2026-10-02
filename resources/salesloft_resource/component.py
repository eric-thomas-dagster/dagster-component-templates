"""Salesloft Resource.

Wraps Salesloft's REST API (https://api.salesloft.com/v2). Unlike
Outreach, Salesloft's list responses are NOT JSON:API-shaped -- each record
in `data` is already a flat dict of fields, and pagination metadata lives
under a top-level `metadata.paging` object (`per_page`, `current_page`,
`next_page`, `total_pages`) rather than `links.next`.

Auth: Bearer API key. Salesloft documents API-key auth as "exclusively for
customers" (current/future partners must use OAuth2 instead) -- this
resource implements the API-key path, which is the common case for an
internal Dagster pipeline reading your own org's data.
"""
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class SalesloftResource(dg.ConfigurableResource):
    """Salesloft REST API client wrapper (Bearer API key auth)."""

    api_key_env_var: str = Field(description="Env var holding the Salesloft API key (Bearer token)")
    base_url: str = Field(
        default="https://api.salesloft.com/v2",
        description="Salesloft API base URL",
    )

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET a path (relative to base_url) or a full URL."""
        import requests

        api_key = os.environ.get(self.api_key_env_var)
        if not api_key:
            raise RuntimeError(f"Missing Salesloft API key env var: {self.api_key_env_var}")

        url = path if path.startswith("http") else f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class SalesloftResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SalesloftResource for use by other components."""

    resource_key: str = Field(
        default="salesloft_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        default="SALESLOFT_API_KEY",
        description="Env var holding the Salesloft API key (Bearer token)",
    )
    base_url: str = Field(
        default="https://api.salesloft.com/v2",
        description="Salesloft API base URL",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = SalesloftResource(
            api_key_env_var=self.api_key_env_var,
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
