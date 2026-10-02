"""Drift Resource.

Wraps Drift's REST API (base URL `https://driftapi.com`). Auth is OAuth2
Bearer token -- either a non-expiring private-app token (recommended for
server-to-server integrations like this one) or a token minted via the
OAuth2 Authorization Code flow. This resource does not perform the OAuth
dance itself; it expects a already-issued access token in an env var.
"""
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class DriftResource(dg.ConfigurableResource):
    """Drift REST API client wrapper (OAuth2 Bearer token)."""

    access_token_env_var: str = Field(
        description="Env var holding a Drift API access token (private-app token or OAuth2 access token)"
    )
    base_url: str = Field(
        default="https://driftapi.com",
        description="Drift API base URL. Override only for Drift-provided test/sandbox environments.",
    )

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        import requests

        token = os.environ.get(self.access_token_env_var)
        if not token:
            raise RuntimeError(f"Missing Drift access token env var: {self.access_token_env_var}")
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.get(
            url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=60
        )
        resp.raise_for_status()
        return resp.json()


class DriftResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a DriftResource for use by other components."""

    resource_key: str = Field(
        default="drift_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    access_token_env_var: str = Field(
        description="Env var holding a Drift API access token"
    )
    base_url: str = Field(
        default="https://driftapi.com",
        description="Drift API base URL",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={
            self.resource_key: DriftResource(
                access_token_env_var=self.access_token_env_var,
                base_url=self.base_url,
            )
        })
