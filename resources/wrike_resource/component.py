"""Wrike Resource.

Wraps Wrike's REST API v4 (https://www.wrike.com/api/v4).

Auth: permanent access token (Apps & Integrations -> API), sent as a
standard `Bearer` token in the `Authorization` header. Same convention
already established by `wrike_ingestion` in this repo.
"""
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class WrikeResource(dg.ConfigurableResource):
    """Wrike REST API v4 client wrapper (Bearer token auth)."""

    access_token_env_var: str = Field(
        description="Env var holding a Wrike permanent access token."
    )
    base_url: str = Field(
        default="https://www.wrike.com/api/v4",
        description="Wrike API base URL.",
    )

    def request(self, method: str, path: str, params: Optional[dict] = None, json_body: Optional[dict] = None) -> dict:
        """Issue one HTTP request (relative to base_url)."""
        import requests

        access_token = os.environ.get(self.access_token_env_var)
        if not access_token:
            raise RuntimeError(f"Missing Wrike access token env var: {self.access_token_env_var}")

        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.request(
            method,
            url,
            params=params,
            json=json_body,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=60,
        )
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()


class WrikeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a WrikeResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.WrikeResourceComponent
        attributes:
          resource_key: wrike_resource
          access_token_env_var: WRIKE_ACCESS_TOKEN
        ```
    """

    resource_key: str = Field(
        default="wrike_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    access_token_env_var: str = Field(
        default="WRIKE_ACCESS_TOKEN",
        description="Env var holding a Wrike permanent access token.",
    )
    base_url: str = Field(
        default="https://www.wrike.com/api/v4",
        description="Wrike API base URL.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = WrikeResource(
            access_token_env_var=self.access_token_env_var,
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
