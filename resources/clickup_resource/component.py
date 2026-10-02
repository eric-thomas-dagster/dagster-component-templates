"""ClickUp Resource.

Wraps ClickUp's REST API v2 (https://api.clickup.com/api/v2).

Auth: personal API token (or OAuth2 access token), sent RAW in the
`Authorization` header -- ClickUp does NOT want a `Bearer ` prefix (same
convention already established by `clickup_ingestion` in this repo, which
configures `dlt`'s rest_api_source with `"name": "Authorization"` and no
prefix).
"""
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class ClickUpResource(dg.ConfigurableResource):
    """ClickUp REST API v2 client wrapper (raw-token Authorization header)."""

    api_token_env_var: str = Field(
        description="Env var holding a ClickUp personal API token or OAuth2 access token."
    )
    base_url: str = Field(
        default="https://api.clickup.com/api/v2",
        description="ClickUp API base URL.",
    )

    def request(self, method: str, path: str, params: Optional[dict] = None, json_body: Optional[dict] = None) -> dict:
        """Issue one HTTP request (relative to base_url)."""
        import requests

        api_token = os.environ.get(self.api_token_env_var)
        if not api_token:
            raise RuntimeError(f"Missing ClickUp API token env var: {self.api_token_env_var}")

        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.request(
            method,
            url,
            params=params,
            json=json_body,
            headers={"Authorization": api_token, "Content-Type": "application/json"},
            timeout=60,
        )
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()


class ClickUpResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ClickUpResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.ClickUpResourceComponent
        attributes:
          resource_key: clickup_resource
          api_token_env_var: CLICKUP_API_TOKEN
        ```
    """

    resource_key: str = Field(
        default="clickup_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_token_env_var: str = Field(
        default="CLICKUP_API_TOKEN",
        description="Env var holding a ClickUp personal API token or OAuth2 access token.",
    )
    base_url: str = Field(
        default="https://api.clickup.com/api/v2",
        description="ClickUp API base URL.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = ClickUpResource(
            api_token_env_var=self.api_token_env_var,
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
