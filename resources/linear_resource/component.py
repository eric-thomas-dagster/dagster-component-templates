"""Linear Resource.

Wraps Linear's GraphQL API (https://api.linear.app/graphql -- Linear has no
REST surface at all, GraphQL is the only way in).

Auth: personal API key or OAuth2 access token, sent RAW in the
`Authorization` header -- unlike most APIs, Linear does NOT want a `Bearer `
prefix for a personal API key (same convention already established by
`linear_ingestion` in this repo). An OAuth2 access token also works in the
same header, raw.
"""
import os
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class LinearResource(dg.ConfigurableResource):
    """Linear GraphQL API client wrapper (raw-token Authorization header)."""

    api_key_env_var: str = Field(
        description="Env var holding a Linear personal API key or OAuth2 access token."
    )
    base_url: str = Field(
        default="https://api.linear.app/graphql",
        description="Linear GraphQL endpoint.",
    )

    def graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> dict:
        """Execute one GraphQL query/mutation. Raises on transport errors
        (non-2xx) AND on a GraphQL-level `errors` array in an otherwise-200
        response (Linear, like most GraphQL APIs, can return 200 with
        `errors` populated)."""
        import requests

        api_key = os.environ.get(self.api_key_env_var)
        if not api_key:
            raise RuntimeError(f"Missing Linear API key/token env var: {self.api_key_env_var}")

        resp = requests.post(
            self.base_url,
            json={"query": query, "variables": variables or {}},
            headers={"Authorization": api_key, "Content-Type": "application/json"},
            timeout=60,
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("errors"):
            raise RuntimeError(f"Linear GraphQL error: {payload['errors']}")
        return payload["data"]


class LinearResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a LinearResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.LinearResourceComponent
        attributes:
          resource_key: linear_resource
          api_key_env_var: LINEAR_API_KEY
        ```
    """

    resource_key: str = Field(
        default="linear_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        default="LINEAR_API_KEY",
        description="Env var holding a Linear personal API key or OAuth2 access token.",
    )
    base_url: str = Field(
        default="https://api.linear.app/graphql",
        description="Linear GraphQL endpoint.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = LinearResource(
            api_key_env_var=self.api_key_env_var,
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
