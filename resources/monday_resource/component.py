"""Monday.com Resource.

Wraps monday.com's GraphQL API:

    POST https://api.monday.com/v2

A single endpoint, not path-based REST -- every call is a `{"query": "...",
"variables": {...}}` JSON body. Auth is a raw API token in the
`Authorization` header (monday.com does NOT use a `Bearer ` prefix -- the
token goes in as-is, per https://developer.monday.com/api-reference/docs/getting-started).
monday.com also requires an `API-Version` header (e.g. `"2026-07"`); omitting
it is a common source of 4xx errors for new integrations.
"""
import os
from typing import Any, Dict, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class MondayGraphQLError(RuntimeError):
    """Raised when monday.com's response body contains a top-level `errors` array."""


class MondayResource(ConfigurableResource):
    """Dagster resource wrapping the monday.com GraphQL API."""

    api_token_env_var: str = Field(description="Env var holding the monday.com API token")
    api_url: str = Field(default="https://api.monday.com/v2", description="monday.com GraphQL endpoint")
    api_version: str = Field(
        default="2026-07",
        description="monday.com API version sent via the 'API-Version' header",
    )
    timeout: int = Field(default=60, description="HTTP timeout in seconds")

    def _headers(self) -> Dict[str, str]:
        token = os.environ.get(self.api_token_env_var)
        if not token:
            raise RuntimeError(f"Missing env var {self.api_token_env_var!r} for monday.com API token")
        return {
            "Authorization": token,
            "Content-Type": "application/json",
            "API-Version": self.api_version,
        }

    def execute(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """POST a GraphQL query (+ optional variables) and return the `data` object.

        Raises `MondayGraphQLError` if the response body carries a top-level
        `errors` array (monday.com's GraphQL layer returns HTTP 200 with
        `errors` for most query-level failures, not a 4xx)."""
        import requests

        resp = requests.post(
            self.api_url,
            json={"query": query, "variables": variables or {}},
            headers=self._headers(),
            timeout=self.timeout,
        )
        resp.raise_for_status()
        body = resp.json()

        if body.get("errors"):
            messages = "; ".join(
                e.get("message", str(e)) for e in body["errors"]
            )
            raise MondayGraphQLError(f"monday.com GraphQL error: {messages}")

        return body.get("data") or {}


class MondayResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a MondayResource wrapping the monday.com GraphQL API."""

    resource_key: str = Field(default="monday_resource", description="Dagster resource key")
    api_token_env_var: str = Field(description="Env var holding the monday.com API token")
    api_url: str = Field(default="https://api.monday.com/v2", description="monday.com GraphQL endpoint")
    api_version: str = Field(
        default="2026-07",
        description="monday.com API version sent via the 'API-Version' header",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(
            resources={
                self.resource_key: MondayResource(
                    api_token_env_var=self.api_token_env_var,
                    api_url=self.api_url,
                    api_version=self.api_version,
                )
            }
        )
