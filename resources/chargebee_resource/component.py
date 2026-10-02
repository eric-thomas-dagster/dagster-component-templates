"""Chargebee Resource.

Wraps the Chargebee API v2 (`https://<site>.chargebee.com/api/v2/...`).

Auth: HTTP Basic, with the Chargebee API key as the username and an EMPTY
password -- there is no OAuth2 flow and no token to refresh. This mirrors
the convention already used by this repo's `chargebee_ingestion` component.

IMPORTANT API-shape caveat (documented, not silently glossed over):
Chargebee's REST API uses **POST for every mutating operation** -- create,
update, AND delete all go through POST. There is no PUT/PATCH/DELETE verb
anywhere in the API. The only thing that distinguishes "create a customer"
from "update a customer" is the URL: `POST /customers` (create) vs.
`POST /customers/{id}` (update, partial-merge semantics -- only fields you
send are changed). Passing `method="PUT"` or similar to this resource's
`request()` would be a caller bug, not a Chargebee requirement.
"""
import os
from typing import Any, Optional

import dagster as dg
from pydantic import Field


class ChargebeeResource(dg.ConfigurableResource):
    """Chargebee API v2 client wrapper. HTTP Basic auth (api_key, blank
    password); every mutating call is a POST (create vs. update is
    determined by whether the path includes a resource id)."""

    site: str = Field(description="Chargebee site name (for <site>.chargebee.com).")
    api_key_env_var: str = Field(description="Env var with the Chargebee API key.")

    def _base_url(self) -> str:
        return f"https://{self.site}.chargebee.com/api/v2"

    def request(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> dict:
        import requests

        api_key = os.environ.get(self.api_key_env_var)
        if not api_key:
            raise RuntimeError(f"Missing Chargebee API key env var {self.api_key_env_var!r}")

        url = f"{self._base_url()}/{path.lstrip('/')}"
        resp = requests.request(
            method,
            url,
            data=json_body,
            params=params,
            auth=(api_key, ""),
            headers={"Accept": "application/json"},
            timeout=60,
        )
        resp.raise_for_status()
        if not resp.content:
            return {}
        return resp.json()


class ChargebeeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ChargebeeResource for use by other components."""

    resource_key: str = Field(default="chargebee", description="Dagster resource key")
    site: str = Field(description="Chargebee site name (for <site>.chargebee.com).")
    api_key_env_var: str = Field(description="Env var with the Chargebee API key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: ChargebeeResource(
            site=self.site,
            api_key_env_var=self.api_key_env_var,
        )})
