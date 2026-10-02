"""Recurly Resource.

Wraps the Recurly API v3 (`https://v3.recurly.com/...`).

Auth: HTTP Basic, with the Recurly **private** API key as the username and
an empty password -- no OAuth2, no token to refresh. This matches the
convention already used by this repo's `recurly_ingestion` component.

IMPORTANT versioning caveat (documented, not silently glossed over): Recurly
v3 requires an explicit API-version `Accept` header on every request --
`Accept: application/vnd.recurly.v2021-02-25+json` (dated version string).
Omitting it does not reliably fall back to "latest"; Recurly expects every
integration to pin a version. This resource defaults to `2021-02-25` (the
long-stable v3 version most client libraries target) but exposes
`api_version` so it can be overridden.
"""
import os
from typing import Any, Optional

import dagster as dg
from pydantic import Field

_API_BASE = "https://v3.recurly.com"


class RecurlyResource(dg.ConfigurableResource):
    """Recurly API v3 client wrapper. HTTP Basic auth (private API key,
    blank password); every request pins a dated API version via the
    Accept header."""

    api_key_env_var: str = Field(description="Env var with the Recurly PRIVATE API key.")
    api_version: str = Field(
        default="2021-02-25",
        description="Recurly dated API version (sent as 'application/vnd.recurly.v<version>+json').",
    )

    def request(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
    ):
        """Generic request against v3.recurly.com. Returns the parsed JSON
        body on success. On a 404, returns None (instead of raising) so
        upsert-style "does this account exist?" lookups can branch on it
        without a try/except at every call site; all other non-2xx status
        codes raise via `raise_for_status()`."""
        import requests

        api_key = os.environ.get(self.api_key_env_var)
        if not api_key:
            raise RuntimeError(f"Missing Recurly API key env var {self.api_key_env_var!r}")

        url = f"{_API_BASE}/{path.lstrip('/')}"
        resp = requests.request(
            method,
            url,
            json=json_body,
            params=params,
            auth=(api_key, ""),
            headers={
                "Accept": f"application/vnd.recurly.v{self.api_version}+json",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        if not resp.content:
            return {}
        return resp.json()


class RecurlyResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a RecurlyResource for use by other components."""

    resource_key: str = Field(default="recurly", description="Dagster resource key")
    api_key_env_var: str = Field(description="Env var with the Recurly PRIVATE API key.")
    api_version: str = Field(default="2021-02-25", description="Recurly dated API version.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: RecurlyResource(
            api_key_env_var=self.api_key_env_var,
            api_version=self.api_version,
        )})
