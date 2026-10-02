"""Outreach Resource.

Wraps Outreach's REST API (https://api.outreach.io/api/v2), which is
JSON:API-shaped (`data`/`attributes`/`relationships`, `links.next` cursor
pagination).

Auth: standard OAuth2 refresh_token grant (NOT Outreach's newer "S2S" app
flow, which requires an RSA keypair + app install and is meant for
marketplace apps, not a single-org server-side Dagster pipeline). A
one-time authorization-code exchange (done once, outside this resource,
e.g. via Outreach's OAuth playground or a short script) produces the
initial refresh_token; after that, this resource refreshes automatically.

Token endpoint: POST https://api.outreach.io/oauth/token
  grant_type=refresh_token, client_id, client_secret, refresh_token

Gotcha: Outreach ROTATES the refresh_token on every exchange -- the
response's `refresh_token` is not the same one you sent, and the old one
stops working. This resource caches the latest refresh_token in-memory
(class-level, keyed by client_id env var) so repeated calls within one
process reuse the rotated token instead of replaying the stale one from
the environment. Refresh tokens are valid 14 days, so a long-lived
process that refreshes at least that often never needs re-authorization;
a process that restarts after 14+ days of inactivity will need a fresh
refresh_token seeded into the env var.
"""
import os
import time
from typing import Optional

import dagster as dg
from pydantic import Field


class OutreachResource(dg.ConfigurableResource):
    """Outreach REST API client wrapper (OAuth2 refresh_token grant)."""

    client_id_env_var: str = Field(description="Env var holding the Outreach OAuth Client ID")
    client_secret_env_var: str = Field(description="Env var holding the Outreach OAuth Client Secret")
    refresh_token_env_var: str = Field(
        description="Env var holding the initial OAuth refresh token (obtained once via the authorization_code grant)"
    )
    base_url: str = Field(
        default="https://api.outreach.io/api/v2",
        description="Outreach API base URL",
    )
    token_url: str = Field(
        default="https://api.outreach.io/oauth/token",
        description="Outreach OAuth token endpoint",
    )

    _token_cache: dict = {}

    def _get_access_token(self) -> str:
        import requests

        cache_key = self.client_id_env_var
        cache = OutreachResource._token_cache.get(cache_key) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["access_token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        # Prefer the rotated refresh_token from a prior exchange this process
        # already made; fall back to the one seeded via env var.
        refresh_token = cache.get("refresh_token") or os.environ.get(self.refresh_token_env_var)
        if not all([client_id, client_secret, refresh_token]):
            raise RuntimeError(
                "Missing Outreach OAuth env vars (client_id/client_secret/refresh_token)"
            )

        resp = requests.post(
            self.token_url,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        new_refresh_token = data.get("refresh_token") or refresh_token
        OutreachResource._token_cache[cache_key] = {
            "access_token": data["access_token"],
            # Refresh a little early (60s) rather than racing expiry.
            "expires": time.time() + data.get("expires_in", 7200) - 60,
            "refresh_token": new_refresh_token,
        }
        return data["access_token"]

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET a path (relative to base_url) or a full URL (e.g. a JSON:API
        `links.next` cursor URL, which already carries its own querystring)."""
        import requests

        url = path if path.startswith("http") else f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        token = self._get_access_token()
        resp = requests.get(
            url,
            params=params,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/vnd.api+json",
                "Accept": "application/vnd.api+json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class OutreachResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an OutreachResource for use by other components."""

    resource_key: str = Field(
        default="outreach_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    client_id_env_var: str = Field(
        default="OUTREACH_CLIENT_ID",
        description="Env var holding the Outreach OAuth Client ID",
    )
    client_secret_env_var: str = Field(
        default="OUTREACH_CLIENT_SECRET",
        description="Env var holding the Outreach OAuth Client Secret",
    )
    refresh_token_env_var: str = Field(
        default="OUTREACH_REFRESH_TOKEN",
        description="Env var holding the initial OAuth refresh token",
    )
    base_url: str = Field(
        default="https://api.outreach.io/api/v2",
        description="Outreach API base URL",
    )
    token_url: str = Field(
        default="https://api.outreach.io/oauth/token",
        description="Outreach OAuth token endpoint",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = OutreachResource(
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            refresh_token_env_var=self.refresh_token_env_var,
            base_url=self.base_url,
            token_url=self.token_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
