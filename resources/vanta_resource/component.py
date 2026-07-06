"""Vanta Resource component.

Wraps the Vanta API v1 (https://developer.vanta.com/). Vanta ships a pure
OAuth2 client-credentials flow: POST client_id + client_secret to
`/oauth/token`, receive a short-lived bearer token (typically ~1h). This
resource mints, caches, and auto-refreshes that token, then exposes an
authenticated `requests.Session` via `get_client()`.

Vanta quirks worth noting:
  - Token TTL comes back in `expires_in` seconds. Refresh a bit early to
    avoid mid-request expiry (see `early_refresh_seconds`).
  - Vanta enforces `grant_type=client_credentials` in the request body
    (form-urlencoded), not Basic auth. We send both id/secret in the body.
  - Rate limits are documented per-endpoint; we don't try to enforce them
    here — caller / ingestion component handles pagination pacing.
"""
import os
import time
from typing import Any

import dagster as dg
import requests
from dagster import ConfigurableResource
from pydantic import Field, PrivateAttr


class VantaResource(ConfigurableResource):
    """Vanta API v1 client with cached OAuth2 client-credentials token.

    Call `get_client()` to receive a `requests.Session` pre-loaded with a
    valid Bearer token. Token is minted on first use and cached in memory
    for the life of the resource instance; auto-refreshed when it's within
    `early_refresh_seconds` of expiry.
    """

    client_id: str = Field(description="Vanta OAuth client_id")
    client_secret: str = Field(description="Vanta OAuth client_secret")
    api_base_url: str = Field(
        default="https://api.vanta.com",
        description="Vanta API base URL (override for staging / EU tenants)",
    )
    token_endpoint_path: str = Field(
        default="/oauth/token",
        description="Path (relative to api_base_url) of the OAuth token endpoint",
    )
    scope: str = Field(
        default="",
        description="Optional space-separated OAuth scopes. Vanta defaults are usually fine.",
    )
    early_refresh_seconds: int = Field(
        default=60,
        description="Refresh the token this many seconds before it expires.",
    )
    timeout_seconds: int = Field(default=30, description="HTTP timeout for token + API calls")

    _cached_token: Any = PrivateAttr(default=None)
    _expires_at: float = PrivateAttr(default=0.0)

    def _mint_token(self) -> tuple[str, int]:
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.scope:
            data["scope"] = self.scope
        url = f"{self.api_base_url.rstrip('/')}{self.token_endpoint_path}"
        resp = requests.post(
            url,
            data=data,
            headers={"Accept": "application/json"},
            timeout=self.timeout_seconds,
        )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"Vanta OAuth token request failed: HTTP {resp.status_code} {resp.text[:500]}"
            )
        body = resp.json()
        access_token = body.get("access_token")
        if not access_token:
            raise RuntimeError(
                f"Vanta token endpoint returned no access_token: keys={list(body.keys())}"
            )
        expires_in = int(body.get("expires_in") or 3600)
        return access_token, expires_in

    def get_access_token(self) -> str:
        """Return a valid Vanta access token, minting or refreshing as needed."""
        now = time.time()
        if self._cached_token and now < self._expires_at - self.early_refresh_seconds:
            return self._cached_token
        token, expires_in = self._mint_token()
        self._cached_token = token
        self._expires_at = now + expires_in
        return token

    def get_client(self) -> requests.Session:
        """Return an authenticated `requests.Session` for Vanta API calls.

        The session's Authorization header is set to a currently-valid bearer
        token. Callers should treat the session as short-lived (per-run) since
        the header is set at hand-out time; long-running loops should call
        `get_client()` again periodically, or use `get_access_token()` per call.
        """
        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {self.get_access_token()}",
            "Accept": "application/json",
        })
        return session


class VantaResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a VantaResource for use by Vanta ingestion / agent components.

    Example:

        ```yaml
        type: dagster_community_components.VantaResourceComponent
        attributes:
          resource_key: vanta_resource
          client_id_env_var: VANTA_CLIENT_ID
          client_secret_env_var: VANTA_CLIENT_SECRET
        ```
    """

    resource_key: str = Field(
        default="vanta_resource",
        description="Key used to register this resource. Other components reference it via resource_name / resource_key.",
    )
    client_id_env_var: str = Field(
        default="VANTA_CLIENT_ID",
        description="Env var holding Vanta OAuth client_id",
    )
    client_secret_env_var: str = Field(
        default="VANTA_CLIENT_SECRET",
        description="Env var holding Vanta OAuth client_secret",
    )
    api_base_url: str = Field(
        default="https://api.vanta.com",
        description="Vanta API base URL (override for staging / regional tenants)",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = VantaResource(
            client_id=dg.EnvVar(self.client_id_env_var),
            client_secret=dg.EnvVar(self.client_secret_env_var),
            api_base_url=self.api_base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
