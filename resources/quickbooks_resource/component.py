"""QuickBooks Online Resource.

Wraps the QuickBooks Online Accounting API (`v3/company/<realmId>/...`).
Auth: OAuth 2.0 refresh-token flow. Every request additionally requires
the `realm_id` (QuickBooks' name for the company/tenant id returned during
the OAuth authorization callback) as a URL path segment -- there is no
account-wide endpoint, every call is scoped to one company.

Token refresh: `POST https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer`
with `grant_type=refresh_token`, Basic-Auth'd with `client_id:client_secret`.

IMPORTANT caveat (documented, not silently glossed over): Intuit rotates the
refresh token itself on every refresh (new refresh_token returned each
call), with the old one invalidated after a grace window. This resource
caches the in-memory access token for its ~1hr lifetime within a process,
but does NOT persist newly-rotated refresh tokens back to the env var or
any secret store -- operators running this in production need an external
mechanism (secret-manager write-back, a scheduled re-auth job, etc.) to
keep the stored refresh token current, or access will eventually fail with
`invalid_grant` once the original refresh token rotates past its window.
"""
import os
import time
from typing import Any, Optional

import dagster as dg
from pydantic import Field

_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
_API_BASE = "https://quickbooks.api.intuit.com/v3/company"


class QuickBooksResource(dg.ConfigurableResource):
    """QuickBooks Online Accounting API client wrapper. OAuth2
    refresh-token flow; every request is scoped to `realm_id`."""

    realm_id: str = Field(description="QuickBooks company/tenant id (realmId).")
    client_id_env_var: str = Field(description="Env var with OAuth2 Client ID")
    client_secret_env_var: str = Field(description="Env var with OAuth2 Client Secret")
    refresh_token_env_var: str = Field(description="Env var with OAuth2 Refresh Token")
    minor_version: int = Field(
        default=75,
        description="QuickBooks API minor version (appended to every request).",
    )

    _token_cache: dict = {}

    def _get_access_token(self) -> str:
        import requests

        cache = self._token_cache.get(self.realm_id) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        refresh_token = os.environ.get(self.refresh_token_env_var)
        if not all([client_id, client_secret, refresh_token]):
            raise RuntimeError("Missing QuickBooks OAuth2 env vars")

        resp = requests.post(
            _TOKEN_URL,
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            auth=(client_id, client_secret),
            headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data["access_token"]
        self._token_cache[self.realm_id] = {
            "token": token,
            "expires": time.time() + data.get("expires_in", 3600),
        }
        return token

    def request(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> dict:
        import requests

        token = self._get_access_token()
        url = f"{_API_BASE}/{self.realm_id}/{path.lstrip('/')}"
        all_params = dict(params or {})
        all_params.setdefault("minorversion", self.minor_version)
        resp = requests.request(
            method,
            url,
            json=json_body,
            params=all_params,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def query(self, query_str: str) -> dict:
        """Run a QBO SQL-like query, e.g. `SELECT * FROM Customer WHERE
        DisplayName = 'Acme Co'`. Returns the raw QueryResponse envelope."""
        return self.request("GET", "query", params={"query": query_str})


class QuickBooksResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a QuickBooksResource for use by other components."""

    resource_key: str = Field(default="quickbooks", description="Dagster resource key")
    realm_id: str = Field(description="QuickBooks company/tenant id (realmId).")
    client_id_env_var: str = Field(description="Env var with OAuth2 Client ID")
    client_secret_env_var: str = Field(description="Env var with OAuth2 Client Secret")
    refresh_token_env_var: str = Field(description="Env var with OAuth2 Refresh Token")
    minor_version: int = Field(default=75, description="QuickBooks API minor version.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: QuickBooksResource(
            realm_id=self.realm_id,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            refresh_token_env_var=self.refresh_token_env_var,
            minor_version=self.minor_version,
        )})
