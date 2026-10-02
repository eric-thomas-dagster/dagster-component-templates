"""Xero Resource.

Wraps the Xero Accounting API (`https://api.xero.com/api.xro/2.0/...`).

Auth: OAuth 2.0 refresh-token flow -- `POST https://identity.xero.com/connect/token`
with `grant_type=refresh_token`, `client_id` + `client_secret` + `refresh_token`
in the form body (Xero does NOT use HTTP Basic for this call -- client_id/
client_secret are posted as regular form fields, unlike QuickBooks/Zuora).

Xero is multi-tenant per OAuth2 app connection: a single access token can be
authorized against multiple orgs ("tenants"), and every API call must carry
an `Xero-tenant-id` header identifying which one. The tenant id is NOT the
access token's `sub` claim -- it's discovered via a dedicated endpoint,
`GET https://api.xero.com/connections`, which returns the list of tenants
the current token is authorized for. This resource discovers the first
connected tenant automatically (caching it), or an explicit `tenant_id` may
be supplied to skip discovery (recommended when more than one tenant is
connected to the same app, since discovery order is otherwise undefined).

IMPORTANT caveat (documented, not silently glossed over): like QuickBooks,
Xero rotates the refresh token on every token refresh for public/PKCE OAuth2
apps (~60-day refresh token lifetime). This resource caches the in-memory
access token for its ~30min lifetime within a process, but does NOT persist
newly-rotated refresh tokens back to the env var or any secret store --
operators need an external mechanism to keep the stored refresh token
current, or access eventually fails with `invalid_grant`.
"""
import os
import time
from typing import Any, Optional

import dagster as dg
from pydantic import Field

_TOKEN_URL = "https://identity.xero.com/connect/token"
_CONNECTIONS_URL = "https://api.xero.com/connections"
_API_BASE = "https://api.xero.com/api.xro/2.0"


class XeroResource(dg.ConfigurableResource):
    """Xero Accounting API client wrapper. OAuth2 refresh-token flow;
    every request is scoped to an `Xero-tenant-id` header."""

    client_id_env_var: str = Field(description="Env var with OAuth2 Client ID")
    client_secret_env_var: str = Field(description="Env var with OAuth2 Client Secret")
    refresh_token_env_var: str = Field(description="Env var with OAuth2 Refresh Token")
    tenant_id: Optional[str] = Field(
        default=None,
        description=(
            "Xero tenant (org) id. If unset, auto-discovered via "
            "GET /connections (uses the first connected tenant -- set "
            "explicitly if your app is connected to more than one org)."
        ),
    )

    _token_cache: dict = {}
    _tenant_id_cache: dict = {}

    def _get_access_token(self) -> str:
        import requests

        cache_key = self.refresh_token_env_var
        cache = self._token_cache.get(cache_key) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        refresh_token = os.environ.get(self.refresh_token_env_var)
        if not all([client_id, client_secret, refresh_token]):
            raise RuntimeError("Missing Xero OAuth2 env vars")

        resp = requests.post(
            _TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data["access_token"]
        self._token_cache[cache_key] = {
            "token": token,
            "expires": time.time() + data.get("expires_in", 1800),
        }
        return token

    def _get_tenant_id(self, token: str) -> str:
        import requests

        if self.tenant_id:
            return self.tenant_id

        cache_key = self.refresh_token_env_var
        cached = self._tenant_id_cache.get(cache_key)
        if cached:
            return cached

        resp = requests.get(
            _CONNECTIONS_URL,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        connections = resp.json() or []
        if not connections:
            raise RuntimeError(
                "Xero /connections returned no authorized tenants for this token."
            )
        discovered = connections[0]["tenantId"]
        self._tenant_id_cache[cache_key] = discovered
        return discovered

    def request(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> Any:
        import requests

        token = self._get_access_token()
        tenant_id = self._get_tenant_id(token)
        url = f"{_API_BASE}/{path.lstrip('/')}"
        resp = requests.request(
            method,
            url,
            json=json_body,
            params=params,
            headers={
                "Authorization": f"Bearer {token}",
                "Xero-tenant-id": tenant_id,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        if not resp.content:
            return {}
        return resp.json()


class XeroResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a XeroResource for use by other components."""

    resource_key: str = Field(default="xero", description="Dagster resource key")
    client_id_env_var: str = Field(description="Env var with OAuth2 Client ID")
    client_secret_env_var: str = Field(description="Env var with OAuth2 Client Secret")
    refresh_token_env_var: str = Field(description="Env var with OAuth2 Refresh Token")
    tenant_id: Optional[str] = Field(
        default=None,
        description="Xero tenant (org) id. If unset, auto-discovered via GET /connections.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: XeroResource(
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            refresh_token_env_var=self.refresh_token_env_var,
            tenant_id=self.tenant_id,
        )})
