"""Zuora Resource.

Wraps the Zuora REST API (`https://<region-host>/v1/...`, e.g.
`rest.na.zuora.com`, `rest.eu.zuora.com`, or a sandbox host).

Auth: OAuth 2.0 client-credentials flow -- `POST <base_url>/oauth/token`
with `grant_type=client_credentials`, `client_id` + `client_secret` in the
form body. This is NOT the old session-based "Z-Session" auth (a
username/password login call returning a cookie-like token) -- Zuora
**deprecated all legacy authentication methods (including Z-Session) on
August 1, 2024** across every Zuora application and environment, so
OAuth2 client-credentials is now the only supported mechanism. This
matches the convention already used by this repo's `zuora_ingestion`
component.

Zuora exposes two different write surfaces for an Account:
  - The full-featured `/v1/accounts` endpoint (bundles subscription +
    payment-method creation in one atomic call) -- out of scope here.
  - The lightweight, generic Object CRUD API -- `POST /v1/object/account`
    (create) and `PUT /v1/object/account/{id}` (update) -- which this
    resource's consumers use, since it mirrors a plain record upsert
    without forcing subscription/payment-method fields into every call.

Looking an account up by a business key (e.g. AccountNumber) requires
Zuora's query language (ZOQL) via `POST /v1/action/query`, since the
generic Object CRUD API has no filter-by-field GET -- only GET-by-id.
"""
import os
import time
from typing import Any, Optional

import dagster as dg
from pydantic import Field


class ZuoraResource(dg.ConfigurableResource):
    """Zuora REST API client wrapper. OAuth2 client-credentials flow
    (session-based Z-Session auth was deprecated by Zuora in 2024)."""

    base_url: str = Field(
        description="Zuora REST endpoint (region/environment-specific, e.g. rest.na.zuora.com, rest.eu.zuora.com, or a sandbox host)."
    )
    client_id_env_var: str = Field(description="Env var with the Zuora OAuth2 Client ID.")
    client_secret_env_var: str = Field(description="Env var with the Zuora OAuth2 Client Secret.")

    _token_cache: dict = {}

    def _base(self) -> str:
        b = self.base_url.strip()
        if not b.startswith("http"):
            b = f"https://{b}"
        return b.rstrip("/")

    def _get_access_token(self) -> str:
        import requests

        cache_key = self.client_id_env_var
        cache = self._token_cache.get(cache_key) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if not all([client_id, client_secret]):
            raise RuntimeError("Missing Zuora OAuth2 client-credentials env vars")

        resp = requests.post(
            f"{self._base()}/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data["access_token"]
        self._token_cache[cache_key] = {
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
        url = f"{self._base()}/{path.lstrip('/')}"
        resp = requests.request(
            method,
            url,
            json=json_body,
            params=params,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        if not resp.content:
            return {}
        return resp.json()

    def query(self, zoql: str) -> dict:
        """Run a ZOQL (Zuora Object Query Language) query via the Action
        Query endpoint, e.g. `select Id, AccountNumber from Account where
        AccountNumber = 'A00000123'`. Returns the raw response envelope
        (has a 'records' list)."""
        return self.request("POST", "v1/action/query", json_body={"queryString": zoql})


class ZuoraResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ZuoraResource for use by other components."""

    resource_key: str = Field(default="zuora", description="Dagster resource key")
    base_url: str = Field(description="Zuora REST endpoint (e.g. rest.na.zuora.com).")
    client_id_env_var: str = Field(description="Env var with the Zuora OAuth2 Client ID.")
    client_secret_env_var: str = Field(description="Env var with the Zuora OAuth2 Client Secret.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: ZuoraResource(
            base_url=self.base_url,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
        )})
