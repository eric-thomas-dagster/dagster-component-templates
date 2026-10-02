"""Marketo Resource.

Wraps Marketo's REST API. Marketo munchkin URLs look like:
  https://<munchkin-id>.mktorest.com

Auth: OAuth2 client credentials grant.

`.post()` reuses the same token-caching `_get_access_token()` as `.get()` --
added so write-side components (e.g. lead upsert via
`POST /rest/v1/leads.json`) can share this resource without re-implementing
OAuth.
"""
import os, time
from typing import Optional
import dagster as dg
from pydantic import Field


class MarketoResource(dg.ConfigurableResource):
    """Marketo REST API client wrapper."""

    rest_url: str = Field(description="Marketo REST URL, e.g. 'https://123-ABC-456.mktorest.com'")
    client_id_env_var: str = Field(description="Env var with OAuth Client ID")
    client_secret_env_var: str = Field(description="Env var with OAuth Client Secret")

    _token_cache: dict = {}

    def _get_access_token(self) -> str:
        # NOTE: must read/write via `self._token_cache` (the per-instance
        # pydantic PrivateAttr value), NOT `MarketoResource._token_cache`
        # (the class attribute) -- the latter is a `ModelPrivateAttr`
        # descriptor object, not the dict, and `.get()`/`[]=` on it raises
        # AttributeError. This was a pre-existing bug that made every real
        # call to `_get_access_token()` (and therefore `.get()`) fail before
        # ever reaching the network -- fixed here since `.post()` mirrors
        # this same caching logic.
        import requests
        cache = self._token_cache.get(self.rest_url) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["token"]
        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if not all([client_id, client_secret]):
            raise RuntimeError("Missing Marketo OAuth env vars")
        resp = requests.get(
            f"{self.rest_url.rstrip('/')}/identity/oauth/token",
            params={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data["access_token"]
        self._token_cache[self.rest_url] = {"token": token, "expires": time.time() + data.get("expires_in", 3600)}
        return token

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        import requests
        url = f"{self.rest_url.rstrip('/')}/{path.lstrip('/')}"
        token = self._get_access_token()
        resp = requests.get(url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, json_body: Optional[dict] = None) -> dict:
        import requests
        url = f"{self.rest_url.rstrip('/')}/{path.lstrip('/')}"
        token = self._get_access_token()
        resp = requests.post(url, json=json_body, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        resp.raise_for_status()
        return resp.json()


class MarketoResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Marketo REST API resource."""

    resource_key: str = Field(default="marketo", description="Dagster resource key")
    rest_url: str = Field(description="Marketo REST URL")
    client_id_env_var: str = Field(description="Env var with OAuth Client ID")
    client_secret_env_var: str = Field(description="Env var with OAuth Client Secret")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: MarketoResource(
            rest_url=self.rest_url,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
        )})
