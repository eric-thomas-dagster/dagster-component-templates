"""Kustomer Resource.

Wraps Kustomer's REST API. Kustomer base URLs are org-scoped:
  https://<org_subdomain>.api.kustomerapp.com

Auth: a Bearer API key created under Kustomer Settings > Security > API Keys.
Most read endpoints are simple `GET`, but Kustomer's best-documented way to
bulk-list/filter Conversations is the Search API (`POST /v1/customers/search`
with `queryContext: "conversation"`), so this resource exposes both `get` and
`post` helpers.
"""
import os
from typing import Any, Optional

import dagster as dg
from pydantic import Field


class KustomerResource(dg.ConfigurableResource):
    """Kustomer REST API client wrapper (Bearer API key)."""

    org_subdomain: str = Field(
        description="Kustomer org subdomain, e.g. 'acme' for https://acme.api.kustomerapp.com"
    )
    api_key_env_var: str = Field(
        description="Env var holding the Kustomer API key (Settings > Security > API Keys)"
    )

    def _headers(self) -> dict:
        api_key = os.environ.get(self.api_key_env_var)
        if not api_key:
            raise RuntimeError(f"Missing Kustomer API key env var: {self.api_key_env_var}")
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def _base_url(self) -> str:
        return f"https://{self.org_subdomain}.api.kustomerapp.com"

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        import requests

        url = f"{self._base_url()}/{path.lstrip('/')}"
        resp = requests.get(url, params=params, headers=self._headers(), timeout=60)
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, json: Optional[Any] = None) -> dict:
        import requests

        url = f"{self._base_url()}/{path.lstrip('/')}"
        resp = requests.post(url, json=json, headers=self._headers(), timeout=60)
        resp.raise_for_status()
        return resp.json()


class KustomerResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a KustomerResource for use by other components."""

    resource_key: str = Field(
        default="kustomer_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    org_subdomain: str = Field(
        description="Kustomer org subdomain, e.g. 'acme' for https://acme.api.kustomerapp.com"
    )
    api_key_env_var: str = Field(
        description="Env var holding the Kustomer API key"
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={
            self.resource_key: KustomerResource(
                org_subdomain=self.org_subdomain,
                api_key_env_var=self.api_key_env_var,
            )
        })
