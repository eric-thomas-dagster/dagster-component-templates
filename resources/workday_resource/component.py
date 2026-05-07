"""Workday Resource.

Workday provides REST APIs (Workday REST + Raas reports) and SOAP APIs.
This resource wraps the REST API. Auth: OAuth2 via the tenant's Workday
OAuth Client Grant flow.

Tenant URLs are like:
  https://wd5-impl-services1.workday.com/ccx/api/v1/<tenant>/<resource>
"""
import os
from typing import Optional
import dagster as dg
from pydantic import Field


class WorkdayResource(dg.ConfigurableResource):
    """Workday REST API client wrapper."""

    tenant_url: str = Field(description="Tenant base URL, e.g. 'https://wd5-impl-services1.workday.com/ccx/api/v1/<tenant>'")
    client_id_env_var: str = Field(description="Env var with OAuth Client ID")
    client_secret_env_var: str = Field(description="Env var with OAuth Client Secret")
    refresh_token_env_var: str = Field(description="Env var with Refresh Token (long-lived)")
    token_url: Optional[str] = Field(default=None, description="OAuth token endpoint URL (default: <tenant_url>/token)")

    def _get_access_token(self) -> str:
        import requests
        token_url = self.token_url or f"{self.tenant_url.rstrip('/')}/token"
        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        refresh_token = os.environ.get(self.refresh_token_env_var)
        if not all([client_id, client_secret, refresh_token]):
            raise RuntimeError("Missing one of Workday OAuth env vars")
        resp = requests.post(
            token_url,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        import requests
        token = self._get_access_token()
        url = f"{self.tenant_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.get(url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        resp.raise_for_status()
        return resp.json()


class WorkdayResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Workday REST API resource."""

    resource_key: str = Field(default="workday", description="Dagster resource key")
    tenant_url: str = Field(description="Tenant base URL")
    client_id_env_var: str = Field(description="Env var with OAuth Client ID")
    client_secret_env_var: str = Field(description="Env var with OAuth Client Secret")
    refresh_token_env_var: str = Field(description="Env var with Refresh Token")
    token_url: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: WorkdayResource(
            tenant_url=self.tenant_url,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            refresh_token_env_var=self.refresh_token_env_var,
            token_url=self.token_url,
        )})
