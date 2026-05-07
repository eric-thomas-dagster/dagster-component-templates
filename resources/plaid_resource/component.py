"""Plaid Resource.

Wraps Plaid's REST API for fintech data ingestion: accounts, transactions,
identity, balance. Plaid has three environments: sandbox, development,
production.
"""
import os
from typing import Optional
import dagster as dg
from pydantic import Field


class PlaidResource(dg.ConfigurableResource):
    """Plaid REST API client."""

    environment: str = Field(default="sandbox", description="'sandbox' | 'development' | 'production'")
    client_id_env_var: str = Field(description="Env var with Plaid client_id")
    secret_env_var: str = Field(description="Env var with Plaid secret")

    @property
    def base_url(self) -> str:
        return {
            "sandbox": "https://sandbox.plaid.com",
            "development": "https://development.plaid.com",
            "production": "https://production.plaid.com",
        }[self.environment]

    def post(self, path: str, body: Optional[dict] = None) -> dict:
        import requests
        cid = os.environ.get(self.client_id_env_var)
        sec = os.environ.get(self.secret_env_var)
        if not all([cid, sec]):
            raise RuntimeError("Missing Plaid client_id or secret env vars")
        body = body or {}
        body["client_id"] = cid
        body["secret"] = sec
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.post(url, json=body, headers={"Content-Type": "application/json"}, timeout=60)
        resp.raise_for_status()
        return resp.json()


class PlaidResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Plaid REST API resource."""

    resource_key: str = Field(default="plaid", description="Dagster resource key")
    environment: str = Field(default="sandbox")
    client_id_env_var: str = Field(description="Env var with Plaid client_id")
    secret_env_var: str = Field(description="Env var with Plaid secret")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: PlaidResource(
            environment=self.environment,
            client_id_env_var=self.client_id_env_var,
            secret_env_var=self.secret_env_var,
        )})
