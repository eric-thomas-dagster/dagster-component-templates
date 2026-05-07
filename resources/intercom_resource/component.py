"""Intercom Resource.

Wraps Intercom's REST API for support data ingestion: contacts,
conversations, tags, custom data attributes. Auth: API key (Bearer).
"""
import os
from typing import Optional
import dagster as dg
from pydantic import Field


class IntercomResource(dg.ConfigurableResource):
    """Intercom REST API client."""

    api_token_env_var: str = Field(description="Env var with Intercom API token")
    base_url: str = Field(default="https://api.intercom.io", description="API base URL")

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        import requests
        token = os.environ.get(self.api_token_env_var)
        if not token:
            raise RuntimeError(f"Missing {self.api_token_env_var}")
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.get(
            url,
            params=params,
            headers={
                "Authorization": f"Bearer {token}",
                "Intercom-Version": "2.11",
                "Accept": "application/json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class IntercomResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Intercom REST API resource."""

    resource_key: str = Field(default="intercom", description="Dagster resource key")
    api_token_env_var: str = Field(description="Env var with Intercom API token")
    base_url: str = Field(default="https://api.intercom.io")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: IntercomResource(
            api_token_env_var=self.api_token_env_var,
            base_url=self.base_url,
        )})
