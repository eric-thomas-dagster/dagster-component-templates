"""Gorgias Resource.

Wraps Gorgias's REST API. Gorgias account URLs look like:
  https://<subdomain>.gorgias.com/api/

Auth: HTTP Basic Auth using the account email as the username and a personal
REST API key (Settings > REST API in the Gorgias admin) as the password. This
key grants full account access -- treat it like a password, and resetting it
invalidates the old one immediately.
"""
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class GorgiasResource(dg.ConfigurableResource):
    """Gorgias REST API client wrapper (HTTP Basic Auth)."""

    subdomain: str = Field(
        description="Gorgias account subdomain, e.g. 'acme' for https://acme.gorgias.com"
    )
    email_env_var: str = Field(
        description="Env var holding the Gorgias account email used as the Basic Auth username"
    )
    api_key_env_var: str = Field(
        description="Env var holding the Gorgias REST API key (Settings > REST API), used as the Basic Auth password"
    )

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        import requests

        email = os.environ.get(self.email_env_var)
        api_key = os.environ.get(self.api_key_env_var)
        if not all([email, api_key]):
            raise RuntimeError(
                f"Missing Gorgias Basic Auth env vars: {self.email_env_var}, {self.api_key_env_var}"
            )
        url = f"https://{self.subdomain}.gorgias.com/api/{path.lstrip('/')}"
        resp = requests.get(url, params=params, auth=(email, api_key), timeout=60)
        resp.raise_for_status()
        return resp.json()


class GorgiasResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a GorgiasResource for use by other components."""

    resource_key: str = Field(
        default="gorgias_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    subdomain: str = Field(
        description="Gorgias account subdomain, e.g. 'acme' for https://acme.gorgias.com"
    )
    email_env_var: str = Field(
        description="Env var holding the Gorgias account email used as the Basic Auth username"
    )
    api_key_env_var: str = Field(
        description="Env var holding the Gorgias REST API key"
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={
            self.resource_key: GorgiasResource(
                subdomain=self.subdomain,
                email_env_var=self.email_env_var,
                api_key_env_var=self.api_key_env_var,
            )
        })
