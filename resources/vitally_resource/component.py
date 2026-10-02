"""Vitally Resource.

Wraps Vitally's REST API. Vitally's API is account-scoped by subdomain:
  US:  https://{subdomain}.rest.vitally.io
  EU:  https://{subdomain}.rest.vitally-eu.io

Auth is HTTP Basic, with the API key as the username and an empty password
(generated in Vitally under Settings -> Integrations -> REST API). See:
  https://docs.vitally.io/en/articles/9880649-rest-api-overview
  https://docs.vitally.io/en/articles/9880654-rest-api-accounts
"""
import base64
import os
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class VitallyResource(dg.ConfigurableResource):
    """Vitally REST API client wrapper (HTTP Basic auth, API key as username)."""

    subdomain: str = Field(
        description="Your Vitally subdomain, from https://<subdomain>.vitally.io"
    )
    region: str = Field(
        default="us",
        description="Vitally region: 'us' or 'eu'. Determines the base URL host.",
    )
    api_key_env_var: str = Field(
        default="VITALLY_API_KEY",
        description="Env var holding the Vitally REST API key",
    )

    def _base_url(self) -> str:
        host = "rest.vitally-eu.io" if self.region == "eu" else "rest.vitally.io"
        return f"https://{self.subdomain}.{host}"

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> dict:
        """GET a Vitally REST resource (e.g. 'resources/accounts') and return parsed JSON."""
        import requests

        api_key = os.environ.get(self.api_key_env_var)
        if not api_key:
            raise RuntimeError(
                f"Missing Vitally API key: env var {self.api_key_env_var!r} is not set"
            )
        token = base64.b64encode(f"{api_key}:".encode()).decode()
        url = f"{self._base_url()}/{path.lstrip('/')}"
        resp = requests.get(
            url,
            params=params or {},
            headers={"Authorization": f"Basic {token}"},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class VitallyResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a `VitallyResource` (Vitally REST API client) for use by other components.

    Other components reference this resource by the value of `resource_key`.
    """

    resource_key: str = Field(
        default="vitally_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    subdomain: str = Field(
        description="Your Vitally subdomain, from https://<subdomain>.vitally.io"
    )
    region: str = Field(
        default="us",
        description="Vitally region: 'us' or 'eu'.",
    )
    api_key_env_var: str = Field(
        default="VITALLY_API_KEY",
        description="Env var holding the Vitally REST API key (Settings -> Integrations -> REST API)",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.region not in ("us", "eu"):
            raise ValueError(f"VitallyResourceComponent: region must be 'us' or 'eu', got {self.region!r}")
        resource = VitallyResource(
            subdomain=self.subdomain,
            region=self.region,
            api_key_env_var=self.api_key_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
