"""Totango Resource.

Wraps Totango's REST v2 Search API. Totango has two regional hosts:
  US: https://api.totango.com
  EU: https://api-eu1.totango.com

Auth uses a service token (Settings -> Integrations -> API Token), passed
as a non-standard Authorization header value: `app-token <token>` (not
`Bearer`, not Basic). Totango enforces a global rate limit of 100 calls/min
per token. See:
  https://support.totango.com/hc/en-us/articles/204174135-Search-API-accounts-and-users
"""
import os
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class TotangoResource(dg.ConfigurableResource):
    """Totango REST v2 API client wrapper (app-token auth)."""

    region: str = Field(
        default="us",
        description="Totango region: 'us' or 'eu'. Determines the base URL host.",
    )
    app_token_env_var: str = Field(
        default="TOTANGO_APP_TOKEN",
        description="Env var holding the Totango service token (Settings -> Integrations -> API Token)",
    )

    def _base_url(self) -> str:
        return "https://api-eu1.totango.com" if self.region == "eu" else "https://api.totango.com"

    def post(self, path: str, json: Optional[Dict[str, Any]] = None) -> dict:
        """POST to a Totango endpoint (e.g. 'api/v1/search/accounts') and return parsed JSON."""
        import requests

        token = os.environ.get(self.app_token_env_var)
        if not token:
            raise RuntimeError(
                f"Missing Totango app token: env var {self.app_token_env_var!r} is not set"
            )
        url = f"{self._base_url()}/{path.lstrip('/')}"
        resp = requests.post(
            url,
            json=json or {},
            headers={"Authorization": f"app-token {token}", "Content-Type": "application/json"},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class TotangoResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a `TotangoResource` (Totango REST v2 Search API client) for use by other components.

    Other components reference this resource by the value of `resource_key`.
    """

    resource_key: str = Field(
        default="totango_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    region: str = Field(
        default="us",
        description="Totango region: 'us' or 'eu'.",
    )
    app_token_env_var: str = Field(
        default="TOTANGO_APP_TOKEN",
        description="Env var holding the Totango service token",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.region not in ("us", "eu"):
            raise ValueError(f"TotangoResourceComponent: region must be 'us' or 'eu', got {self.region!r}")
        resource = TotangoResource(
            region=self.region,
            app_token_env_var=self.app_token_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
