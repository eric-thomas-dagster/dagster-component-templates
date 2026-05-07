"""Dynatrace Resource.

Wraps Dynatrace's Environment API v2 + Metrics API v2. Dynatrace tenants
have URLs like `https://<tenant-id>.live.dynatrace.com` (SaaS) or
`https://<managed-server>/e/<env-id>` (Managed). Auth via API token
with the appropriate scopes.

Companion components:
- `dataframe_to_dynatrace_events` (sink) — push generic events
- `dynatrace_metrics_query` (source) — query metrics → DataFrame
"""

import os
from typing import Optional
import dagster as dg
from pydantic import Field


class DynatraceResource(dg.ConfigurableResource):
    """Dynatrace API client wrapper."""

    environment_url: str = Field(
        description=(
            "Dynatrace environment URL with no trailing slash. SaaS: "
            "'https://abc12345.live.dynatrace.com'. Managed: "
            "'https://my-server/e/<env-id>'."
        )
    )
    api_token_env_var: str = Field(description="Env var holding the API token")

    def _token(self) -> str:
        token = os.environ.get(self.api_token_env_var)
        if not token:
            raise RuntimeError(f"Missing {self.api_token_env_var} environment variable")
        return token

    @property
    def api_v2_base(self) -> str:
        return f"{self.environment_url.rstrip('/')}/api/v2"

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET <api/v2>/<path> with token auth."""
        import requests
        url = f"{self.api_v2_base}/{path.lstrip('/')}"
        resp = requests.get(
            url,
            params=params,
            headers={"Authorization": f"Api-Token {self._token()}"},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, body: dict) -> dict:
        """POST <api/v2>/<path> with JSON body."""
        import requests
        url = f"{self.api_v2_base}/{path.lstrip('/')}"
        resp = requests.post(
            url,
            json=body,
            headers={
                "Authorization": f"Api-Token {self._token()}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json() if resp.text else {}


class DynatraceResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Dynatrace resource."""

    resource_key: str = Field(default="dynatrace", description="Dagster resource key")
    environment_url: str = Field(description="Dynatrace environment URL")
    api_token_env_var: str = Field(description="Env var holding the API token")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: DynatraceResource(
            environment_url=self.environment_url,
            api_token_env_var=self.api_token_env_var,
        )})
