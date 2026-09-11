"""Braze Resource component.

Register a ``BrazeResource`` for use by other components (typically
``dataframe_to_braze``). Holds the region-specific REST endpoint URL +
the env var name for the REST API key.

Braze REST endpoints are region-specific (`https://rest.iad-01.braze.com`,
`rest.iad-02`, `rest.fra-01`, ...). Look yours up in the Braze dashboard
under Settings → REST API Keys.

API keys are scoped per capability (`users.track`, `catalogs.<name>.update_items`,
etc.) — create keys with only the scopes your components need.

Usage:

```yaml
type: dagster_component_templates.BrazeResourceComponent
attributes:
  resource_key: braze
  api_key_env_var: BRAZE_API_KEY
  rest_endpoint: https://rest.iad-01.braze.com
```

Downstream components reference `resource_key: braze` to share auth
without embedding endpoint/api-key config on each sink.
"""
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class BrazeResource(dg.ConfigurableResource):
    """A Braze REST API workhorse — holds auth + endpoint, exposes a
    session-based ``post()`` helper for downstream sinks."""

    api_key_env_var: str = Field(
        default="BRAZE_API_KEY",
        description="Env var holding the Braze REST API key.",
    )
    rest_endpoint: str = Field(
        description=(
            "Region-specific Braze REST endpoint URL "
            "(e.g. https://rest.iad-01.braze.com)."
        ),
    )
    request_timeout_seconds: int = Field(
        default=30,
        description="Per-request HTTP timeout (seconds).",
    )

    def _token(self) -> str:
        import os
        token = os.environ.get(self.api_key_env_var)
        if not token:
            raise dg.Failure(
                f"env var {self.api_key_env_var!r} is empty or unset — set your Braze REST API key."
            )
        return token

    def _base_url(self) -> str:
        return self.rest_endpoint.rstrip("/")

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token()}",
            "Content-Type": "application/json",
        }

    def post(self, path: str, json_body: Any) -> Any:
        """POST ``json_body`` to ``{rest_endpoint}{path}``. Returns the
        parsed JSON response. Raises ``dg.Failure`` on non-2xx status
        with the response body attached for debugging."""
        import requests
        path = "/" + path.lstrip("/")
        url = self._base_url() + path
        resp = requests.post(
            url, json=json_body, headers=self._headers(),
            timeout=self.request_timeout_seconds,
        )
        if not (200 <= resp.status_code < 300):
            body = (resp.text or "")[:500]
            raise dg.Failure(
                f"Braze POST {path} failed: HTTP {resp.status_code} body={body}"
            )
        try:
            return resp.json()
        except Exception:
            return None


class BrazeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a BrazeResource under a resource key for use by downstream
    Braze components (``dataframe_to_braze``, custom Braze sinks, etc.)."""

    resource_key: str = Field(
        default="braze",
        description="Dagster resource key. Downstream components reference this.",
    )
    api_key_env_var: str = Field(
        default="BRAZE_API_KEY",
        description="Env var holding the Braze REST API key.",
    )
    rest_endpoint: str = Field(
        description="Region-specific Braze REST endpoint URL.",
    )
    request_timeout_seconds: int = Field(
        default=30,
        description="Per-request HTTP timeout (seconds).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = BrazeResource(
            api_key_env_var=self.api_key_env_var,
            rest_endpoint=self.rest_endpoint,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
