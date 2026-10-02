"""Sigma Computing Resource.

Wraps Sigma's REST API (OpenAPI-based -- https://help.sigmacomputing.com/reference).

Auth: OAuth2 **client_credentials** grant against per-organization API
credentials (a Client ID + Client Secret pair), generated via Sigma's
Admin -> Developer access UI (or `POST /v2/credentials`). Verified against
Sigma's own docs:
  - https://help.sigmacomputing.com/reference/get-started-sigma-api
  - https://help.sigmacomputing.com/reference/generate-client-credentials
  - https://help.sigmacomputing.com/reference/post-token

Token endpoint: POST {base_url}/v2/auth/token
  Content-Type: application/x-www-form-urlencoded
  Body: grant_type=client_credentials, client_id=..., client_secret=...
Response: access_token, token_type, expires_in (access tokens are valid
~1 hour). There is no refresh_token in the client_credentials grant --
unlike Outreach/Salesloft, this resource simply re-requests a fresh token
once the cached one is near expiry, rather than rotating a refresh_token.

Base URL is cloud/region-specific -- Sigma hosts separate API hosts per
cloud provider + region (confirmed via "Get started with the Sigma REST
API"), e.g.:
  AWS-US (West, default): https://api.sigmacomputing.com
  AWS-US (East):           https://api.us-a.aws.sigmacomputing.com
  AWS-CA:                  https://api.ca.aws.sigmacomputing.com
  AWS-EU:                  https://api.eu.aws.sigmacomputing.com
  AWS-UK:                  https://api.uk.aws.sigmacomputing.com
  AWS-AU:                  https://api.au.aws.sigmacomputing.com
  Azure / GCP:             additional regional variants
`base_url` is therefore a required-ish field (defaulted to the common
AWS-US-West host) rather than a hardcoded constant -- always confirm your
org's actual API host in Sigma's Admin -> Account settings before relying
on the default.
"""
import os
import time
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class SigmaResource(dg.ConfigurableResource):
    """Sigma REST API client wrapper (OAuth2 client_credentials grant)."""

    client_id_env_var: str = Field(description="Env var holding the Sigma API Client ID")
    client_secret_env_var: str = Field(description="Env var holding the Sigma API Client Secret")
    base_url: str = Field(
        default="https://api.sigmacomputing.com",
        description=(
            "Sigma REST API base URL for your org's cloud/region. Defaults to "
            "the AWS-US-West host -- confirm your actual host in Sigma's "
            "Admin -> Account settings (AWS-US-East/CA/EU/UK/AU and Azure/GCP "
            "each have a distinct host)."
        ),
    )

    # Per-instance cache (see resources/marketo_resource for the pydantic
    # gotcha this mirrors: must be accessed via `self._token_cache`, never
    # `SigmaResource._token_cache` -- the latter is a ModelPrivateAttr
    # descriptor, not the dict).
    _token_cache: dict = {}

    def _get_access_token(self) -> str:
        import requests

        cache = self._token_cache.get(self.base_url) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["access_token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if not all([client_id, client_secret]):
            raise RuntimeError(
                "Missing Sigma OAuth env vars (client_id/client_secret)"
            )

        resp = requests.post(
            f"{self.base_url.rstrip('/')}/v2/auth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data["access_token"]
        self._token_cache[self.base_url] = {
            "access_token": token,
            # Refresh a little early (60s) rather than racing expiry.
            "expires": time.time() + data.get("expires_in", 3600) - 60,
        }
        return token

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET a path relative to base_url (or a full URL)."""
        import requests

        url = path if path.startswith("http") else f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        token = self._get_access_token()
        resp = requests.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, json_body: Optional[Dict[str, Any]] = None) -> dict:
        """POST a JSON body to a path relative to base_url (or a full URL).

        Used both for regular write endpoints and for Sigma's inbound
        **Webhook trigger** endpoint
        (`POST /v2/webhooks/{workbookId}/{sequenceId}`) that
        `sigma_input_table_upsert` posts rows to -- that endpoint takes the
        same Bearer-token auth as every other Sigma REST call, so no
        separate client is needed for it.
        """
        import requests

        url = path if path.startswith("http") else f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        token = self._get_access_token()
        resp = requests.post(
            url,
            json=json_body,
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json() if resp.content else {}


class SigmaResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SigmaResource for use by other components."""

    resource_key: str = Field(
        default="sigma_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    client_id_env_var: str = Field(
        default="SIGMA_CLIENT_ID",
        description="Env var holding the Sigma API Client ID",
    )
    client_secret_env_var: str = Field(
        default="SIGMA_CLIENT_SECRET",
        description="Env var holding the Sigma API Client Secret",
    )
    base_url: str = Field(
        default="https://api.sigmacomputing.com",
        description="Sigma REST API base URL for your org's cloud/region (see component docstring for the regional host table).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = SigmaResource(
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
