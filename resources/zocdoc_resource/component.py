"""Zocdoc Resource component.

Wraps Zocdoc's Developer Platform API using the OAuth2 **client_credentials**
grant -- the documented machine-to-machine flow for a backend service with
no end-user login involved:
https://api-docs.zocdoc.com/guides/authentication

Token endpoint:
  POST https://auth.zocdoc.com/oauth/token                               (production)
  POST https://auth-api-developer-sandbox.zocdoc.com/oauth/token         (sandbox)
    body: {client_id, client_secret, grant_type: "client_credentials",
           audience: "https://api-developer.zocdoc.com/"}                (production)
           audience: "https://api-developer-sandbox.zocdoc.com/"         (sandbox)

Base API URLs:
  https://api-developer.zocdoc.com/            (production)
  https://api-developer-sandbox.zocdoc.com/     (sandbox)

Access tokens expire after 60 minutes; this resource caches and refetches
a bit early rather than racing expiry (same pattern as auth0_resource).

IMPORTANT -- Zocdoc's API is partner-gated. A Client ID/Secret pair is
useless until Zocdoc has approved your integration; there is no self-serve
signup. See https://api-docs.zocdoc.com/guides/faqs.

PHI note: Zocdoc's API surfaces Protected Health Information (patient
names, appointment details, provider schedules). This resource itself
never logs or returns anything beyond the bearer token and an
authenticated `requests.Session` -- it has no knowledge of what any
downstream component does with the response bodies it fetches. The PHI
safety guarantees (no data preview in metadata/logs/errors) live in the
two components that actually touch patient/appointment data:
`zocdoc_appointments_ingestion` and `zocdoc_availability_upsert`.
"""
import time
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field

_TOKEN_URLS = {
    "production": "https://auth.zocdoc.com/oauth/token",
    "sandbox": "https://auth-api-developer-sandbox.zocdoc.com/oauth/token",
}
_BASE_URLS = {
    "production": "https://api-developer.zocdoc.com/",
    "sandbox": "https://api-developer-sandbox.zocdoc.com/",
}


def _fetch_zocdoc_access_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    audience: str,
    scope: Optional[str],
) -> Dict[str, Any]:
    """Isolates the one external API call this resource makes (the OAuth2
    client_credentials token request) so it can be monkeypatched wholesale
    in tests. Returns the raw token response dict ({'access_token',
    'expires_in', ...}) -- callers are responsible for caching.
    """
    import requests

    body: Dict[str, Any] = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "audience": audience,
    }
    if scope:
        body["scope"] = scope

    resp = requests.post(token_url, json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


class ZocdocResource(dg.ConfigurableResource):
    """Dagster resource wrapping Zocdoc's Developer Platform API (OAuth2
    client_credentials)."""

    environment: str = Field(
        description=(
            "Which Zocdoc environment to call: 'sandbox' or 'production'. "
            "No default -- an environment must be chosen deliberately given "
            "production carries real patient-facing data (PHI)."
        )
    )
    client_id_env_var: str = Field(
        default="ZOCDOC_CLIENT_ID",
        description="Env var holding the Zocdoc OAuth2 Client ID for your partner-gated application.",
    )
    client_secret_env_var: str = Field(
        default="ZOCDOC_CLIENT_SECRET",
        description="Env var holding the Zocdoc OAuth2 Client Secret.",
    )
    scope: Optional[str] = Field(
        default=None,
        description="Optional OAuth2 scope to request (e.g. 'offline_access' for a refresh token). Most client_credentials integrations leave this unset -- scopes are fixed per the partner application's Zocdoc-side configuration.",
    )

    _token_cache: dict = {}

    def _urls(self) -> Dict[str, str]:
        env = self.environment
        if env not in _TOKEN_URLS:
            raise ValueError(
                f"ZocdocResource: environment must be 'sandbox' or 'production', got {env!r}."
            )
        return {"token_url": _TOKEN_URLS[env], "base_url": _BASE_URLS[env]}

    def _get_access_token(self) -> str:
        # NOTE: must read/write via `self._token_cache` (the per-instance
        # pydantic PrivateAttr value), NOT `ZocdocResource._token_cache`
        # (the class attribute descriptor) -- same class-vs-instance
        # footgun already found and fixed in marketo_resource/auth0_resource.
        import os

        urls = self._urls()
        cache_key = f"{self.environment}:{self.client_id_env_var}"
        cache = self._token_cache.get(cache_key) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["access_token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if not client_id or not client_secret:
            raise RuntimeError(
                f"Missing Zocdoc OAuth2 credentials: env vars "
                f"{self.client_id_env_var!r} and {self.client_secret_env_var!r} "
                f"must both be set."
            )

        data = _fetch_zocdoc_access_token(
            token_url=urls["token_url"],
            client_id=client_id,
            client_secret=client_secret,
            audience=urls["base_url"],
            scope=self.scope,
        )
        self._token_cache[cache_key] = {
            "access_token": data["access_token"],
            # Access tokens expire after 60 minutes per Zocdoc's docs;
            # refresh a bit early rather than racing expiry.
            "expires": time.time() + data.get("expires_in", 3600) - 60,
        }
        return data["access_token"]

    def get_client(self):
        """Return an authenticated `requests.Session` (Bearer token)."""
        import requests

        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self._get_access_token()}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        return session

    def get_base_url(self) -> str:
        """Base URL for the configured environment, e.g.
        'https://api-developer.zocdoc.com/'."""
        return self._urls()["base_url"]


class ZocdocResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ZocdocResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.ZocdocResourceComponent
        attributes:
          resource_key: zocdoc_resource
          environment: sandbox
          client_id_env_var: ZOCDOC_CLIENT_ID
          client_secret_env_var: ZOCDOC_CLIENT_SECRET
        ```

    Zocdoc's API is partner-gated -- apply through Zocdoc first; a Client
    ID/Secret pair does nothing until your integration is approved.
    """

    resource_key: str = Field(
        default="zocdoc_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    environment: str = Field(
        description="'sandbox' or 'production'. No default -- must be chosen deliberately.",
    )
    client_id_env_var: str = Field(
        default="ZOCDOC_CLIENT_ID",
        description="Env var holding the Zocdoc OAuth2 Client ID.",
    )
    client_secret_env_var: str = Field(
        default="ZOCDOC_CLIENT_SECRET",
        description="Env var holding the Zocdoc OAuth2 Client Secret.",
    )
    scope: Optional[str] = Field(
        default=None,
        description="Optional OAuth2 scope to request (e.g. 'offline_access').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.environment not in _TOKEN_URLS:
            raise ValueError(
                f"ZocdocResourceComponent: environment must be 'sandbox' or "
                f"'production', got {self.environment!r}."
            )
        resource = ZocdocResource(
            environment=self.environment,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            scope=self.scope,
        )
        return dg.Definitions(resources={self.resource_key: resource})
