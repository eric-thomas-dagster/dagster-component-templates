"""Basecamp Resource.

Wraps the Basecamp 3/4 REST API, which has an unusual URL + auth shape
compared to a typical multi-tenant SaaS REST API:

  - Auth is OAuth2 (3-legged authorization-code grant via
    https://launchpad.37signals.com/authorization/new) -- there is no
    simple API-key option. The access token is obtained ONCE, outside
    this resource (e.g. via a short setup script or the OAuth playground),
    and passed in as a plain Bearer token here. Access tokens expire
    (~2 weeks); refreshing is out of scope for this resource -- same
    convention already established by `basecamp_ingestion` in this repo.
  - There is no single global API host. EVERY request is scoped under a
    per-account path: `https://3.basecampapi.com/<account_id>/...`. The
    account_id is NOT returned by the OAuth token exchange itself -- it
    comes from a separate call to
    `https://launchpad.37signals.com/authorization.json` (done once,
    outside this resource, same place the access token is minted).
  - Basecamp's docs ask every integration to send a descriptive
    `User-Agent` header identifying the app + a contact method -- this is
    a courtesy convention (helps 37signals reach you about API issues),
    not a hard requirement enforced by the API.
"""
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class BasecampResource(dg.ConfigurableResource):
    """Basecamp 3/4 REST API client wrapper (per-account URL + OAuth2 Bearer)."""

    account_id: str = Field(
        description=(
            "Basecamp account ID (the numeric segment right after the host in "
            "any Basecamp URL, e.g. 'https://3.basecamp.com/<account_id>/...'). "
            "Obtained once via https://launchpad.37signals.com/authorization.json "
            "post-OAuth -- it is NOT part of the token response itself."
        ),
    )
    access_token_env_var: str = Field(
        description="Env var holding a Basecamp OAuth2 access token (3-legged authorization-code grant)."
    )
    user_agent: str = Field(
        default="Dagster Community Components (https://github.com/dagster-io)",
        description="User-Agent header value. Basecamp asks integrations to identify themselves + a contact method.",
    )

    def request(self, method: str, path: str, params: Optional[dict] = None, json_body: Optional[dict] = None) -> dict:
        """Issue one HTTP request. `path` is relative to
        `https://3.basecampapi.com/<account_id>/`."""
        import requests

        access_token = os.environ.get(self.access_token_env_var)
        if not access_token:
            raise RuntimeError(f"Missing Basecamp access token env var: {self.access_token_env_var}")

        url = f"https://3.basecampapi.com/{self.account_id}/{path.lstrip('/')}"
        resp = requests.request(
            method,
            url,
            params=params,
            json=json_body,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "User-Agent": self.user_agent,
            },
            timeout=60,
        )
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()


class BasecampResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a BasecampResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.BasecampResourceComponent
        attributes:
          resource_key: basecamp_resource
          account_id: "195539477"
          access_token_env_var: BASECAMP_ACCESS_TOKEN
        ```
    """

    resource_key: str = Field(
        default="basecamp_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    account_id: str = Field(description="Basecamp account ID.")
    access_token_env_var: str = Field(
        default="BASECAMP_ACCESS_TOKEN",
        description="Env var holding a Basecamp OAuth2 access token.",
    )
    user_agent: str = Field(
        default="Dagster Community Components (https://github.com/dagster-io)",
        description="User-Agent header value sent on every request.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = BasecampResource(
            account_id=self.account_id,
            access_token_env_var=self.access_token_env_var,
            user_agent=self.user_agent,
        )
        return dg.Definitions(resources={self.resource_key: resource})
