"""Okta Resource component.

Wraps the Okta Users API using OAuth 2.0 **client_credentials** against an
Okta API Service Integration (a Service/machine-to-machine app) -- NOT the
SSWS API token used by `okta_management_ingestion` (that component reads
org/users/groups data; this one writes). Okta's docs describe
client_credentials as the flow for minting tokens that carry first-party
Okta API scopes (e.g. `okta.users.manage`):
https://developer.okta.com/docs/guides/implement-oauth-for-okta-serviceapp/

Token endpoint (client_secret_basic): POST https://{org_url}/oauth2/v1/token
  headers: Authorization: Basic base64(client_id:client_secret)
  body: grant_type=client_credentials&scope=okta.users.manage

The Service app behind client_id/client_secret should be granted ONLY the
`okta.users.manage` scope (covers create/update/deactivate) -- it does not
need any scope that would allow a hard delete call, and this resource
never makes one regardless of scope.

This resource exposes ONLY the operations needed for safe profile sync:
  - find_user       -- GET /api/v1/users/{id|login|email}  (404 -> None)
  - create_user     -- POST /api/v1/users
  - update_user     -- POST /api/v1/users/{id}  (profile MERGE semantics)
  - deactivate_user -- POST /api/v1/users/{id}/lifecycle/deactivate

There is deliberately NO delete_user method. Okta's Users API does expose
`DELETE /api/v1/users/{id}` -- but ONLY on a user already in the
DEPROVISIONED (deactivated) status, and it is a genuine, unrecoverable
hard delete (Okta's own docs recommend against it for audit/compliance
reasons). This resource never calls it. See okta_user_upsert's README
"Safety" section for the full reasoning.
"""
import base64
import time
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class OktaResource(dg.ConfigurableResource):
    """Dagster resource wrapping the Okta Users API (client_credentials OAuth2)."""

    org_url: str = Field(
        description="Your Okta org URL, e.g. 'https://mycompany.okta.com' (include scheme)."
    )
    client_id_env_var: str = Field(
        description=(
            "Env var holding the Okta API Service Integration's Client ID. "
            "Grant it ONLY the okta.users.manage scope -- that covers every "
            "operation this resource performs."
        )
    )
    client_secret_env_var: str = Field(
        description="Env var holding the Service Integration's Client Secret."
    )
    scope: str = Field(
        default="okta.users.manage",
        description="Space-separated OAuth scope(s) requested. okta.users.manage covers create/update/deactivate.",
    )
    token_path: str = Field(
        default="/oauth2/v1/token",
        description=(
            "Token endpoint path, relative to org_url. The org authorization "
            "server ('/oauth2/v1/token') is what Okta documents for "
            "client_credentials access to first-party Okta API scopes; "
            "override only if your org requires a custom authorization server."
        ),
    )

    _token_cache: dict = {}

    def _get_access_token(self) -> str:
        # NOTE: must read/write via `self._token_cache` (the per-instance
        # pydantic PrivateAttr value), NOT `OktaResource._token_cache` (the
        # class attribute) -- the latter is a `ModelPrivateAttr` descriptor
        # object, not a dict, and `.get()`/`[]=` on it raises AttributeError.
        # (Same class-vs-instance footgun already found and fixed in
        # marketo_resource.)
        import os

        import requests

        cache_key = f"{self.org_url}:{self.client_id_env_var}"
        cache = self._token_cache.get(cache_key) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["access_token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if not client_id or not client_secret:
            raise RuntimeError(
                f"Missing Okta Service Integration credentials: env vars "
                f"{self.client_id_env_var!r} and {self.client_secret_env_var!r} "
                f"must both be set."
            )

        basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
        resp = requests.post(
            f"{self.org_url.rstrip('/')}{self.token_path}",
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            data={"grant_type": "client_credentials", "scope": self.scope},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token_cache[cache_key] = {
            "access_token": data["access_token"],
            "expires": time.time() + data.get("expires_in", 3600) - 60,
        }
        return data["access_token"]

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch for
        anything not covered by the convenience methods below."""
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

    def find_user(self, identifier: str) -> Optional[Dict[str, Any]]:
        """GET /api/v1/users/{id|login|email}. Okta's Get User endpoint
        accepts any of the three as the path segment. Returns None on 404
        (not found) rather than raising, so callers can treat 'no such
        user' as an ordinary branch, not an error."""
        session = self.get_client()
        resp = session.get(
            f"{self.org_url.rstrip('/')}/api/v1/users/{identifier}",
            timeout=30,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def create_user(
        self,
        profile: Dict[str, Any],
        credentials: Optional[Dict[str, Any]] = None,
        activate: bool = True,
    ) -> Dict[str, Any]:
        """POST /api/v1/users?activate=... Body: {'profile': {...}[, 'credentials': {...}]}."""
        session = self.get_client()
        body: Dict[str, Any] = {"profile": profile}
        if credentials:
            body["credentials"] = credentials
        resp = session.post(
            f"{self.org_url.rstrip('/')}/api/v1/users",
            params={"activate": str(bool(activate)).lower()},
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def update_user(self, user_id: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/users/{id} -- partial-update (MERGE) semantics: only
        the profile attributes supplied are changed; every other attribute
        already on the user is left alone. (Okta's PUT is a strict replace
        that deletes any attribute you don't specify -- deliberately never
        used here.)"""
        session = self.get_client()
        resp = session.post(
            f"{self.org_url.rstrip('/')}/api/v1/users/{user_id}",
            json={"profile": profile},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def deactivate_user(self, user_id: str, send_email: bool = False) -> None:
        """POST /api/v1/users/{id}/lifecycle/deactivate -- Okta's distinct
        lifecycle endpoint, structurally separate from create_user /
        update_user. There is no delete_user method on this resource at
        all -- see the module docstring."""
        session = self.get_client()
        resp = session.post(
            f"{self.org_url.rstrip('/')}/api/v1/users/{user_id}/lifecycle/deactivate",
            params={"sendEmail": str(bool(send_email)).lower()},
            timeout=30,
        )
        resp.raise_for_status()


class OktaResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an OktaResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.OktaResourceComponent
        attributes:
          resource_key: okta_resource
          org_url: "https://mycompany.okta.com"
          client_id_env_var: OKTA_CLIENT_ID
          client_secret_env_var: OKTA_CLIENT_SECRET
        ```
    """

    resource_key: str = Field(
        default="okta_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    org_url: str = Field(description="Your Okta org URL, e.g. 'https://mycompany.okta.com'.")
    client_id_env_var: str = Field(
        default="OKTA_CLIENT_ID",
        description="Env var holding the Okta API Service Integration's Client ID.",
    )
    client_secret_env_var: str = Field(
        default="OKTA_CLIENT_SECRET",
        description="Env var holding the Service Integration's Client Secret.",
    )
    scope: str = Field(
        default="okta.users.manage",
        description="Space-separated OAuth scope(s) requested.",
    )
    token_path: str = Field(
        default="/oauth2/v1/token",
        description="Token endpoint path, relative to org_url.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = OktaResource(
            org_url=self.org_url,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            scope=self.scope,
            token_path=self.token_path,
        )
        return dg.Definitions(resources={self.resource_key: resource})
