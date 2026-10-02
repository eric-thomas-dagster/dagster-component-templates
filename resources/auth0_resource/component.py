"""Auth0 Resource component.

Wraps the Auth0 Management API using the OAuth2 **client_credentials**
grant -- the documented auth flow for server-side/machine-to-machine
access to the Management API:
https://auth0.com/docs/secure/tokens/access-tokens/management-api-access-tokens/get-management-api-access-tokens-for-production

Token endpoint: POST https://{tenant_domain}/oauth/token
  body: {client_id, client_secret, audience: "https://{tenant_domain}/api/v2/",
         grant_type: "client_credentials"}

The Auth0 Machine-to-Machine Application behind client_id/client_secret
must be authorized for the Management API with (at minimum) the
`read:users`, `create:users`, and `update:users` scopes. It does NOT
need `delete:users` -- this resource never requests or uses it.

This resource exposes ONLY the operations needed for safe profile sync:
  - find_user_by_email -- GET /api/v2/users-by-email
  - create_user        -- POST /api/v2/users
  - update_user        -- PATCH /api/v2/users/{id}  (profile attributes)
  - set_blocked        -- PATCH /api/v2/users/{id}  (ONLY {"blocked": ...})

There is deliberately NO delete_user method. Auth0's Management API does
expose `DELETE /api/v2/users/{id}` (a true, permanent, unrecoverable hard
delete), but this resource never calls it. See auth0_user_upsert's
README "Safety" section for the full reasoning -- the short version is
that this repo's reverse-ETL components must never be able to
permanently destroy a real identity.
"""
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class Auth0Resource(dg.ConfigurableResource):
    """Dagster resource wrapping the Auth0 Management API (client_credentials OAuth2)."""

    tenant_domain: str = Field(
        description="Your Auth0 tenant domain, e.g. 'mycompany.us.auth0.com' (no scheme, no trailing slash)."
    )
    client_id_env_var: str = Field(
        description=(
            "Env var holding the Auth0 Machine-to-Machine application's Client ID. "
            "That M2M app must be authorized for the Management API with scopes "
            "read:users, create:users, update:users (NOT delete:users -- this "
            "resource never uses it)."
        )
    )
    client_secret_env_var: str = Field(
        description="Env var holding the M2M application's Client Secret."
    )

    _token_cache: dict = {}

    def _get_access_token(self) -> str:
        # NOTE: must read/write via `self._token_cache` (the per-instance
        # pydantic PrivateAttr value), NOT `Auth0Resource._token_cache` (the
        # class attribute) -- the latter is a `ModelPrivateAttr` descriptor
        # object, not a dict, and `.get()`/`[]=` on it raises AttributeError.
        # (Same class-vs-instance footgun already found and fixed in
        # marketo_resource.)
        import os

        import requests

        cache_key = f"{self.tenant_domain}:{self.client_id_env_var}"
        cache = self._token_cache.get(cache_key) or {}
        if cache.get("expires", 0) > time.time() + 60:
            return cache["access_token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if not client_id or not client_secret:
            raise RuntimeError(
                f"Missing Auth0 M2M credentials: env vars {self.client_id_env_var!r} "
                f"and {self.client_secret_env_var!r} must both be set."
            )

        resp = requests.post(
            f"https://{self.tenant_domain}/oauth/token",
            json={
                "client_id": client_id,
                "client_secret": client_secret,
                "audience": f"https://{self.tenant_domain}/api/v2/",
                "grant_type": "client_credentials",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token_cache[cache_key] = {
            "access_token": data["access_token"],
            # Refresh a bit early rather than racing expiry.
            "expires": time.time() + data.get("expires_in", 86400) - 60,
        }
        return data["access_token"]

    def get_client(self):
        """Return an authenticated `requests.Session` (Bearer token). Escape
        hatch for anything not covered by the convenience methods below."""
        import requests

        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self._get_access_token()}",
                "Content-Type": "application/json",
            }
        )
        return session

    def find_user_by_email(self, email: str) -> List[Dict[str, Any]]:
        """GET /api/v2/users-by-email -- returns a list. Auth0 allows more
        than one user to share an email across different connections, so
        callers must handle the 0 / 1 / many cases explicitly rather than
        assuming a unique match."""
        session = self.get_client()
        resp = session.get(
            f"https://{self.tenant_domain}/api/v2/users-by-email",
            params={"email": email},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def create_user(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v2/users -- payload must include 'connection' and 'email'
        (plus a 'password' if the connection requires one)."""
        session = self.get_client()
        resp = session.post(
            f"https://{self.tenant_domain}/api/v2/users",
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def update_user(self, user_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /api/v2/users/{id} -- ordinary profile-attribute update.
        Callers must never put `blocked` in this payload -- use
        `set_blocked` instead, so the deactivation code path stays
        structurally distinct from routine profile sync."""
        session = self.get_client()
        resp = session.patch(
            f"https://{self.tenant_domain}/api/v2/users/{user_id}",
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def set_blocked(self, user_id: str, blocked: bool) -> Dict[str, Any]:
        """PATCH /api/v2/users/{id} with ONLY {'blocked': ...} -- Auth0's
        documented mechanism for blocking/unblocking a user:
        https://auth0.com/docs/manage-users/user-accounts/block-and-unblock-users
        This is the only method on this resource that can ever touch
        `blocked`, and this resource has no method that deletes a user."""
        session = self.get_client()
        resp = session.patch(
            f"https://{self.tenant_domain}/api/v2/users/{user_id}",
            json={"blocked": bool(blocked)},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()


class Auth0ResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Auth0Resource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.Auth0ResourceComponent
        attributes:
          resource_key: auth0_resource
          tenant_domain: "mycompany.us.auth0.com"
          client_id_env_var: AUTH0_CLIENT_ID
          client_secret_env_var: AUTH0_CLIENT_SECRET
        ```
    """

    resource_key: str = Field(
        default="auth0_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    tenant_domain: str = Field(
        description="Your Auth0 tenant domain, e.g. 'mycompany.us.auth0.com'."
    )
    client_id_env_var: str = Field(
        default="AUTH0_CLIENT_ID",
        description="Env var holding the Auth0 M2M application's Client ID.",
    )
    client_secret_env_var: str = Field(
        default="AUTH0_CLIENT_SECRET",
        description="Env var holding the M2M application's Client Secret.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = Auth0Resource(
            tenant_domain=self.tenant_domain,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
