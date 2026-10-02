"""Help Scout Resource component.

OAuth2 `client_credentials` wrapper over the Help Scout Mailbox API v2
(`https://api.helpscout.net/v2`) with convenience methods for the
conversation lifecycle.

IMPORTANT -- auth is DELIBERATELY different from `help_scout_ingestion`
elsewhere in this repo. The ingestion component takes a static, pre-minted
`access_token` field and uses it directly as a Bearer token. This resource
instead implements Help Scout's real documented server-to-server auth flow:
the OAuth2 `client_credentials` grant (`POST /v2/oauth2/token` with
`grant_type=client_credentials` + `client_id` + `client_secret`). That grant
has NO refresh_token -- the returned `access_token` is simply valid for
~48 hours (`expires_in: 172800`), and when it expires you re-POST the same
client_id/client_secret for a fresh one. This resource caches the token
in-memory (keyed by `client_id_env_var`, mirroring the caching shape used by
`resources/outreach_resource` for its own, different OAuth flow) so repeated
calls within one process reuse the token instead of re-authenticating on
every request.

Drop to `.get_client()` for anything not covered -- returns an authenticated
`requests.Session`.
"""
import time
from typing import List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class HelpScoutResource(ConfigurableResource):
    """Dagster resource wrapping the Help Scout Mailbox API v2 with convenience methods.

    Covers conversation creation (which natively upserts the underlying
    Customer record by email -- see `create_conversation`), tag replacement,
    note threads, and status patches. For anything not covered, use
    `.get_client()` to get an authenticated `requests.Session`.
    """

    client_id_env_var: str = Field(description="Env var holding the Help Scout OAuth2 Client ID.")
    client_secret_env_var: str = Field(description="Env var holding the Help Scout OAuth2 Client Secret.")
    base_url: str = Field(default="https://api.helpscout.net/v2", description="Help Scout API base URL.")
    token_url: str = Field(
        default="https://api.helpscout.net/v2/oauth2/token",
        description="Help Scout OAuth2 token endpoint.",
    )

    _token_cache: dict = {}

    def _get_access_token(self) -> str:
        """Fetch (or reuse a cached) OAuth2 access token via the
        `client_credentials` grant. No refresh_token is involved in this
        grant type -- on expiry we simply re-authenticate with the same
        client_id/client_secret.
        """
        import os
        import requests

        cache_key = self.client_id_env_var
        cached = HelpScoutResource._token_cache.get(cache_key) or {}
        if cached.get("expires", 0) > time.time() + 60:
            return cached["access_token"]

        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if not client_id or not client_secret:
            raise RuntimeError(
                "Missing Help Scout OAuth env vars "
                f"({self.client_id_env_var}/{self.client_secret_env_var})"
            )

        resp = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        HelpScoutResource._token_cache[cache_key] = {
            "access_token": data["access_token"],
            # Refresh a little early (60s) rather than racing expiry.
            "expires": time.time() + data.get("expires_in", 172800) - 60,
        }
        return data["access_token"]

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch."""
        import requests

        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        })
        return session

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}{path}"

    # ----------------------------------------------------------------- writes

    def create_conversation(
        self,
        mailbox_id: int,
        customer_email: str,
        subject: str,
        body_text: str,
        type_: str = "email",
        status: str = "active",
        tags: Optional[List[str]] = None,
        customer_first_name: Optional[str] = None,
        customer_last_name: Optional[str] = None,
    ) -> int:
        """Create a new conversation. `POST /conversations`.

        Native Help Scout behavior: if `customer_email` doesn't match an
        existing Customer, Help Scout auto-creates one -- this IS the
        documented customer-upsert-by-email mechanism, no separate
        customer-create call is needed.

        A successful create returns HTTP 201 with NO response body -- the
        new conversation's ID comes back in the `Resource-ID` response
        header. Raises `RuntimeError` if that header is unexpectedly absent.
        """
        customer: dict = {"email": customer_email}
        if customer_first_name:
            customer["firstName"] = customer_first_name
        if customer_last_name:
            customer["lastName"] = customer_last_name

        body: dict = {
            "subject": subject,
            "customer": customer,
            "mailboxId": mailbox_id,
            "type": type_,
            "status": status,
            "threads": [
                {"type": "customer", "customer": customer, "text": body_text}
            ],
        }
        if tags:
            body["tags"] = tags

        resp = self.get_client().post(self._url("/conversations"), json=body, timeout=60)
        resp.raise_for_status()
        resource_id = resp.headers.get("Resource-ID")
        if not resource_id:
            raise RuntimeError(
                "Help Scout create_conversation succeeded but no 'Resource-ID' "
                f"response header was present (status={resp.status_code}, "
                f"headers={dict(resp.headers)})"
            )
        return int(resource_id)

    def update_tags(self, conversation_id: int, tags: List[str]) -> None:
        """Replace a conversation's ENTIRE tag list. `PUT /conversations/{id}/tags`.

        NOT additive -- any existing tag not included in `tags` is removed.
        """
        resp = self.get_client().put(
            self._url(f"/conversations/{conversation_id}/tags"),
            json={"tags": tags},
            timeout=60,
        )
        resp.raise_for_status()

    def add_note(self, conversation_id: int, text: str) -> None:
        """Add an internal note thread to an existing conversation.

        `POST /conversations/{id}/notes` body `{"text": text}`.
        """
        resp = self.get_client().post(
            self._url(f"/conversations/{conversation_id}/notes"),
            json={"text": text},
            timeout=60,
        )
        resp.raise_for_status()

    def patch_status(self, conversation_id: int, status: str) -> None:
        """Update a conversation's status via Help Scout's JSON-Patch endpoint.

        `PATCH /conversations/{id}` body is a JSON-Patch ARRAY (not a flat
        object): `[{"op": "replace", "path": "/status", "value": status}]`.
        """
        resp = self.get_client().patch(
            self._url(f"/conversations/{conversation_id}"),
            json=[{"op": "replace", "path": "/status", "value": status}],
            timeout=60,
        )
        resp.raise_for_status()


class HelpScoutResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a HelpScoutResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.HelpScoutResourceComponent
        attributes:
          resource_key: help_scout_resource
          client_id_env_var: HELPSCOUT_CLIENT_ID
          client_secret_env_var: HELPSCOUT_CLIENT_SECRET
        ```
    """

    resource_key: str = Field(
        default="help_scout_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    client_id_env_var: str = Field(
        default="HELPSCOUT_CLIENT_ID",
        description="Env var holding the Help Scout OAuth2 Client ID.",
    )
    client_secret_env_var: str = Field(
        default="HELPSCOUT_CLIENT_SECRET",
        description="Env var holding the Help Scout OAuth2 Client Secret.",
    )
    base_url: str = Field(default="https://api.helpscout.net/v2", description="Help Scout API base URL.")
    token_url: str = Field(
        default="https://api.helpscout.net/v2/oauth2/token",
        description="Help Scout OAuth2 token endpoint.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = HelpScoutResource(
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            base_url=self.base_url,
            token_url=self.token_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
