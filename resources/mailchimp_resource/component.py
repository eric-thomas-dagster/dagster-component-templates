"""Mailchimp Resource component.

API key wrapper over the Mailchimp Marketing API with an ergonomic write
convenience method so Dagster assets can call
`context.resources.mailchimp.upsert_member(list_id, email, merge_fields=...)`
without touching HTTP.

Mailchimp has a **native upsert endpoint** for list members --
`PUT /lists/{list_id}/members/{subscriber_hash}` (subscriber_hash is the
lowercased, MD5-hashed email) does server-side create-or-update.

Drop to `.get_client()` for anything not covered -- returns an
authenticated `requests.Session`.
"""
import hashlib
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class MailchimpResource(ConfigurableResource):
    """Dagster resource wrapping the Mailchimp Marketing API.

    Auth is an API key -- the datacenter suffix (e.g. 'us19') is parsed
    from it automatically, matching Mailchimp's own convention.
    """

    api_key: str = Field(description="Mailchimp API key (includes the datacenter suffix, e.g. 'abc123-us19').")

    def _datacenter(self) -> str:
        if "-" not in self.api_key:
            raise ValueError("Mailchimp api_key must include the datacenter suffix, e.g. 'abc123-us19'.")
        return self.api_key.rsplit("-", 1)[-1]

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch."""
        import requests
        session = requests.Session()
        session.auth = ("anystring", self.api_key)
        session.headers.update({"Content-Type": "application/json"})
        return session

    def _url(self, path: str) -> str:
        return f"https://{self._datacenter()}.api.mailchimp.com/3.0{path}"

    def upsert_member(
        self,
        list_id: str,
        email: str,
        merge_fields: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        status_if_new: str = "subscribed",
    ) -> dict:
        """Native upsert: `PUT /lists/{list_id}/members/{subscriber_hash}`.

        Creates the member with `status_if_new` if they don't exist yet,
        or updates their merge_fields if they do (status is left alone
        on update -- Mailchimp does not let you silently resubscribe
        someone via this endpoint).

        `tags` (if set) are applied via a separate call to the member's
        tags sub-resource, since PUT on the member itself does not accept
        a tags array directly.
        """
        subscriber_hash = hashlib.md5(email.strip().lower().encode("utf-8")).hexdigest()
        body: dict = {
            "email_address": email,
            "status_if_new": status_if_new,
        }
        if merge_fields:
            body["merge_fields"] = merge_fields
        session = self.get_client()
        resp = session.put(self._url(f"/lists/{list_id}/members/{subscriber_hash}"), json=body, timeout=60)
        resp.raise_for_status()
        member = resp.json()

        if tags:
            tags_resp = session.post(
                self._url(f"/lists/{list_id}/members/{subscriber_hash}/tags"),
                json={"tags": [{"name": t, "status": "active"} for t in tags]},
                timeout=60,
            )
            tags_resp.raise_for_status()

        return member


class MailchimpResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a MailchimpResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.MailchimpResourceComponent
        attributes:
          resource_key: mailchimp
          api_key_env_var: MAILCHIMP_API_KEY
        ```
    """

    resource_key: str = Field(
        default="mailchimp",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        default="MAILCHIMP_API_KEY",
        description="Env var holding a Mailchimp API key.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = MailchimpResource(api_key=dg.EnvVar(self.api_key_env_var))
        return dg.Definitions(resources={self.resource_key: resource})
