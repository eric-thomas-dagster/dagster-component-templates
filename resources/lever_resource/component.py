"""Lever Resource component.

HTTP Basic auth wrapper over the Lever Hire REST API (`https://api.lever.co/v1`)
with convenience methods for the opportunity (candidate) lifecycle. Basic auth
uses the Lever API key as the username and a BLANK password -- this matches
the convention already established in `lever_ingestion` elsewhere in this repo.

IMPORTANT -- `perform_as`: every mutating Lever API call (create, tag, stage
change, archive) requires a `perform_as` query param identifying the acting
Lever user by their Lever user ID. This is a hard requirement of Lever's real
API, not an optional nicety, so it is a REQUIRED field on this resource (not
an env var -- it's a user ID, not a secret). Look up a user ID once via
Lever's `GET /users` endpoint (e.g. filter by email) and hardcode it in your
component config.

Drop to `.get_client()` for anything not covered -- returns an authenticated
`requests.Session`.
"""
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class LeverResource(ConfigurableResource):
    """Dagster resource wrapping the Lever Hire REST API with convenience methods.

    Covers opportunity search-by-email, creation, tagging (additive only --
    Lever has no "replace all tags" call), stage changes, and archiving. For
    anything not covered, use `.get_client()` to get an authenticated
    `requests.Session`.
    """

    api_key_env_var: str = Field(description="Env var holding the Lever API key (Basic auth username).")
    perform_as: str = Field(
        description=(
            "Lever user ID of the acting user, required by Lever on every "
            "mutating call. Look this up once via GET /users and hardcode it "
            "here -- it is NOT optional on Lever's real API."
        ),
    )
    base_url: str = Field(default="https://api.lever.co/v1", description="Lever API base URL.")

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch.

        Lever Basic auth uses the API key as username and a BLANK password.
        """
        import os
        import requests

        api_key = os.environ.get(self.api_key_env_var, "")
        session = requests.Session()
        session.auth = (api_key, "")
        session.headers.update({"Content-Type": "application/json"})
        return session

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}{path}"

    # ------------------------------------------------------------------ reads

    def search_opportunities_by_email(self, email: str) -> list:
        """Search opportunities by exact-match email (Lever's documented `email` filter).

        `GET /opportunities?email={email}`. Response shape is
        `{"data": [...], "hasNext": bool, ...}` -- returns the `data` list
        (empty list if no match).
        """
        resp = self.get_client().get(self._url("/opportunities"), params={"email": email}, timeout=60)
        resp.raise_for_status()
        return resp.json().get("data", [])

    # ----------------------------------------------------------------- writes

    def create_opportunity(self, body: dict, posting_id: str | None = None) -> dict:
        """Create a new opportunity (candidate).

        `POST /opportunities?perform_as={perform_as}` (+ `&posting={posting_id}`
        if given). `body` is the JSON payload, e.g.
        `{"name": ..., "emails": [...], "phones": [...], "headline": ...,
        "tags": [...], "sources": [...], "stage": ...}`. Response shape is
        `{"data": {...}}` -- returns the inner `data` dict.
        """
        params = {"perform_as": self.perform_as}
        if posting_id:
            params["posting"] = posting_id
        resp = self.get_client().post(self._url("/opportunities"), params=params, json=body, timeout=60)
        resp.raise_for_status()
        return resp.json()["data"]

    def add_tags(self, opportunity_id: str, tags: list) -> dict:
        """Add tags to an opportunity. ADDITIVE ONLY.

        `POST /opportunities/{id}/addTags?perform_as={perform_as}` body
        `{"tags": tags}`. Lever has no "replace all tags" call -- this can
        only add tags, never remove or replace existing ones. This is a real
        limitation of Lever's API, not a bug in this wrapper.
        """
        resp = self.get_client().post(
            self._url(f"/opportunities/{opportunity_id}/addTags"),
            params={"perform_as": self.perform_as},
            json={"tags": tags},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def update_stage(self, opportunity_id: str, stage_id: str) -> dict:
        """Move an opportunity to a new pipeline stage.

        `PUT /opportunities/{id}/stage?perform_as={perform_as}` body `{"stage": stage_id}`.
        """
        resp = self.get_client().put(
            self._url(f"/opportunities/{opportunity_id}/stage"),
            params={"perform_as": self.perform_as},
            json={"stage": stage_id},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def archive_opportunity(self, opportunity_id: str, reason: str) -> dict:
        """Archive an opportunity with a reason.

        `PUT /opportunities/{id}/archived?perform_as={perform_as}` body `{"reason": reason}`.
        """
        resp = self.get_client().put(
            self._url(f"/opportunities/{opportunity_id}/archived"),
            params={"perform_as": self.perform_as},
            json={"reason": reason},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class LeverResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a LeverResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.LeverResourceComponent
        attributes:
          resource_key: lever_resource
          api_key_env_var: LEVER_API_KEY
          perform_as: "abc123-lever-user-id"
        ```
    """

    resource_key: str = Field(
        default="lever_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(description="Env var holding the Lever API key (Basic auth username).")
    perform_as: str = Field(
        description=(
            "Lever user ID of the acting user, required by Lever on every "
            "mutating call. Look this up once via GET /users."
        ),
    )
    base_url: str = Field(default="https://api.lever.co/v1", description="Lever API base URL.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = LeverResource(
            api_key_env_var=self.api_key_env_var,
            perform_as=self.perform_as,
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
