"""Greenhouse Resource component.

Harvest API key wrapper (HTTP Basic, key as username, blank password) with
an ergonomic write convenience method so Dagster assets can call
`context.resources.greenhouse.update_candidate(candidate_id, tags=[...])`
without touching HTTP.

Drop to `.get_client()` for anything not covered -- returns an
authenticated `requests.Session`.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class GreenhouseResource(ConfigurableResource):
    """Dagster resource wrapping the Greenhouse Harvest API."""

    api_key: str = Field(description="Greenhouse Harvest API key (HTTP Basic username, blank password).")
    on_behalf_of_user_id: str = Field(
        description=(
            "Greenhouse user ID to attribute write operations to. Greenhouse's Harvest "
            "API requires the On-Behalf-Of header on every POST/PATCH/PUT -- the named "
            "user's permissions are checked server-side, so this must be a real user ID "
            "with appropriate access, not an arbitrary value."
        ),
    )

    _BASE: str = "https://harvest.greenhouse.io/v1"

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch."""
        import requests
        session = requests.Session()
        session.auth = (self.api_key, "")
        session.headers.update({"Content-Type": "application/json", "On-Behalf-Of": str(self.on_behalf_of_user_id)})
        return session

    def update_candidate(
        self,
        candidate_id: str,
        tags: Optional[List[str]] = None,
        custom_fields: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """PATCH /v1/candidates/{id} -- update a candidate's tags and/or
        custom fields. `custom_fields` keys must be the custom field's
        internal name (not its display label) -- check your Greenhouse
        account's field configuration if updates silently no-op.
        """
        body: dict = {}
        if tags is not None:
            body["tags"] = tags
        if custom_fields:
            body["custom_fields"] = custom_fields
        if not body:
            raise ValueError("update_candidate requires tags and/or custom_fields.")
        session = self.get_client()
        resp = session.patch(f"{self._BASE}/candidates/{candidate_id}", json=body, timeout=60)
        resp.raise_for_status()
        return resp.json()


class GreenhouseResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a GreenhouseResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.GreenhouseResourceComponent
        attributes:
          resource_key: greenhouse
          api_key_env_var: GREENHOUSE_API_KEY
          on_behalf_of_user_id: "4223"
        ```
    """

    resource_key: str = Field(
        default="greenhouse",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        default="GREENHOUSE_API_KEY",
        description="Env var holding a Greenhouse Harvest API key.",
    )
    on_behalf_of_user_id: str = Field(
        description="Greenhouse user ID to attribute write operations to (required by the Harvest API's On-Behalf-Of header).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = GreenhouseResource(
            api_key=dg.EnvVar(self.api_key_env_var),
            on_behalf_of_user_id=self.on_behalf_of_user_id,
        )
        return dg.Definitions(resources={self.resource_key: resource})
