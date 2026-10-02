"""Freshservice Resource component.

Freshservice is a DIFFERENT Freshworks product from Freshdesk -- an ITSM
tool (tickets, assets, changes) rather than a helpdesk/contacts tool. The
auth shape is identical to Freshdesk (HTTP Basic, API key as username,
literal "X" as password) but the API surface and methods below are
Freshservice-specific (tickets, not contacts).
"""
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class FreshserviceResource(ConfigurableResource):
    """Dagster resource for the Freshservice REST API (v2)."""

    domain: str = Field(description="Freshservice domain e.g. 'mycompany.freshservice.com' (full host)")
    api_key_env_var: str = Field(description="Env var holding Freshservice API key")

    def get_session(self):
        import requests

        api_key = os.environ.get(self.api_key_env_var)
        base_url = f"https://{self.domain}/api/v2"

        session = requests.Session()
        session.auth = (api_key, "X")
        session.headers.update({"Content-Type": "application/json"})
        session.base_url = base_url  # type: ignore[attr-defined]
        return session

    def create_ticket(self, fields: dict) -> dict:
        """POST /api/v2/tickets -- create a new ticket."""
        session = self.get_session()
        base_url = session.base_url  # type: ignore[attr-defined]
        resp = session.post(f"{base_url}/tickets", json=fields)
        resp.raise_for_status()
        return resp.json().get("ticket", {})

    def update_ticket(self, ticket_id, fields: dict) -> dict:
        """PUT /api/v2/tickets/{ticket_id} -- update an existing ticket."""
        session = self.get_session()
        base_url = session.base_url  # type: ignore[attr-defined]
        resp = session.put(f"{base_url}/tickets/{ticket_id}", json=fields)
        resp.raise_for_status()
        return resp.json().get("ticket", {})

    def filter_tickets(self, query: str) -> list:
        """GET /api/v2/tickets/filter -- the Filter Tickets API.

        `query` must be the bare `"field:'value'"` expression (already
        quoted); this method wraps it and lets requests handle URL-encoding
        via `params=` rather than double-encoding manually. Note: date-type
        custom fields cannot be filtered this way (a documented Freshservice
        limitation).
        """
        session = self.get_session()
        base_url = session.base_url  # type: ignore[attr-defined]
        resp = session.get(f"{base_url}/tickets/filter", params={"query": f'"{query}"'})
        resp.raise_for_status()
        return resp.json().get("tickets", [])


class FreshserviceResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a FreshserviceResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.FreshserviceResourceComponent
        attributes:
          resource_key: freshservice_resource
          domain: mycompany.freshservice.com
          api_key_env_var: FRESHSERVICE_API_KEY
        ```
    """

    resource_key: str = Field(
        default="freshservice_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    domain: str = Field(
        description="Freshservice domain e.g. 'mycompany.freshservice.com' (full host)",
    )
    api_key_env_var: str = Field(
        description="Env var holding Freshservice API key",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = FreshserviceResource(
            domain=self.domain,
            api_key_env_var=self.api_key_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
