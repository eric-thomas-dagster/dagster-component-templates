"""Zendesk Resource component."""
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class ZendeskResource(ConfigurableResource):
    """Dagster resource wrapping the Zenpy client."""

    subdomain: str = Field(description="Zendesk subdomain e.g. 'mycompany'")
    email: str = Field(description="Zendesk agent email address")
    api_token: str = Field(description="Zendesk API token")

    def get_client(self):
        from zenpy import Zenpy
        return Zenpy(
            subdomain=self.subdomain,
            email=self.email,
            token=self.api_token,
        )

    def create_or_update_user(self, email: str, name: str, external_id: str = None, user_fields: dict = None):
        """Native Zendesk upsert: `POST /api/v2/users/create_or_update.json`.

        Matches on `email` (or `external_id` if the user already has one set)
        and creates or updates in a single call -- no search-then-write
        needed. `user_fields` sets custom field values (must already exist
        in the Zendesk admin schema).
        """
        from zenpy.lib.api_objects import User
        client = self.get_client()
        user = User(email=email, name=name, external_id=external_id, user_fields=user_fields or {})
        return client.users.create_or_update(user)

    def create_or_update_ticket_field_via_search(self, external_id: str, ticket_fields: dict):
        """Zendesk has no native ticket upsert. Searches by `external_id`
        first; updates the match if found, else creates a new ticket with
        that external_id set (so subsequent runs can find it).
        """
        from zenpy.lib.api_objects import Ticket
        client = self.get_client()
        matches = list(client.search(type="ticket", external_id=external_id))
        if matches:
            ticket = matches[0]
            for k, v in ticket_fields.items():
                setattr(ticket, k, v)
            return client.tickets.update(ticket)
        return client.tickets.create(Ticket(external_id=external_id, **ticket_fields))


class ZendeskResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ZendeskResource for use by other components."""

    resource_key: str = Field(
        default="zendesk_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    subdomain: str = Field(
        description="Zendesk subdomain e.g. 'mycompany'",
    )
    email: str = Field(
        description="Zendesk agent email address",
    )
    api_token_env_var: str = Field(
        description="Env var holding Zendesk API token",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = ZendeskResource(
            subdomain=self.subdomain,
            email=self.email,
            api_token=dg.EnvVar(self.api_token_env_var),
        )
        return dg.Definitions(resources={self.resource_key: resource})
