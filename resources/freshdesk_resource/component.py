"""Freshdesk Resource component."""
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class FreshdeskResource(ConfigurableResource):
    """Dagster resource for the Freshdesk REST API."""

    domain: str = Field(description="Freshdesk domain e.g. 'mycompany.freshdesk.com'")
    api_key_env_var: str = Field(description="Env var holding Freshdesk API key")

    def get_session(self):
        import requests

        api_key = dg.EnvVar(self.api_key_env_var)
        base_url = f"https://{self.domain}/api/v2"

        session = requests.Session()
        session.auth = (api_key, "X")
        session.headers.update({"Content-Type": "application/json"})
        session.base_url = base_url  # type: ignore[attr-defined]
        return session

    def upsert_contact(self, unique_external_id: str, fields: dict) -> dict:
        """Freshdesk has no single-call native upsert: lists `/contacts`
        filtered by `unique_external_id` (a real, documented filter param),
        then PUTs the match or POSTs a new contact with that external_id set.
        """
        session = self.get_session()
        base_url = session.base_url  # type: ignore[attr-defined]
        search_resp = session.get(f"{base_url}/contacts", params={"unique_external_id": unique_external_id})
        search_resp.raise_for_status()
        matches = search_resp.json() or []
        body = dict(fields)
        body["unique_external_id"] = unique_external_id
        if matches:
            resp = session.put(f"{base_url}/contacts/{matches[0]['id']}", json=body)
        else:
            resp = session.post(f"{base_url}/contacts", json=body)
        resp.raise_for_status()
        return resp.json()


class FreshdeskResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a FreshdeskResource for use by other components."""

    resource_key: str = Field(
        default="freshdesk_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    domain: str = Field(
        description="Freshdesk domain e.g. 'mycompany.freshdesk.com'",
    )
    api_key_env_var: str = Field(
        description="Env var holding Freshdesk API key",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = FreshdeskResource(
            domain=self.domain,
            api_key_env_var=self.api_key_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
