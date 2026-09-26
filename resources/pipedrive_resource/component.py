"""Pipedrive Resource component."""
from typing import Optional
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class PipedriveResource(ConfigurableResource):
    """Dagster resource for the Pipedrive API."""

    api_token_env_var: str = Field(description="Env var holding Pipedrive API token")
    company_domain: Optional[str] = Field(
        default=None,
        description="Company domain e.g. 'mycompany'",
    )

    def get_client(self):
        import requests

        api_token = dg.EnvVar(self.api_token_env_var)
        domain = self.company_domain or "api"
        base_url = f"https://{domain}.pipedrive.com/api/v1"

        session = requests.Session()
        session.params = {"api_token": api_token}  # type: ignore[assignment]
        session.headers.update({"Content-Type": "application/json"})
        # Store base_url as a convenience attribute
        session.base_url = base_url  # type: ignore[attr-defined]
        return session

    def upsert_person(self, key_field: str, key_value: str, fields: dict) -> dict:
        """Pipedrive has no native upsert: searches `/persons/search` by
        `key_field` (e.g. 'email' or a custom field key), then PUTs the
        match or POSTs a new person.
        """
        session = self.get_client()
        base_url = session.base_url  # type: ignore[attr-defined]
        search_resp = session.get(
            f"{base_url}/persons/search",
            params={"term": key_value, "fields": key_field, "exact_match": "true"},
        )
        search_resp.raise_for_status()
        items = (search_resp.json().get("data") or {}).get("items") or []
        if items:
            person_id = items[0]["item"]["id"]
            resp = session.put(f"{base_url}/persons/{person_id}", json=fields)
        else:
            resp = session.post(f"{base_url}/persons", json=fields)
        resp.raise_for_status()
        return resp.json()


class PipedriveResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PipedriveResource for use by other components."""

    resource_key: str = Field(
        default="pipedrive_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_token_env_var: str = Field(
        description="Env var holding Pipedrive API token",
    )
    company_domain: Optional[str] = Field(
        default=None,
        description="Company domain e.g. 'mycompany'",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = PipedriveResource(
            api_token_env_var=self.api_token_env_var,
            company_domain=self.company_domain,
        )
        return dg.Definitions(resources={self.resource_key: resource})
