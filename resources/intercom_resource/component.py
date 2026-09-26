"""Intercom Resource.

Wraps Intercom's REST API for support data ingestion: contacts,
conversations, tags, custom data attributes. Auth: API key (Bearer).
"""
import os
from typing import Optional
import dagster as dg
from pydantic import Field


class IntercomResource(dg.ConfigurableResource):
    """Intercom REST API client."""

    api_token_env_var: str = Field(description="Env var with Intercom API token")
    base_url: str = Field(default="https://api.intercom.io", description="API base URL")

    def _headers(self) -> dict:
        token = os.environ.get(self.api_token_env_var)
        if not token:
            raise RuntimeError(f"Missing {self.api_token_env_var}")
        return {
            "Authorization": f"Bearer {token}",
            "Intercom-Version": "2.11",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        import requests
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.get(url, params=params, headers=self._headers(), timeout=60)
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, body: dict) -> dict:
        import requests
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.post(url, json=body, headers=self._headers(), timeout=60)
        resp.raise_for_status()
        return resp.json()

    def put(self, path: str, body: dict) -> dict:
        import requests
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.put(url, json=body, headers=self._headers(), timeout=60)
        resp.raise_for_status()
        return resp.json()

    def upsert_contact(self, external_id: str = None, email: str = None, attributes: Optional[dict] = None) -> dict:
        """Intercom has no single-call native upsert for Contacts: this
        searches `/contacts/search` by external_id (or email), then PUTs
        the match or POSTs a new contact. Requires external_id OR email.
        """
        if not external_id and not email:
            raise ValueError("upsert_contact requires external_id or email.")
        field, value = ("external_id", external_id) if external_id else ("email", email)
        search_result = self.post("contacts/search", {
            "query": {"field": field, "operator": "=", "value": value}
        })
        matches = search_result.get("data") or []
        body = dict(attributes or {})
        if external_id:
            body["external_id"] = external_id
        if email:
            body["email"] = email
        if matches:
            return self.put(f"contacts/{matches[0]['id']}", body)
        body.setdefault("role", "user")
        return self.post("contacts", body)


class IntercomResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Intercom REST API resource."""

    resource_key: str = Field(default="intercom", description="Dagster resource key")
    api_token_env_var: str = Field(description="Env var with Intercom API token")
    base_url: str = Field(default="https://api.intercom.io")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: IntercomResource(
            api_token_env_var=self.api_token_env_var,
            base_url=self.base_url,
        )})
