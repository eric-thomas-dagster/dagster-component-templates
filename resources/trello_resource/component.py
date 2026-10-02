"""Trello Resource.

Wraps Trello's REST API (https://api.trello.com/1).

Auth: API key + token, passed as QUERY STRING parameters on every request
(`?key=...&token=...`) -- Trello has no header-based auth scheme. Same
convention already established by `trello_ingestion` in this repo.
"""
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class TrelloResource(dg.ConfigurableResource):
    """Trello REST API client wrapper (key+token query-param auth)."""

    api_key_env_var: str = Field(description="Env var holding the Trello API key.")
    api_token_env_var: str = Field(description="Env var holding the Trello API token.")
    base_url: str = Field(
        default="https://api.trello.com/1",
        description="Trello API base URL.",
    )

    def request(self, method: str, path: str, params: Optional[dict] = None, json_body: Optional[dict] = None) -> dict:
        """Issue one HTTP request (relative to base_url). `key`/`token` are
        merged into the query string automatically."""
        import requests

        api_key = os.environ.get(self.api_key_env_var)
        api_token = os.environ.get(self.api_token_env_var)
        if not api_key or not api_token:
            raise RuntimeError(
                f"Missing Trello API key/token env var(s): {self.api_key_env_var}/{self.api_token_env_var}"
            )

        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        all_params = dict(params or {})
        all_params["key"] = api_key
        all_params["token"] = api_token
        resp = requests.request(method, url, params=all_params, json=json_body, timeout=60)
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()


class TrelloResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a TrelloResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.TrelloResourceComponent
        attributes:
          resource_key: trello_resource
          api_key_env_var: TRELLO_API_KEY
          api_token_env_var: TRELLO_API_TOKEN
        ```
    """

    resource_key: str = Field(
        default="trello_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        default="TRELLO_API_KEY",
        description="Env var holding the Trello API key.",
    )
    api_token_env_var: str = Field(
        default="TRELLO_API_TOKEN",
        description="Env var holding the Trello API token.",
    )
    base_url: str = Field(
        default="https://api.trello.com/1",
        description="Trello API base URL.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = TrelloResource(
            api_key_env_var=self.api_key_env_var,
            api_token_env_var=self.api_token_env_var,
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
