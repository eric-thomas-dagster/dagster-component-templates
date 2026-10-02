"""Metronome Resource component.

Bearer-token wrapper over the Metronome usage-based billing REST API.

Base URL: https://api.metronome.com/v1
Auth header: Authorization: Bearer <api_key>

Verified against docs.metronome.com (2026-10):
  - Metronome's API uses a static bearer API token (no OAuth2 handshake) --
    created in the Metronome app under Developer > API tokens, sent as
    `Authorization: Bearer <token>` on every request.
  - The usage-event ingestion endpoint (`POST /v1/ingest`) and read
    endpoints (`GET /v1/customers`, `GET /v1/customers/{id}/invoices`) all
    live under this same base URL and share this same auth scheme.

Drop to `.get_client()` for anything not covered -- returns an
authenticated `requests.Session`. Per this repo's self-contained-component
convention, the one real external-call boundary (`session.request(...)`)
is NOT wrapped here with convenience methods -- each consuming component
(`metronome_usage_event_send`, `metronome_invoices_ingestion`) isolates its
own module-level API-call function around `get_client()` so it can be
monkeypatched independently in tests, and so the two sibling components
never share helper code with each other.
"""
import os

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class MetronomeResource(ConfigurableResource):
    """Dagster resource wrapping the Metronome REST API (bearer-token auth)."""

    api_key_env_var: str = Field(description="Env var holding the Metronome API key (bearer token).")
    base_url: str = Field(
        default="https://api.metronome.com/v1",
        description="Metronome API base URL. Override only for a regional/dedicated deployment.",
    )

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch for anything
        not covered by a dedicated component -- every Metronome endpoint (ingest,
        customers, invoices, contracts, ...) lives under the same bearer auth."""
        import requests

        api_key = os.environ.get(self.api_key_env_var)
        if not api_key:
            raise ValueError(
                f"MetronomeResource: env var {self.api_key_env_var!r} is unset or empty."
            )
        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })
        return session


class MetronomeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a MetronomeResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.MetronomeResourceComponent
        attributes:
          resource_key: metronome_resource
          api_key_env_var: METRONOME_API_KEY
        ```
    """

    resource_key: str = Field(
        default="metronome_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        description="Env var holding the Metronome API key (bearer token).",
    )
    base_url: str = Field(
        default="https://api.metronome.com/v1",
        description="Metronome API base URL. Override only for a regional/dedicated deployment.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = MetronomeResource(
            api_key_env_var=self.api_key_env_var,
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
