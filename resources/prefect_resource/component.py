"""Prefect resource — a Dagster resource that holds Prefect API connection info.

Downstream Prefect components can reference this resource by key, or you
can inline api_url + api_key_env_var directly on each Prefect component's
YAML. The resource is a convenience when you have many components pointing
at the same Prefect instance.

Works against:
  - Local Prefect server (`prefect server start` → default api_url is
    'http://127.0.0.1:4200/api').
  - Prefect Cloud (set api_key_env_var and api_url to your cloud account's
    API URL, e.g. 'https://api.prefect.cloud/api/accounts/<acct>/workspaces/<ws>').

Under the hood, the resource sets PREFECT_API_URL + PREFECT_API_KEY env
vars from its config so any Prefect SDK call sees the right instance.
"""
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class PrefectResource(dg.ConfigurableResource):
    """A Dagster resource that configures the Prefect Python SDK to point
    at a specific Prefect instance."""

    api_url: str = "http://127.0.0.1:4200/api"
    api_key_env_var: Optional[str] = None
    ui_url: Optional[str] = None

    def apply_env(self) -> None:
        """Set PREFECT_API_URL + PREFECT_API_KEY env vars so any subsequent
        Prefect SDK call uses this resource's connection."""
        os.environ["PREFECT_API_URL"] = self.api_url
        if self.api_key_env_var:
            key = os.environ.get(self.api_key_env_var)
            if key:
                os.environ["PREFECT_API_KEY"] = key

    def run_url(self, flow_run_id) -> str:
        """Link into the Prefect UI for a flow run. `ui_url` if set, else
        `api_url` with its trailing `/api` stripped (correct for a
        local/self-hosted server — Prefect Cloud needs `ui_url` set
        explicitly, since it serves the UI from a different host)."""
        base = (self.ui_url or self.api_url).rstrip("/").removesuffix("/api")
        return f"{base}/runs/flow-run/{flow_run_id}"


class PrefectResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Dagster resource that configures the Prefect Python SDK.

    Example (local Prefect server):

        ```yaml
        type: dagster_community_components.PrefectResourceComponent
        attributes:
          resource_key: prefect
          api_url: http://127.0.0.1:4200/api
        ```

    Example (Prefect Cloud):

        ```yaml
        type: dagster_community_components.PrefectResourceComponent
        attributes:
          resource_key: prefect
          api_url: https://api.prefect.cloud/api/accounts/<acct>/workspaces/<ws>
          api_key_env_var: PREFECT_API_KEY
        ```

    Downstream Prefect components reference this via `resource:` — or they
    can inline api_url + api_key_env_var themselves and skip the resource.
    """

    resource_key: str = Field(
        default="prefect",
        description="Resource key. Other Prefect components reference this.",
    )
    api_url: str = Field(
        default="http://127.0.0.1:4200/api",
        description="Prefect API URL. Default is local server at :4200.",
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a Prefect Cloud API key. Leave unset for local server.",
    )
    ui_url: Optional[str] = Field(
        default=None,
        description=(
            "Base URL of the Prefect UI, for building 'Prefect Run URL' links in "
            "materialization metadata. Defaults to api_url with its trailing '/api' "
            "stripped, correct for a local/self-hosted server. Prefect Cloud serves "
            "its UI from a different host — set this explicitly there."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(
            resources={
                self.resource_key: PrefectResource(
                    api_url=self.api_url,
                    api_key_env_var=self.api_key_env_var,
                    ui_url=self.ui_url,
                )
            }
        )
