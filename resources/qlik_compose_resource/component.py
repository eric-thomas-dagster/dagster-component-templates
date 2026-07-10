"""Qlik Compose Resource component.

Provides shared connection config (base URL + auth) for the **Qlik Compose
REST API**. Qlik Compose is the data-warehouse automation tool in the Qlik
Data Integration platform (paired with Qlik Replicate for CDC).

Other components (`qlik_compose_workflow_trigger_job`,
`qlik_compose_workflow_status_sensor`, `qlik_compose_workflow_metrics_ingestion`,
`qlik_compose_workspace`) reference this resource so credentials and the
base URL are centralized.

Two auth modes:
  1. **Session-based** — username + password login via `/api/v1/login`,
     session cookie cached.
  2. **API token** — bearer token from the Compose UI.

Compose REST API base path is `/qlikcompose/api/v1`.
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class QlikComposeResource(dg.ConfigurableResource):
    """Provides Qlik Compose base URL + auth."""

    base_url: str  # e.g. 'https://qlikcompose.acme.com'
    username: Optional[str] = None
    password: Optional[str] = None
    api_token: Optional[str] = None
    verify_ssl: bool = True

    @property
    def api_base(self) -> str:
        return f"{self.base_url.rstrip('/')}/qlikcompose/api/v1"

    def project_url(self, project: str) -> str:
        return f"{self.api_base}/projects/{urllib.parse.quote(project)}"

    def workflow_url(self, project: str, workflow: str) -> str:
        return f"{self.project_url(project)}/workflows/{urllib.parse.quote(workflow)}"

    def data_mart_url(self, project: str, data_mart: str) -> str:
        return f"{self.project_url(project)}/data_marts/{urllib.parse.quote(data_mart)}"

    def get_auth_headers(self) -> dict:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def login_body(self) -> Optional[dict]:
        if self.api_token:
            return None
        if self.username and self.password:
            return {"username": self.username, "password": self.password}
        return None


class QlikComposeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Qlik Compose REST API resource.

    Pairs with:
      - `qlik_compose_workflow_trigger_job` (start / stop a workflow)
      - `qlik_compose_workflow_status_sensor` (event-drive on state)
      - `qlik_compose_workflow_metrics_ingestion` (per-workflow metrics)
      - `qlik_compose_workspace` (auto-emit assets per Project × Workflow × Data Mart)

    Example:

        ```yaml
        type: dagster_community_components.QlikComposeResourceComponent
        attributes:
          resource_key: qlik_compose_resource
          base_url_env_var: QLIK_COMPOSE_URL
          api_token_env_var: QLIK_COMPOSE_TOKEN
        ```
    """

    resource_key: str = Field(default="qlik_compose_resource")
    base_url_env_var: str = Field(description="Env var with the Compose base URL (no /api path).")
    username_env_var: Optional[str] = Field(default=None, description="Env var with username (session auth).")
    password_env_var: Optional[str] = Field(default=None, description="Env var with password (session auth).")
    api_token_env_var: Optional[str] = Field(default=None, description="Env var with API token (preferred for prod).")
    verify_ssl: bool = Field(default=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        has_token = bool(self.api_token_env_var)
        has_basic = bool(self.username_env_var and self.password_env_var)
        if not (has_token or has_basic):
            raise ValueError(
                "QlikComposeResourceComponent: provide api_token_env_var OR (username_env_var + password_env_var)."
            )
        resource = QlikComposeResource(
            base_url=os.environ.get(self.base_url_env_var, ""),
            username=os.environ.get(self.username_env_var, "") if self.username_env_var else None,
            password=os.environ.get(self.password_env_var, "") if self.password_env_var else None,
            api_token=os.environ.get(self.api_token_env_var, "") if self.api_token_env_var else None,
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
