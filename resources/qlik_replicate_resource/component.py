"""Qlik Replicate Resource component.

Provides shared connection config (base URL + auth) for the **Qlik Enterprise
Manager REST API**, which is the control plane for Qlik Replicate. Other
components (`qlik_replicate_task_trigger_job`, `qlik_replicate_task_status_sensor`,
`qlik_replicate_task_metrics_ingestion`) reference this resource so credentials
and server list are centralized.

Two auth modes:

  1. **Session-based (Windows / SAML / LDAP)** — username + password. The
     resource logs in on first use and caches the `EnterpriseManager.APISessionID`
     cookie. Right for dev / air-gapped installs.
  2. **API token** — bearer token acquired from Enterprise Manager UI under
     "Manage API Tokens." Right for production / CI use.

Enterprise Manager REST API base path is `/attunityenterprisemanager/api/v1`.
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class QlikReplicateResource(dg.ConfigurableResource):
    """Provides Qlik Enterprise Manager base URL + auth for REST calls."""

    base_url: str  # e.g. 'https://qlikem.acme.com' (no trailing slash, no API path)
    username: Optional[str] = None
    password: Optional[str] = None
    api_token: Optional[str] = None
    verify_ssl: bool = True

    @property
    def api_base(self) -> str:
        return f"{self.base_url.rstrip('/')}/attunityenterprisemanager/api/v1"

    def task_url(self, server: str, task: str) -> str:
        return (
            f"{self.api_base}/servers/{urllib.parse.quote(server)}"
            f"/tasks/{urllib.parse.quote(task)}"
        )

    def server_url(self, server: str) -> str:
        return f"{self.api_base}/servers/{urllib.parse.quote(server)}"

    def get_auth_headers(self) -> dict:
        """Headers for the request. API-token if present, otherwise empty
        (session cookie handled by the caller via a `requests.Session`)."""
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def login_body(self) -> Optional[dict]:
        """Body for the login POST when using session-based auth. Returns
        None when API-token auth is configured (no login needed)."""
        if self.api_token:
            return None
        if self.username and self.password:
            return {"username": self.username, "password": self.password}
        return None


class QlikReplicateResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Qlik Enterprise Manager resource for other components.

    Pairs with:
      - `qlik_replicate_task_trigger_job` (start / stop / resume a task)
      - `qlik_replicate_task_status_sensor` (event-drive on task state)
      - `qlik_replicate_task_metrics_ingestion` (materialize task metrics)

    Example (API token — production):

        ```yaml
        type: dagster_community_components.QlikReplicateResourceComponent
        attributes:
          resource_key: qlik_replicate_resource
          base_url_env_var: QLIK_EM_URL           # e.g. https://qlikem.acme.com
          api_token_env_var: QLIK_EM_API_TOKEN
        ```

    Example (basic auth — dev / self-hosted):

        ```yaml
        type: dagster_community_components.QlikReplicateResourceComponent
        attributes:
          resource_key: qlik_replicate_resource
          base_url_env_var: QLIK_EM_URL
          username_env_var: QLIK_EM_USER
          password_env_var: QLIK_EM_PASSWORD
        ```
    """

    resource_key: str = Field(
        default="qlik_replicate_resource",
        description="Resource key. Other components reference it via this name.",
    )
    base_url_env_var: str = Field(
        description=(
            "Env var with the Enterprise Manager base URL, e.g. "
            "'https://qlikem.acme.com'. Do NOT include the '/attunityenterprisemanager/api/v1' "
            "path — the resource appends it."
        ),
    )
    username_env_var: Optional[str] = Field(
        default=None,
        description="Env var with Enterprise Manager username (session-based auth). Required unless api_token_env_var is set.",
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Env var with Enterprise Manager password (session-based auth). Required unless api_token_env_var is set.",
    )
    api_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var with a Qlik Enterprise Manager API token. Preferred for "
            "production / CI. When set, username/password fields are ignored."
        ),
    )
    verify_ssl: bool = Field(
        default=True,
        description="Enable TLS certificate verification. Set false only for self-signed dev servers.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        has_token = bool(self.api_token_env_var)
        has_basic = bool(self.username_env_var and self.password_env_var)
        if not (has_token or has_basic):
            raise ValueError(
                "QlikReplicateResourceComponent: provide either api_token_env_var "
                "OR (username_env_var + password_env_var)."
            )

        resource = QlikReplicateResource(
            base_url=os.environ.get(self.base_url_env_var, ""),
            username=os.environ.get(self.username_env_var, "") if self.username_env_var else None,
            password=os.environ.get(self.password_env_var, "") if self.password_env_var else None,
            api_token=os.environ.get(self.api_token_env_var, "") if self.api_token_env_var else None,
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
