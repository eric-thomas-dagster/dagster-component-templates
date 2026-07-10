"""Cognos Analytics Resource component.

Provides shared connection config (base URL + auth) for the **IBM Cognos
Analytics REST API v1+**. Other components (`cognos_report_run_job`,
`cognos_report_status_sensor`, `cognos_report_data_ingestion`,
`cognos_workspace`) reference this resource so credentials and the base
URL are centralized.

Auth: session-based via `POST /api/v1/session` with namespace/username/password
(where `namespace` is a Cognos security namespace like `CognosEx` or `LDAP`).
Session cookie is cached on the requests.Session object.

Cognos REST API base path is `/api/v1`.
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class CognosResource(dg.ConfigurableResource):
    """Provides Cognos Analytics base URL + auth."""

    base_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    namespace: Optional[str] = None  # Cognos security namespace
    verify_ssl: bool = True

    @property
    def api_base(self) -> str:
        return f"{self.base_url.rstrip('/')}/api/v1"

    def report_url(self, report_id: str) -> str:
        return f"{self.api_base}/reports/{urllib.parse.quote(report_id)}"

    def content_url(self, path: str) -> str:
        return f"{self.api_base}/content{path if path.startswith('/') else '/' + path}"

    def get_auth_headers(self) -> dict:
        return {"Accept": "application/json", "Content-Type": "application/json"}

    def login_body(self) -> Optional[dict]:
        if self.username and self.password and self.namespace:
            return {
                "parameters": [
                    {"name": "CAMNamespace", "value": self.namespace},
                    {"name": "CAMUsername", "value": self.username},
                    {"name": "CAMPassword", "value": self.password},
                ]
            }
        return None


class CognosResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Cognos Analytics REST API resource.

    Pairs with:
      - `cognos_report_run_job` (execute a report)
      - `cognos_report_status_sensor` (event-drive)
      - `cognos_report_data_ingestion` (report result as DataFrame)
      - `cognos_workspace` (auto-emit assets per Folder × Report × Dashboard)

    Example:

        ```yaml
        type: dagster_community_components.CognosResourceComponent
        attributes:
          resource_key: cognos_resource
          base_url_env_var: COGNOS_URL          # e.g. https://cognos.acme.com
          username_env_var: COGNOS_USER
          password_env_var: COGNOS_PASSWORD
          namespace_env_var: COGNOS_NAMESPACE   # e.g. LDAP or CognosEx
        ```
    """

    resource_key: str = Field(default="cognos_resource")
    base_url_env_var: str = Field(description="Env var with Cognos Analytics base URL (no /api path).")
    username_env_var: str = Field(description="Env var with Cognos username.")
    password_env_var: str = Field(description="Env var with Cognos password.")
    namespace_env_var: str = Field(description="Env var with the Cognos security namespace (e.g. LDAP, CognosEx).")
    verify_ssl: bool = Field(default=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        resource = CognosResource(
            base_url=os.environ.get(self.base_url_env_var, ""),
            username=os.environ.get(self.username_env_var, ""),
            password=os.environ.get(self.password_env_var, ""),
            namespace=os.environ.get(self.namespace_env_var, ""),
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
