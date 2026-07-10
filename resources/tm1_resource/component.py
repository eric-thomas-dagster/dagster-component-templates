"""IBM Planning Analytics (TM1) Resource component.

Provides shared connection config (base URL + auth) for the **TM1 REST API v11+**.
Other components (`tm1_process_trigger_job`, `tm1_process_status_sensor`,
`tm1_cube_data_ingestion`, `tm1_workspace`) reference this resource so
credentials and the server endpoint are centralized.

Two auth modes:

  1. **Basic auth** — username + password. Works with `IntegratedSecurityMode: 1`
     TM1 servers (native TM1 users). Right for dev / self-hosted.
  2. **CAM auth** — username + password + CAM namespace. Works with
     `IntegratedSecurityMode: 4/5` TM1 servers (IBM Cognos CAM SSO).
     Common in enterprise deployments.

The TM1 REST API base path is `/api/v1`.
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class TM1Resource(dg.ConfigurableResource):
    """Provides TM1 base URL + auth for REST calls."""

    base_url: str  # e.g. 'https://tm1.acme.com:5495' (with port, no /api path)
    username: Optional[str] = None
    password: Optional[str] = None
    cam_namespace: Optional[str] = None  # CAM security only
    verify_ssl: bool = True

    @property
    def api_base(self) -> str:
        return f"{self.base_url.rstrip('/')}/api/v1"

    def cube_url(self, cube: str) -> str:
        return f"{self.api_base}/Cubes('{urllib.parse.quote(cube)}')"

    def process_url(self, process: str) -> str:
        return f"{self.api_base}/Processes('{urllib.parse.quote(process)}')"

    def chore_url(self, chore: str) -> str:
        return f"{self.api_base}/Chores('{urllib.parse.quote(chore)}')"

    def get_auth_headers(self) -> dict:
        """Headers for basic + optional CAM namespace."""
        import base64
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.username and self.password:
            token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
            if self.cam_namespace:
                # TM1 CAM SSO: Authorization header uses CAMNamespace prefix.
                # user:password:namespace base64-encoded.
                cam_token = base64.b64encode(
                    f"{self.username}:{self.password}:{self.cam_namespace}".encode()
                ).decode()
                headers["Authorization"] = f"CAMNamespace {cam_token}"
        return headers


class TM1ResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a TM1 REST API resource for other components.

    Pairs with:
      - `tm1_process_trigger_job` (execute a TI process / chore)
      - `tm1_process_status_sensor` (event-drive on process state)
      - `tm1_cube_data_ingestion` (materialize a cube slice as a DataFrame)
      - `tm1_workspace` (auto-emit assets per Cube / Process / Chore)

    Example (native TM1 users — basic auth):

        ```yaml
        type: dagster_community_components.TM1ResourceComponent
        attributes:
          resource_key: tm1_resource
          base_url_env_var: TM1_URL            # e.g. https://tm1.acme.com:5495
          username_env_var: TM1_USER
          password_env_var: TM1_PASSWORD
        ```

    Example (CAM SSO — enterprise):

        ```yaml
        attributes:
          resource_key: tm1_resource
          base_url_env_var: TM1_URL
          username_env_var: TM1_USER
          password_env_var: TM1_PASSWORD
          cam_namespace_env_var: TM1_CAM_NS    # e.g. LDAP or your CAM namespace
        ```
    """

    resource_key: str = Field(
        default="tm1_resource",
        description="Resource key. Other components reference it via this name.",
    )
    base_url_env_var: str = Field(
        description=(
            "Env var with the TM1 server base URL, e.g. 'https://tm1.acme.com:5495'. "
            "Include the port. Do NOT include '/api/v1' — the resource appends it."
        ),
    )
    username_env_var: str = Field(description="Env var with TM1 username.")
    password_env_var: str = Field(description="Env var with TM1 password.")
    cam_namespace_env_var: Optional[str] = Field(
        default=None,
        description="Env var with CAM namespace (Cognos SSO). Omit for native TM1 auth.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Enable TLS certificate verification. Set false only for self-signed dev servers.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        resource = TM1Resource(
            base_url=os.environ.get(self.base_url_env_var, ""),
            username=os.environ.get(self.username_env_var, ""),
            password=os.environ.get(self.password_env_var, ""),
            cam_namespace=os.environ.get(self.cam_namespace_env_var, "") if self.cam_namespace_env_var else None,
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
