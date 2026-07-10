"""JDE Orchestrator Resource component.

Provides shared connection config (base URL + auth) for the **JD Edwards
EnterpriseOne Orchestrator REST API**. Orchestrator is Oracle's low-code
automation tool for JDE — it composes JDE Business Functions, Interoperability
Services, and Data Services into orchestrations that can be triggered via REST.

Other components (`jde_orchestration_trigger_job`, `jde_orchestration_status_sensor`,
`jde_orchestration_output_ingestion`, `jde_orchestrator_workspace`) reference
this resource so credentials + base URL are centralized.

Auth: HTTP Basic with JDE credentials. The username / password pair is what
you'd log into the AIS Server with.

Orchestrator REST API base path is `/jderest/v3/orchestrator` (or `/jderest/v2/orchestrator`
depending on JDE Tools version — the resource accepts a configurable path prefix).
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class JDEOrchestratorResource(dg.ConfigurableResource):
    """Provides JDE Orchestrator base URL + auth."""

    base_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    api_path_prefix: str = "/jderest/v3/orchestrator"
    verify_ssl: bool = True

    @property
    def api_base(self) -> str:
        return f"{self.base_url.rstrip('/')}{self.api_path_prefix}"

    def orchestration_url(self, orchestration: str) -> str:
        return f"{self.api_base}/{urllib.parse.quote(orchestration)}"

    def status_url(self, job_id: str) -> str:
        return f"{self.base_url.rstrip('/')}/jderest/v3/orchestrator/status/{urllib.parse.quote(job_id)}"

    def get_auth_headers(self) -> dict:
        import base64
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.username and self.password:
            token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
        return headers


class JDEOrchestratorResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a JDE Orchestrator REST API resource.

    Pairs with:
      - `jde_orchestration_trigger_job` (execute an orchestration)
      - `jde_orchestration_status_sensor` (event-drive on completion)
      - `jde_orchestration_output_ingestion` (result set as a DataFrame)
      - `jde_orchestrator_workspace` (auto-emit assets per orchestration)

    Example:

        ```yaml
        type: dagster_community_components.JDEOrchestratorResourceComponent
        attributes:
          resource_key: jde_orchestrator_resource
          base_url_env_var: JDE_AIS_URL       # e.g. https://ais.acme.com
          username_env_var: JDE_USER
          password_env_var: JDE_PASSWORD
        ```
    """

    resource_key: str = Field(default="jde_orchestrator_resource")
    base_url_env_var: str = Field(description="Env var with the AIS server base URL (no /jderest path).")
    username_env_var: str = Field(description="Env var with JDE username.")
    password_env_var: str = Field(description="Env var with JDE password.")
    api_path_prefix: str = Field(default="/jderest/v3/orchestrator", description="Orchestrator API path. v3 for JDE Tools 9.2.7+, v2 for older.")
    verify_ssl: bool = Field(default=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        resource = JDEOrchestratorResource(
            base_url=os.environ.get(self.base_url_env_var, ""),
            username=os.environ.get(self.username_env_var, ""),
            password=os.environ.get(self.password_env_var, ""),
            api_path_prefix=self.api_path_prefix,
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
