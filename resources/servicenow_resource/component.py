"""ServiceNow Resource component.

Provides shared connection config (instance + auth) for the ServiceNow Table
API. Other components (`servicenow_ingestion`, `servicenow_sensor`,
optionally `dataframe_to_servicenow` in the future) can reference this
resource so credentials and the instance subdomain are centralized.

Two auth modes:

  1. **Basic auth** — username + password env vars. Right for ServiceNow
     Developer Instances (PDIs) and dev sandboxes.
  2. **Bearer token** — OAuth bearer token via env var. Pair with the
     community `oauth_token_resource` component to acquire + rotate the
     token from a ServiceNow OAuth app (client_credentials grant works
     headlessly; refresh_token grant for delegated flows).
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class ServiceNowResource(dg.ConfigurableResource):
    """Provides ServiceNow Table API base URL + auth headers / auth tuple."""

    instance: str  # e.g. 'mycompany' (no scheme, no .service-now.com)
    username: Optional[str] = None
    password: Optional[str] = None
    bearer_token: Optional[str] = None
    verify_ssl: bool = True

    @property
    def base_url(self) -> str:
        return f"https://{self.instance}.service-now.com"

    def table_url(self, table: str) -> str:
        return f"{self.base_url}/api/now/table/{urllib.parse.quote(table)}"

    def get_auth_headers(self) -> dict:
        """Headers for the request. Bearer token if present."""
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        return headers

    def get_auth(self):
        """Returns a (user, pass) tuple for requests' basic-auth, or None."""
        if self.bearer_token:
            return None
        if self.username and self.password:
            return (self.username, self.password)
        return None


class ServiceNowResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ServiceNow Table API resource for use by other components.

    Pairs with `servicenow_ingestion` for reading any ServiceNow table
    (incident / change_request / sc_request / cmdb_ci / sys_user / etc.)
    into a DataFrame asset, and with `servicenow_sensor` for triggering
    Dagster jobs on ServiceNow record state changes.

    Example (Developer Instance — basic auth):

        ```yaml
        type: dagster_component_templates.ServiceNowResourceComponent
        attributes:
          resource_key: servicenow_resource
          instance_env_var: SNOW_INSTANCE
          username_env_var: SNOW_USERNAME
          password_env_var: SNOW_PASSWORD
        ```

    Example (Production — OAuth bearer token):

        ```yaml
        type: dagster_component_templates.ServiceNowResourceComponent
        attributes:
          resource_key: servicenow_resource
          instance_env_var: SNOW_INSTANCE
          bearer_token_env_var: SNOW_ACCESS_TOKEN
        ```

    Pair with `oauth_token_resource` to acquire the bearer_token via OAuth
    client_credentials grant (headless) or refresh_token rotation (delegated).
    """

    resource_key: str = Field(
        default="servicenow_resource",
        description="Resource key. Other components reference it via this name.",
    )
    instance_env_var: str = Field(
        description=(
            "Env var with the ServiceNow instance subdomain (e.g. 'mycompany' or "
            "'dev123456'). Do NOT include 'https://' or '.service-now.com' — just "
            "the subdomain."
        ),
    )
    username_env_var: Optional[str] = Field(
        default=None,
        description="Env var with ServiceNow username (for basic auth). Required unless bearer_token_env_var is set.",
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Env var with ServiceNow password (for basic auth). Required unless bearer_token_env_var is set.",
    )
    bearer_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding a ServiceNow OAuth bearer token. When set, basic-auth "
            "fields are ignored. Pair with `oauth_token_resource` to acquire + "
            "rotate the token."
        ),
    )
    verify_ssl: bool = Field(
        default=True,
        description="Enable TLS certificate verification (only set false for self-signed dev instances).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        has_bearer = bool(self.bearer_token_env_var)
        has_basic = bool(self.username_env_var and self.password_env_var)
        if not (has_bearer or has_basic):
            raise ValueError(
                "ServiceNowResourceComponent: provide either bearer_token_env_var "
                "OR (username_env_var + password_env_var)."
            )

        resource = ServiceNowResource(
            instance=os.environ.get(self.instance_env_var, ""),
            username=os.environ.get(self.username_env_var, "") if self.username_env_var else None,
            password=os.environ.get(self.password_env_var, "") if self.password_env_var else None,
            bearer_token=os.environ.get(self.bearer_token_env_var, "") if self.bearer_token_env_var else None,
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
