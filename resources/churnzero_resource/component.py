"""ChurnZero Resource.

Wraps ChurnZero's v1 REST API (OData-flavored; Account/Contact/Event
objects). Unlike most SaaS APIs, ChurnZero's base URL is NOT a fixed domain
-- it is account-specific (and sometimes region-specific), of the shape:
  https://{subdomain}.churnzero.net/public/v1/

You find your exact base URL on the Application Key page under
Admin -> API Keys in your own ChurnZero tenant; it is NOT derivable from a
simple formula (some tenants are on dedicated/IP-pinned hosts), so this
resource takes the full base_url as config rather than assembling it from a
subdomain field.

Auth is HTTP Basic: your ChurnZero username (your login email) paired with
an API key as the password, base64-encoded in the Authorization header.
See: https://support.churnzero.com/hc/en-us/articles/360003183171
"""
import base64
import os
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class ChurnZeroResource(dg.ConfigurableResource):
    """ChurnZero REST API client wrapper (HTTP Basic auth: username + API key)."""

    base_url: str = Field(
        description="Your ChurnZero tenant's API base URL, e.g. 'https://yourtenant.churnzero.net'. "
                    "Find this on the Application Key page under Admin -> API Keys -- it is "
                    "account-specific and NOT a fixed/shared domain."
    )
    username_env_var: str = Field(
        default="CHURNZERO_USERNAME",
        description="Env var holding your ChurnZero login email (Basic auth username)",
    )
    api_key_env_var: str = Field(
        default="CHURNZERO_API_KEY",
        description="Env var holding the ChurnZero API key (Basic auth password)",
    )

    def _auth_header(self) -> str:
        username = os.environ.get(self.username_env_var)
        api_key = os.environ.get(self.api_key_env_var)
        if not username or not api_key:
            raise RuntimeError(
                f"Missing ChurnZero credentials: env vars {self.username_env_var!r} "
                f"and/or {self.api_key_env_var!r} are not set"
            )
        token = base64.b64encode(f"{username}:{api_key}".encode()).decode()
        return f"Basic {token}"

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> dict:
        """GET a path relative to base_url (e.g. 'public/v1/Account') and return parsed JSON."""
        import requests

        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.get(url, params=params or {}, headers={"Authorization": self._auth_header()}, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def get_url(self, full_url: str) -> dict:
        """GET a fully-qualified URL (e.g. an OData '@odata.nextLink' value) and return parsed JSON."""
        import requests

        resp = requests.get(full_url, headers={"Authorization": self._auth_header()}, timeout=60)
        resp.raise_for_status()
        return resp.json()


class ChurnZeroResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a `ChurnZeroResource` (ChurnZero REST API client) for use by other components.

    Other components reference this resource by the value of `resource_key`.
    """

    resource_key: str = Field(
        default="churnzero_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    base_url: str = Field(
        description="Your ChurnZero tenant's API base URL (account-specific -- see Admin -> API Keys)"
    )
    username_env_var: str = Field(
        default="CHURNZERO_USERNAME",
        description="Env var holding your ChurnZero login email",
    )
    api_key_env_var: str = Field(
        default="CHURNZERO_API_KEY",
        description="Env var holding the ChurnZero API key",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = ChurnZeroResource(
            base_url=self.base_url,
            username_env_var=self.username_env_var,
            api_key_env_var=self.api_key_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
