"""HTTP Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


class HTTPResource(dg.ConfigurableResource):
    """Provides a configured requests.Session with base URL and auth header."""

    base_url: str
    auth_header_value: str = ""
    timeout_seconds: int = 30

    def get_session(self):
        import requests
        session = requests.Session()
        session.headers["Authorization"] = self.auth_header_value
        return session

    def get(self, path: str, **kwargs):
        return self.get_session().get(f"{self.base_url.rstrip('/')}/{path.lstrip('/')}", timeout=self.timeout_seconds, **kwargs)

    def post(self, path: str, **kwargs):
        return self.get_session().post(f"{self.base_url.rstrip('/')}/{path.lstrip('/')}", timeout=self.timeout_seconds, **kwargs)


@dataclass
class HTTPResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an HTTP resource (base URL + auth) for use by other components."""

    resource_key: str = Field(default="http_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    base_url: str = Field(description="Base URL for all requests, e.g. 'https://api.example.com/v1'")
    auth_header_env_var: Optional[str] = Field(default=None, description="Env var containing the Authorization header value, e.g. 'Bearer token123'")
    timeout_seconds: int = Field(default=30, description="Request timeout in seconds")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = HTTPResource(
            base_url=self.base_url,
            auth_header_value=os.environ.get(self.auth_header_env_var, "") if self.auth_header_env_var else "",
            timeout_seconds=self.timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
