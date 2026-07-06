"""Gong Resource component.

Wraps the Gong API v2 Basic Auth credentials as a Dagster resource. Gong
authenticates every REST call with an `access_key` + `access_key_secret` pair
passed as HTTP Basic Auth. This resource stores the two credentials and
exposes `get_client()` which returns a pre-authenticated `requests.Session`
already configured with the correct base URL and auth header.
"""
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class GongClient:
    """Thin adapter over `requests.Session` pre-configured for Gong's API v2.

    Attributes:
        base_url: e.g. `https://api.gong.io/`.
        session: `requests.Session` with Basic Auth applied. Callers can hit
            any endpoint via ``session.get(f"{base_url}v2/calls")`` etc.
    """

    def __init__(self, access_key: str, access_key_secret: str, base_url: str):
        import requests
        self.base_url = base_url.rstrip("/") + "/"
        self.session = requests.Session()
        self.session.auth = (access_key, access_key_secret)
        self.session.headers.update({"Content-Type": "application/json"})

    def get(self, path: str, **kwargs):
        return self.session.get(self.base_url + path.lstrip("/"), **kwargs)

    def post(self, path: str, **kwargs):
        return self.session.post(self.base_url + path.lstrip("/"), **kwargs)


class GongResource(ConfigurableResource):
    """Dagster resource wrapping Gong API v2 Basic Auth credentials."""

    access_key: str = Field(description="Gong API access key (username in Basic Auth)")
    access_key_secret: str = Field(description="Gong API access key secret (password in Basic Auth)")
    base_url: str = Field(
        default="https://api.gong.io/",
        description="Gong API base URL. Override for regional endpoints (EU/APAC).",
    )

    def get_client(self) -> GongClient:
        return GongClient(
            access_key=self.access_key,
            access_key_secret=self.access_key_secret,
            base_url=self.base_url,
        )


class GongResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a GongResource for use by other components."""

    resource_key: str = Field(
        default="gong_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    access_key_env_var: str = Field(
        default="GONG_ACCESS_KEY",
        description="Env var holding the Gong API access key",
    )
    access_key_secret_env_var: str = Field(
        default="GONG_ACCESS_KEY_SECRET",
        description="Env var holding the Gong API access key secret",
    )
    base_url: str = Field(
        default="https://api.gong.io/",
        description="Gong API base URL. Override for regional endpoints (EU/APAC).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = GongResource(
            access_key=dg.EnvVar(self.access_key_env_var),
            access_key_secret=dg.EnvVar(self.access_key_secret_env_var),
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
