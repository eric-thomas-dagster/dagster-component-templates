"""OpenSearch Resource component."""
import os
from typing import Optional, Tuple

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class OpenSearchResource(ConfigurableResource):
    """Dagster resource for an OpenSearch cluster's REST API (raw HTTP, no
    opensearch-py dependency). Primary auth case is HTTP basic auth
    (username/password), which is how most self-managed and AWS OpenSearch
    (fine-grained access control) deployments are configured."""

    host: str = Field(description="OpenSearch endpoint, e.g. 'https://my-cluster:9200'")
    username_env_var: str = Field(
        default="",
        description="Env var holding the basic-auth username. Mutually exclusive with api_key_env_var.",
    )
    password_env_var: str = Field(
        default="",
        description="Env var holding the basic-auth password. Required if username_env_var is set.",
    )
    api_key_env_var: str = Field(
        default="",
        description=(
            "Env var holding an API key for the 'Authorization: ApiKey ...' "
            "header. Mutually exclusive with username_env_var/password_env_var."
        ),
    )
    verify_ssl: bool = Field(
        default=True, description="Verify TLS certificates. Set false for self-signed dev clusters only."
    )

    def get_base_url(self) -> str:
        return self.host.rstrip("/")

    def get_auth(self) -> Optional[Tuple[str, str]]:
        if self.username_env_var:
            return (
                os.environ.get(self.username_env_var, ""),
                os.environ.get(self.password_env_var, ""),
            )
        return None

    def get_headers(self) -> dict:
        headers = {"Content-Type": "application/x-ndjson"}
        if self.api_key_env_var:
            api_key = os.environ.get(self.api_key_env_var, "")
            headers["Authorization"] = f"ApiKey {api_key}"
        return headers


class OpenSearchResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an OpenSearchResource for use by other components."""

    resource_key: str = Field(
        default="opensearch_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    host: str = Field(description="OpenSearch endpoint, e.g. 'https://my-cluster:9200'")
    username_env_var: Optional[str] = Field(
        default="", description="Env var holding the basic-auth username (primary auth case)."
    )
    password_env_var: Optional[str] = Field(
        default="", description="Env var holding the basic-auth password."
    )
    api_key_env_var: Optional[str] = Field(
        default="", description="Env var holding an API key (alternative to basic auth)."
    )
    verify_ssl: bool = Field(default=True, description="Verify TLS certificates.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = OpenSearchResource(
            host=self.host,
            username_env_var=self.username_env_var or "",
            password_env_var=self.password_env_var or "",
            api_key_env_var=self.api_key_env_var or "",
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
