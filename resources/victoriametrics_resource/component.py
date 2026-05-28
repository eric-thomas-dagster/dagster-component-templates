"""VictoriaMetrics resource component.

[VictoriaMetrics](https://victoriametrics.com/) is a high-performance,
Prometheus-compatible time-series database. Speaks the Prometheus HTTP
API for reads + its own ``/api/v1/import/prometheus`` and
``/api/v1/write`` endpoints for ingestion. We use it at Dagster
internally; this resource exposes its read + write surface as a
Dagster ConfigurableResource.

Endpoints (vmsingle / vmselect default ports):

  - Read:    GET  {url}/api/v1/query             (PromQL instant)
             GET  {url}/api/v1/query_range       (PromQL range)
             GET  {url}/api/v1/labels            (label discovery)
  - Ingest:  POST {url}/api/v1/import/prometheus (Prometheus text format)
             POST {url}/api/v1/write             (Prometheus remote-write)
             POST {url}/api/v1/import             (JSON-lines native)

Auth: optional bearer token (vmauth-fronted clusters) or HTTP basic.

Pairs with:
  - ``dataframe_to_victoriametrics``  — write Pandas DataFrame as series
  - ``victoriametrics_query_asset``  — PromQL query → DataFrame
"""
from typing import Optional

import dagster as dg
from pydantic import Field


class VictoriaMetricsResource(dg.ConfigurableResource):
    """Provides a VictoriaMetrics HTTP endpoint + auth.

    Doesn't open a long-lived connection — VictoriaMetrics is HTTP-only.
    Components compose URLs against ``base_url`` + add auth headers via
    ``auth_headers()``.
    """

    base_url: str  # e.g. http://vm.mycompany.com:8428
    bearer_token: Optional[str] = None
    basic_user: Optional[str] = None
    basic_password: Optional[str] = None
    timeout_seconds: int = 60

    def auth_headers(self) -> dict:
        if self.bearer_token:
            return {"Authorization": f"Bearer {self.bearer_token}"}
        return {}

    def basic_auth(self) -> Optional[tuple]:
        if self.basic_user:
            return (self.basic_user, self.basic_password or "")
        return None

    def query_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/api/v1/query"

    def query_range_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/api/v1/query_range"

    def write_prometheus_url(self) -> str:
        """Endpoint for Prometheus text-format ingestion (line per series)."""
        return f"{self.base_url.rstrip('/')}/api/v1/import/prometheus"

    def remote_write_url(self) -> str:
        """Endpoint for Prometheus remote-write (snappy-compressed protobuf)."""
        return f"{self.base_url.rstrip('/')}/api/v1/write"


class VictoriaMetricsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a VictoriaMetrics resource for use by other components.

    Example (local docker / single-node):

        ```yaml
        type: dagster_community_components.VictoriaMetricsResourceComponent
        attributes:
          resource_key: vm_resource
          base_url: http://localhost:8428
        ```

    Example (production cluster fronted by vmauth):

        ```yaml
        type: dagster_community_components.VictoriaMetricsResourceComponent
        attributes:
          resource_key: vm_resource
          base_url: https://vm.mycompany.com
          bearer_token_env_var: VM_TOKEN
        ```
    """

    resource_key: str = Field(default="vm_resource", description="Resource key.")
    base_url: str = Field(description="VictoriaMetrics base URL (e.g. http://localhost:8428).")
    bearer_token: Optional[str] = Field(default=None, description="Bearer token (literal). Set this OR bearer_token_env_var.")
    bearer_token_env_var: Optional[str] = Field(default=None, description="Env var with bearer token.")
    basic_user: Optional[str] = Field(default=None, description="Optional HTTP basic auth user.")
    basic_password_env_var: Optional[str] = Field(default=None, description="Env var with HTTP basic password.")
    timeout_seconds: int = Field(default=60, ge=1)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = VictoriaMetricsResource(
            base_url=self.base_url,
            bearer_token=self.bearer_token if self.bearer_token else (
                dg.EnvVar(self.bearer_token_env_var) if self.bearer_token_env_var else None
            ),
            basic_user=self.basic_user,
            basic_password=dg.EnvVar(self.basic_password_env_var) if self.basic_password_env_var else None,
            timeout_seconds=self.timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
