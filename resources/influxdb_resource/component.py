"""InfluxDB resource component.

[InfluxDB](https://www.influxdata.com/) is a time-series database. This
resource targets **InfluxDB 2.x / 3.x** via the official ``influxdb_client``
Python SDK — v1.x is end-of-life and not covered here (file an issue if
needed).

Auth: API token + organization + bucket. v3 (Cloud Serverless / Dedicated)
uses the same token model.

Read path: Flux (v2.x) or SQL (v3.x — InfluxDB IOx engine speaks SQL via
Arrow Flight).

Pairs with:
  - ``dataframe_to_influxdb``       — write Pandas DataFrame as points
  - ``influxdb_query_asset``        — Flux/SQL → DataFrame
"""
from typing import Optional

import dagster as dg
from pydantic import Field


class InfluxDBResource(dg.ConfigurableResource):
    """Provides an InfluxDB 2.x / 3.x client."""

    url: str  # e.g. http://localhost:8086 (v2) or https://us-east-1-1.aws.cloud2.influxdata.com (v2/v3)
    token: str
    org: Optional[str] = None  # required for v2; optional for v3
    bucket: Optional[str] = None  # default destination bucket
    timeout_ms: int = 30000

    def get_client(self):
        """Return an influxdb_client.InfluxDBClient."""
        from influxdb_client import InfluxDBClient
        return InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout_ms)


class InfluxDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an InfluxDB resource.

    Example (local Docker InfluxDB 2.x):

        ```yaml
        type: dagster_community_components.InfluxDBResourceComponent
        attributes:
          resource_key: influxdb_resource
          url: http://localhost:8086
          token_env_var: INFLUXDB_TOKEN
          org: my-org
          bucket: metrics
        ```

    Example (InfluxDB Cloud 3.x):

        ```yaml
        type: dagster_community_components.InfluxDBResourceComponent
        attributes:
          resource_key: influxdb_resource
          url: https://us-east-1-1.aws.cloud2.influxdata.com
          token_env_var: INFLUXDB_TOKEN
          org: my-org
          bucket: events
        ```
    """

    resource_key: str = Field(default="influxdb_resource", description="Resource key.")
    url: str = Field(description="InfluxDB base URL.")
    token: Optional[str] = Field(default=None, description="API token. Set this OR token_env_var.")
    token_env_var: Optional[str] = Field(default=None, description="Env var with API token.")
    org: Optional[str] = Field(default=None, description="InfluxDB organization (required for v2).")
    bucket: Optional[str] = Field(default=None, description="Default destination bucket.")
    timeout_ms: int = Field(default=30000, ge=100)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = InfluxDBResource(
            url=self.url,
            token=self.token if self.token else dg.EnvVar(self.token_env_var or ""),
            org=self.org,
            bucket=self.bucket,
            timeout_ms=self.timeout_ms,
        )
        return dg.Definitions(resources={self.resource_key: resource})
