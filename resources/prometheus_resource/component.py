"""PrometheusResourceComponent.

Wraps `dagster-prometheus`'s `PrometheusResource` so any asset / op can push counters and gauges to a Prometheus pushgateway. Useful for emitting per-run metrics that flow into Grafana dashboards.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class PrometheusResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Push metrics to a Prometheus pushgateway via dagster-prometheus."""

    gateway: str = Field(description="Prometheus pushgateway URL (e.g. 'http://prometheus-pushgateway:9091').")
    timeout: int = Field(default=30, description="Push timeout in seconds.")
    resource_key: str = Field(default="prometheus", description="Dagster resource key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_prometheus import PrometheusResource
        resource = PrometheusResource(gateway=self.gateway, timeout=self.timeout)
        return dg.Definitions(resources={self.resource_key: resource})

