"""PagerDuty Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from pydantic import Field


@dataclass
class PagerDutyResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-pagerduty PagerDutyService for use by other components."""

    resource_key: str = Field(default="pagerduty_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    routing_key_env_var: str = Field(description="Environment variable holding the PagerDuty Events API v2 routing key (integration key)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_pagerduty import PagerDutyService
        resource = PagerDutyService(
            routing_key=os.environ.get(self.routing_key_env_var, ""),
        )
        return dg.Definitions(resources={self.resource_key: resource})
