"""Datadog Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class DatadogResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-datadog DatadogResource for use by other components."""

    resource_key: str = Field(default="datadog_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    api_key_env_var: str = Field(description="Environment variable holding the Datadog API key")
    app_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Datadog application key")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_datadog import DatadogResource
        resource = DatadogResource(
            api_key=os.environ.get(self.api_key_env_var, ""),
            app_key=os.environ.get(self.app_key_env_var, "") if self.app_key_env_var else "",
        )
        return dg.Definitions(resources={self.resource_key: resource})
