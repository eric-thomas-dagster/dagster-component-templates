"""Slack Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from pydantic import Field


@dataclass
class SlackResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-slack SlackResource for use by other components."""

    resource_key: str = Field(default="slack_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    token_env_var: str = Field(description="Environment variable holding the Slack bot token (xoxb-...)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_slack import SlackResource
        resource = SlackResource(token=os.environ.get(self.token_env_var, ""))
        return dg.Definitions(resources={self.resource_key: resource})
