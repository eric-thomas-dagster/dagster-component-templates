"""Anthropic Resource component."""
from dataclasses import dataclass
import dagster as dg
from pydantic import Field


@dataclass
class AnthropicResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-anthropic AnthropicResource for use by other components."""

    resource_key: str = Field(default="anthropic_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    api_key_env_var: str = Field(default="ANTHROPIC_API_KEY", description="Environment variable holding the Anthropic API key")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_anthropic import AnthropicResource
        resource = AnthropicResource(api_key=dg.EnvVar(self.api_key_env_var))
        return dg.Definitions(resources={self.resource_key: resource})
