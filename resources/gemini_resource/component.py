"""Google Gemini Resource component."""
from dataclasses import dataclass
import dagster as dg
from pydantic import Field


@dataclass
class GeminiResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-gemini GeminiResource for use by other components."""

    resource_key: str = Field(default="gemini_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    api_key_env_var: str = Field(default="GEMINI_API_KEY", description="Environment variable holding the Google Gemini API key")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_gemini import GeminiResource
        resource = GeminiResource(api_key=dg.EnvVar(self.api_key_env_var))
        return dg.Definitions(resources={self.resource_key: resource})
