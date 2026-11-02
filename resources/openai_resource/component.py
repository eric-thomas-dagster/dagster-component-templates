"""OpenAI Resource component."""
import dagster as dg
from pydantic import Field


class OpenAIResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-openai OpenAIResource for use by other components."""

    resource_key: str = Field(default="openai_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Environment variable holding the OpenAI API key")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Open AI Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_openai import OpenAIResource
        resource = OpenAIResource(api_key=dg.EnvVar(self.api_key_env_var))
        return dg.Definitions(resources={self.resource_key: resource})
