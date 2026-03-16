"""Jira Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class JiraResource(ConfigurableResource):
    """Dagster resource wrapping the Jira Python client."""

    server_url: str = Field(description="Jira server URL e.g. https://mycompany.atlassian.net")
    username: str = Field(description="Jira username (email)")
    api_token: str = Field(description="Jira API token")

    def get_client(self):
        from jira import JIRA
        return JIRA(
            server=self.server_url,
            basic_auth=(self.username, self.api_token),
        )


@dataclass
class JiraResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a JiraResource for use by other components."""

    resource_key: str = Field(
        default="jira_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    server_url: str = Field(
        description="Jira server URL e.g. https://mycompany.atlassian.net",
    )
    username: str = Field(
        description="Jira username (email)",
    )
    api_token_env_var: str = Field(
        description="Env var holding Jira API token",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = JiraResource(
            server_url=self.server_url,
            username=self.username,
            api_token=os.environ.get(self.api_token_env_var, ""),
        )
        return dg.Definitions(resources={self.resource_key: resource})
