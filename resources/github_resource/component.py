"""GitHub Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from pydantic import Field


@dataclass
class GithubResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-github GithubResource for use by other components."""

    resource_key: str = Field(default="github_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    github_app_id: int = Field(description="GitHub App ID (numeric)")
    github_app_private_rsa_key_env_var: str = Field(description="Environment variable holding the GitHub App private RSA key (PEM format)")
    github_installation_id: int = Field(description="GitHub App installation ID (numeric)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_github import GithubResource
        resource = GithubResource(
            github_app_id=self.github_app_id,
            github_app_private_rsa_key=os.environ.get(self.github_app_private_rsa_key_env_var, ""),
            github_installation_id=self.github_installation_id,
        )
        return dg.Definitions(resources={self.resource_key: resource})
