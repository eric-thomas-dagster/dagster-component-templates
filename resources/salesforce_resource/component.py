"""Salesforce Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class SalesforceResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-salesforce SalesforceResource for use by other components."""

    resource_key: str = Field(default="salesforce_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    username: str = Field(description="Salesforce username (email)")
    password_env_var: str = Field(description="Environment variable holding the Salesforce password")
    security_token_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Salesforce security token (appended to password for IP-restricted orgs)")
    domain: Optional[str] = Field(default=None, description="Salesforce domain: 'test' for sandbox, 'login' for production (default), or custom domain")
    consumer_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Connected App consumer key (for OAuth)")
    consumer_secret_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Connected App consumer secret (for OAuth)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_salesforce import SalesforceResource
        resource = SalesforceResource(
            username=self.username,
            password=os.environ.get(self.password_env_var, ""),
            security_token=os.environ.get(self.security_token_env_var, "") if self.security_token_env_var else "",
            domain=self.domain,
            consumer_key=os.environ.get(self.consumer_key_env_var) if self.consumer_key_env_var else None,
            consumer_secret=os.environ.get(self.consumer_secret_env_var) if self.consumer_secret_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: resource})
