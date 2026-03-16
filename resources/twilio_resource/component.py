"""Twilio Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from pydantic import Field


@dataclass
class TwilioResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-twilio TwilioResource for use by other components."""

    resource_key: str = Field(default="twilio_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    account_sid_env_var: str = Field(description="Environment variable holding the Twilio Account SID")
    auth_token_env_var: str = Field(description="Environment variable holding the Twilio Auth Token")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_twilio import TwilioResource
        resource = TwilioResource(
            account_sid=os.environ.get(self.account_sid_env_var, ""),
            auth_token=os.environ.get(self.auth_token_env_var, ""),
        )
        return dg.Definitions(resources={self.resource_key: resource})
