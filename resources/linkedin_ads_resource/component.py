"""LinkedIn Ads Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class LinkedInAdsResource(ConfigurableResource):
    """Dagster resource for the LinkedIn Marketing API."""

    access_token_env_var: str = Field(
        description="Env var holding LinkedIn OAuth2 access token"
    )
    ad_account_id: Optional[str] = Field(
        default=None,
        description="LinkedIn Ad Account ID",
    )

    def get_headers(self) -> dict:
        access_token = os.environ.get(self.access_token_env_var, "")
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }


@dataclass
class LinkedInAdsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a LinkedInAdsResource for use by other components."""

    resource_key: str = Field(
        default="linkedin_ads_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    access_token_env_var: str = Field(
        description="Env var holding LinkedIn OAuth2 access token",
    )
    ad_account_id: Optional[str] = Field(
        default=None,
        description="LinkedIn Ad Account ID",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = LinkedInAdsResource(
            access_token_env_var=self.access_token_env_var,
            ad_account_id=self.ad_account_id,
        )
        return dg.Definitions(resources={self.resource_key: resource})
