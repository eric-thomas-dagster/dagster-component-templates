"""TikTok Ads Resource component."""
import os

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class TikTokAdsResource(ConfigurableResource):
    """Dagster resource for the TikTok Marketing API."""

    access_token_env_var: str = Field(description="Env var holding the TikTok Marketing API access token")
    advertiser_id: str = Field(description="TikTok Advertiser ID")
    api_base_url: str = Field(
        default="https://business-api.tiktok.com/open_api/v1.3",
        description="TikTok Marketing API base URL (override for a different API version).",
    )

    def get_headers(self) -> dict:
        access_token = os.environ.get(self.access_token_env_var)
        return {
            "Access-Token": access_token,
            "Content-Type": "application/json",
        }


class TikTokAdsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a TikTokAdsResource for use by other components."""

    resource_key: str = Field(
        default="tiktok_ads_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    access_token_env_var: str = Field(
        description="Env var holding the TikTok Marketing API access token",
    )
    advertiser_id: str = Field(
        description="TikTok Advertiser ID",
    )
    api_base_url: str = Field(
        default="https://business-api.tiktok.com/open_api/v1.3",
        description="TikTok Marketing API base URL (override for a different API version).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = TikTokAdsResource(
            access_token_env_var=self.access_token_env_var,
            advertiser_id=self.advertiser_id,
            api_base_url=self.api_base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
