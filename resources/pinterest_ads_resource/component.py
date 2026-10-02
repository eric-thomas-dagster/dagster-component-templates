"""Pinterest Ads Resource component."""
import os
from typing import Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class PinterestAdsResource(ConfigurableResource):
    """Dagster resource for the Pinterest Ads API (OAuth 2.0 bearer token)."""

    access_token_env_var: str = Field(
        description="Env var holding the Pinterest OAuth 2.0 access token (needs ads:read + ads:write scopes for activation)"
    )
    ad_account_id: str = Field(description="Pinterest Ad Account ID")
    app_id_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Pinterest App ID (optional, only needed for token refresh)",
    )
    app_secret_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Pinterest App Secret (optional, only needed for token refresh)",
    )
    api_base_url: str = Field(
        default="https://api.pinterest.com/v5",
        description="Pinterest Ads API base URL (override for a different API version).",
    )

    def get_headers(self) -> dict:
        access_token = os.environ.get(self.access_token_env_var)
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }


class PinterestAdsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PinterestAdsResource for use by other components."""

    resource_key: str = Field(
        default="pinterest_ads_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    access_token_env_var: str = Field(
        description="Env var holding the Pinterest OAuth 2.0 access token (needs ads:read + ads:write scopes for activation)",
    )
    ad_account_id: str = Field(
        description="Pinterest Ad Account ID",
    )
    app_id_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Pinterest App ID (optional, only needed for token refresh)",
    )
    app_secret_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Pinterest App Secret (optional, only needed for token refresh)",
    )
    api_base_url: str = Field(
        default="https://api.pinterest.com/v5",
        description="Pinterest Ads API base URL (override for a different API version).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = PinterestAdsResource(
            access_token_env_var=self.access_token_env_var,
            ad_account_id=self.ad_account_id,
            app_id_env_var=self.app_id_env_var,
            app_secret_env_var=self.app_secret_env_var,
            api_base_url=self.api_base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
