"""Facebook Ads Resource component."""
from dataclasses import dataclass
import os
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class FacebookAdsResource(ConfigurableResource):
    """Dagster resource wrapping the Facebook Business SDK."""

    app_id_env_var: str = Field(description="Env var holding Facebook App ID")
    app_secret_env_var: str = Field(description="Env var holding Facebook App Secret")
    access_token_env_var: str = Field(description="Env var holding long-lived access token")
    ad_account_id: str = Field(description="Facebook Ad Account ID e.g. 'act_123456789'")

    def get_api(self):
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.adaccount import AdAccount

        app_id = os.environ.get(self.app_id_env_var, "")
        app_secret = os.environ.get(self.app_secret_env_var, "")
        access_token = os.environ.get(self.access_token_env_var, "")

        FacebookAdsApi.init(app_id, app_secret, access_token)
        return AdAccount(self.ad_account_id)


@dataclass
class FacebookAdsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a FacebookAdsResource for use by other components."""

    resource_key: str = Field(
        default="facebook_ads_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    app_id_env_var: str = Field(
        description="Env var holding Facebook App ID",
    )
    app_secret_env_var: str = Field(
        description="Env var holding Facebook App Secret",
    )
    access_token_env_var: str = Field(
        description="Env var holding long-lived access token",
    )
    ad_account_id: str = Field(
        description="Facebook Ad Account ID e.g. 'act_123456789'",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = FacebookAdsResource(
            app_id_env_var=self.app_id_env_var,
            app_secret_env_var=self.app_secret_env_var,
            access_token_env_var=self.access_token_env_var,
            ad_account_id=self.ad_account_id,
        )
        return dg.Definitions(resources={self.resource_key: resource})
