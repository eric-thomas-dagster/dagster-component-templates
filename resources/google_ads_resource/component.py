"""Google Ads Resource component."""
import os
from typing import Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value



class GoogleAdsResource(ConfigurableResource):
    """Dagster resource wrapping the official `google-ads` Python client."""

    developer_token_env_var: str = Field(description="Env var holding your Google Ads developer token")
    client_id_env_var: str = Field(description="Env var holding the OAuth2 client ID")
    client_secret_env_var: str = Field(description="Env var holding the OAuth2 client secret")
    refresh_token_env_var: str = Field(description="Env var holding the OAuth2 refresh token")
    customer_id: str = Field(
        description="Target Google Ads account ID, 10 digits, no dashes (e.g. '1234567890')"
    )
    login_customer_id: Optional[str] = Field(
        default=None,
        description=(
            "Manager (MCC) account ID, 10 digits, no dashes. Required only when "
            "`customer_id` is managed under an MCC and the OAuth2 credentials "
            "are the MCC's, not the client account's."
        ),
    )

    def get_client(self):
        from google.ads.googleads.client import GoogleAdsClient

        config = {
            "developer_token": _require_env(self.developer_token_env_var),
            "client_id": _require_env(self.client_id_env_var),
            "client_secret": _require_env(self.client_secret_env_var),
            "refresh_token": _require_env(self.refresh_token_env_var),
            "use_proto_plus": True,
        }
        if self.login_customer_id:
            config["login_customer_id"] = self.login_customer_id
        return GoogleAdsClient.load_from_dict(config)


class GoogleAdsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a GoogleAdsResource for use by other components."""

    resource_key: str = Field(
        default="google_ads_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    developer_token_env_var: str = Field(
        description="Env var holding your Google Ads developer token",
    )
    client_id_env_var: str = Field(
        description="Env var holding the OAuth2 client ID",
    )
    client_secret_env_var: str = Field(
        description="Env var holding the OAuth2 client secret",
    )
    refresh_token_env_var: str = Field(
        description="Env var holding the OAuth2 refresh token",
    )
    customer_id: str = Field(
        description="Target Google Ads account ID, 10 digits, no dashes",
    )
    login_customer_id: Optional[str] = Field(
        default=None,
        description="Manager (MCC) account ID, 10 digits, no dashes. Optional.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = GoogleAdsResource(
            developer_token_env_var=self.developer_token_env_var,
            client_id_env_var=self.client_id_env_var,
            client_secret_env_var=self.client_secret_env_var,
            refresh_token_env_var=self.refresh_token_env_var,
            customer_id=self.customer_id,
            login_customer_id=self.login_customer_id,
        )
        return dg.Definitions(resources={self.resource_key: resource})
