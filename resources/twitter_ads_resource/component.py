"""Twitter/X Ads Resource component.

Wraps the X (Twitter) Ads API's OAuth 1.0a authentication. The Ads API
still requires OAuth 1.0a (consumer key/secret + access token/secret) --
unlike most of X's newer v2 APIs, which moved to OAuth 2.0.
"""
import base64
import hashlib
import hmac
import os
import secrets
import time
import urllib.parse
from typing import Dict, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class TwitterAdsResource(ConfigurableResource):
    """Dagster resource for the X (Twitter) Ads API (OAuth 1.0a)."""

    consumer_key_env_var: str = Field(description="Env var holding the X Ads API Consumer Key (API Key)")
    consumer_secret_env_var: str = Field(description="Env var holding the X Ads API Consumer Secret (API Secret)")
    access_token_env_var: str = Field(description="Env var holding the X Ads API Access Token")
    access_token_secret_env_var: str = Field(description="Env var holding the X Ads API Access Token Secret")
    account_id: str = Field(description="X Ads Account ID (not the numeric X/Twitter user ID)")
    api_base_url: str = Field(
        default="https://ads-api.x.com/12",
        description=(
            "X Ads API base URL (override for a different API version). "
            "The legacy 'https://ads-api.twitter.com/12' host also still works."
        ),
    )

    def get_oauth1_header(self, method: str, url: str, extra_params: Optional[Dict[str, str]] = None) -> str:
        """Builds an OAuth 1.0a `Authorization` header for one request.

        Only query-string parameters are included in the signature base
        string (not JSON body fields) -- this matches how the X Ads API's
        own client libraries sign JSON-bodied requests: OAuth 1.0a's
        percent-encoding scheme is defined for form/query parameters, not
        arbitrary JSON, so JSON POST bodies are sent unsigned alongside a
        signature computed from the URL + oauth_* params (+ any querystring
        params) alone.
        """
        consumer_key = os.environ.get(self.consumer_key_env_var, "")
        consumer_secret = os.environ.get(self.consumer_secret_env_var, "")
        access_token = os.environ.get(self.access_token_env_var, "")
        access_token_secret = os.environ.get(self.access_token_secret_env_var, "")

        oauth_params = {
            "oauth_consumer_key": consumer_key,
            "oauth_token": access_token,
            "oauth_signature_method": "HMAC-SHA1",
            "oauth_timestamp": str(int(time.time())),
            "oauth_nonce": secrets.token_hex(16),
            "oauth_version": "1.0",
        }

        all_params = dict(oauth_params)
        if extra_params:
            all_params.update(extra_params)

        sorted_params = sorted(all_params.items())
        param_string = "&".join(
            f"{urllib.parse.quote(str(k), safe='')}={urllib.parse.quote(str(v), safe='')}"
            for k, v in sorted_params
        )
        base_string = "&".join(
            [
                method.upper(),
                urllib.parse.quote(url, safe=""),
                urllib.parse.quote(param_string, safe=""),
            ]
        )
        signing_key = (
            f"{urllib.parse.quote(consumer_secret, safe='')}&{urllib.parse.quote(access_token_secret, safe='')}"
        )
        signature = base64.b64encode(
            hmac.new(signing_key.encode("utf-8"), base_string.encode("utf-8"), hashlib.sha1).digest()
        ).decode("utf-8")
        oauth_params["oauth_signature"] = signature

        return "OAuth " + ", ".join(
            f'{urllib.parse.quote(str(k), safe="")}="{urllib.parse.quote(str(v), safe="")}"'
            for k, v in sorted(oauth_params.items())
        )

    def get_headers(self, method: str, url: str) -> dict:
        return {
            "Authorization": self.get_oauth1_header(method, url),
            "Content-Type": "application/json",
        }


class TwitterAdsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a TwitterAdsResource for use by other components."""

    resource_key: str = Field(
        default="twitter_ads_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    consumer_key_env_var: str = Field(
        description="Env var holding the X Ads API Consumer Key (API Key)",
    )
    consumer_secret_env_var: str = Field(
        description="Env var holding the X Ads API Consumer Secret (API Secret)",
    )
    access_token_env_var: str = Field(
        description="Env var holding the X Ads API Access Token",
    )
    access_token_secret_env_var: str = Field(
        description="Env var holding the X Ads API Access Token Secret",
    )
    account_id: str = Field(
        description="X Ads Account ID (not the numeric X/Twitter user ID)",
    )
    api_base_url: str = Field(
        default="https://ads-api.x.com/12",
        description="X Ads API base URL (override for a different API version).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = TwitterAdsResource(
            consumer_key_env_var=self.consumer_key_env_var,
            consumer_secret_env_var=self.consumer_secret_env_var,
            access_token_env_var=self.access_token_env_var,
            access_token_secret_env_var=self.access_token_secret_env_var,
            account_id=self.account_id,
            api_base_url=self.api_base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
