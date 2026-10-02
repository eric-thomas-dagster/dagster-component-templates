"""NetSuite Resource.

Wraps NetSuite's SuiteTalk REST Web Services (record/v1) API. NetSuite
account-specific REST base URLs look like:
  https://<account-id>.suitetalk.api.netsuite.com/services/rest/record/v1

Auth: OAuth 1.0a (Token-Based Authentication / TBA) -- NOT OAuth2. This is a
common point of confusion: NetSuite's REST API still signs every request
with a consumer key/secret + token id/secret pair via HMAC-SHA256, exactly
like SOAP-era TBA. There is no bearer token, no client-credentials grant,
and no expiring access token to refresh -- the token id/secret pair is
long-lived (revoked manually in NetSuite, not time-expired).

Required NetSuite-side setup:
  1. Enable "Token-Based Authentication" on an Integration record -- this
     yields a Consumer Key + Consumer Secret.
  2. Create an Access Token (scoped to a User + Role + that Integration) --
     this yields a Token ID + Token Secret.

Signing uses `requests_oauthlib.OAuth1` rather than hand-rolled HMAC --
NetSuite requires `signature_method=HMAC-SHA256` (oauthlib's default is
HMAC-SHA1, which NetSuite rejects unless the Integration record is
configured to allow it; HMAC-SHA256 is the current NetSuite-recommended
default) and the account id passed as the OAuth `realm`.
"""
import os
from typing import Any, Optional

import dagster as dg
from pydantic import Field


def _account_id_to_hostname(account_id: str) -> str:
    """NetSuite REST hostnames lowercase the account id and replace '_'
    with '-' (e.g. sandbox account '1234567_SB1' -> '1234567-sb1')."""
    return account_id.strip().lower().replace("_", "-")


class NetSuiteResource(dg.ConfigurableResource):
    """NetSuite SuiteTalk REST (record/v1) API client wrapper. OAuth 1.0a
    (Token-Based Authentication), HMAC-SHA256 signed."""

    account_id: str = Field(
        description="NetSuite account id, e.g. '1234567' or '1234567_SB1' (sandbox)."
    )
    consumer_key_env_var: str = Field(description="Env var with OAuth1 Consumer Key")
    consumer_secret_env_var: str = Field(description="Env var with OAuth1 Consumer Secret")
    token_id_env_var: str = Field(description="Env var with OAuth1 Token ID")
    token_secret_env_var: str = Field(description="Env var with OAuth1 Token Secret")

    def _base_url(self) -> str:
        host = _account_id_to_hostname(self.account_id)
        return f"https://{host}.suitetalk.api.netsuite.com/services/rest/record/v1"

    def _auth(self):
        from requests_oauthlib import OAuth1

        consumer_key = os.environ.get(self.consumer_key_env_var)
        consumer_secret = os.environ.get(self.consumer_secret_env_var)
        token_id = os.environ.get(self.token_id_env_var)
        token_secret = os.environ.get(self.token_secret_env_var)
        if not all([consumer_key, consumer_secret, token_id, token_secret]):
            raise RuntimeError("Missing NetSuite OAuth1 (TBA) env vars")

        return OAuth1(
            client_key=consumer_key,
            client_secret=consumer_secret,
            resource_owner_key=token_id,
            resource_owner_secret=token_secret,
            signature_method="HMAC-SHA256",
            realm=self.account_id,
        )

    def request(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> Optional[Any]:
        """Generic signed request against record/v1. Returns the parsed
        JSON body, or None for 204 No Content responses (NetSuite PATCH/
        DELETE typically return 204)."""
        import requests

        url = f"{self._base_url()}/{path.lstrip('/')}"
        resp = requests.request(
            method,
            url,
            json=json_body,
            params=params,
            auth=self._auth(),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=60,
        )
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()


class NetSuiteResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a NetSuite SuiteTalk REST resource (OAuth 1.0a / TBA)."""

    resource_key: str = Field(default="netsuite", description="Dagster resource key")
    account_id: str = Field(description="NetSuite account id, e.g. '1234567' or '1234567_SB1'")
    consumer_key_env_var: str = Field(description="Env var with OAuth1 Consumer Key")
    consumer_secret_env_var: str = Field(description="Env var with OAuth1 Consumer Secret")
    token_id_env_var: str = Field(description="Env var with OAuth1 Token ID")
    token_secret_env_var: str = Field(description="Env var with OAuth1 Token Secret")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(resources={self.resource_key: NetSuiteResource(
            account_id=self.account_id,
            consumer_key_env_var=self.consumer_key_env_var,
            consumer_secret_env_var=self.consumer_secret_env_var,
            token_id_env_var=self.token_id_env_var,
            token_secret_env_var=self.token_secret_env_var,
        )})
