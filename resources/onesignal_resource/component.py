"""OneSignal Resource component.

Registers a thin OneSignal REST API resource (app id + REST API key auth)
for use by other components. OneSignal has no official first-party Python
SDK with Dagster bindings, so this wraps the plain REST API directly —
`POST https://api.onesignal.com/notifications` — mirroring this repo's
other lightweight REST resources (e.g. ``tiktok_ads_resource``).

Auth: `Authorization: Key <REST_API_KEY>` (current host/scheme, confirmed
2026 — OneSignal migrated from the legacy `onesignal.com/api/v1/...` host
+ `include_player_ids` targeting field to `api.onesignal.com/...` +
`include_subscription_ids`/`include_aliases`).
"""
import os

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class OneSignalResource(ConfigurableResource):
    """Dagster resource for the OneSignal REST API (Notifications endpoint)."""

    app_id: str = Field(description="OneSignal App ID.")
    rest_api_key_env_var: str = Field(
        description="Env var holding the OneSignal REST API Key (Settings -> Keys & IDs)."
    )
    api_base_url: str = Field(
        default="https://api.onesignal.com",
        description="OneSignal REST API base URL (override for a different region/version).",
    )

    def get_headers(self) -> dict:
        rest_api_key = os.environ.get(self.rest_api_key_env_var)
        return {
            "Authorization": f"Key {rest_api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }


class OneSignalResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a OneSignalResource for use by other components."""

    resource_key: str = Field(
        default="onesignal_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    app_id: str = Field(description="OneSignal App ID.")
    rest_api_key_env_var: str = Field(
        description="Env var holding the OneSignal REST API Key (Settings -> Keys & IDs).",
    )
    api_base_url: str = Field(
        default="https://api.onesignal.com",
        description="OneSignal REST API base URL (override for a different region/version).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = OneSignalResource(
            app_id=self.app_id,
            rest_api_key_env_var=self.rest_api_key_env_var,
            api_base_url=self.api_base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
