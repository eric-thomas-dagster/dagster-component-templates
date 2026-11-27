"""Slack Resource component."""
import json
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


def _parse_connection_json(value: str) -> Dict[str, Optional[str]]:
    """Parse a JSON blob ('{"token": "xoxb-..."}' or '{"webhook_url": "https://..."}')
    for Slack credentials. A URI shape doesn't fit Slack auth well (it's one opaque
    bot token OR one webhook URL, neither of which maps naturally onto
    scheme://login:password@host), so this resource supports a JSON blob instead of
    the Airflow-style connection URI the other resources in this repo use."""
    data = json.loads(value)
    return {"token": data.get("token"), "webhook_url": data.get("webhook_url")}


class SlackPostResource(dg.ConfigurableResource):
    """Post messages to Slack -- via a bot token (real `chat.postMessage` Web API call,
    same `slack_sdk.WebClient` convention this repo's `slack_reply`/`slack_chat_tools`
    already use) or via an incoming-webhook URL. Also exposes `get_client()` for anything
    that needs the raw `slack_sdk.WebClient`, same as `dagster_slack.SlackResource`."""

    token: Optional[str] = None
    webhook_url: Optional[str] = None

    def get_client(self):
        """Returns a real `slack_sdk.WebClient` built from the configured token."""
        from slack_sdk import WebClient
        if not self.token:
            raise ValueError("slack_resource: get_client() requires a bot token; none configured.")
        return WebClient(token=self.token)

    def post_message(
        self,
        channel: Optional[str] = None,
        text: Optional[str] = None,
        blocks: Optional[Any] = None,
        webhook_url: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Post one message. Uses `webhook_url` (param, falling back to the configured one) if
        set, else the real Slack `chat.postMessage` Web API via a `slack_sdk.WebClient` built
        from the configured bot token. `channel` is required for the token path. Modern,
        app-based incoming webhooks are bound to one fixed channel and have no JSON-payload
        channel override (that only existed for Slack's now-deprecated legacy webhooks), so
        `channel` must be omitted on the webhook path -- passing it raises rather than
        silently having no effect."""
        url = webhook_url or self.webhook_url
        if url:
            if channel:
                raise ValueError(
                    "slack_resource: `channel` has no effect when posting via an incoming "
                    "webhook -- modern Slack app-based webhooks are bound to one fixed channel "
                    "and do not support a per-request channel override (that capability only "
                    "existed for Slack's legacy webhooks, which are deprecated). Omit `channel`, "
                    "or post via a bot token (chat.postMessage) instead."
                )
            import requests
            payload: Dict[str, Any] = {}
            if text is not None:
                payload["text"] = text
            if blocks is not None:
                payload["blocks"] = blocks
            payload.update(kwargs)
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code != 200 or resp.text != "ok":
                raise RuntimeError(f"Slack webhook post failed: {resp.status_code} {resp.text!r}")
            return {"ok": True, "via": "webhook"}

        if not channel:
            raise ValueError("slack_resource: post_message() requires `channel` when posting via a bot token.")
        client = self.get_client()
        resp = client.chat_postMessage(channel=channel, text=text, blocks=blocks, **kwargs)
        if not resp.get("ok", True) or resp.get("error"):
            raise RuntimeError(f"Slack chat_postMessage failed: {resp.get('error')}")
        return dict(resp.data) if hasattr(resp, "data") else dict(resp)


class SlackResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SlackPostResource (bot-token `chat.postMessage`, or an incoming-webhook URL)
    for use by other components, ops, or assets. `get_client()` still returns a real
    `slack_sdk.WebClient`, same contract as the official `dagster_slack.SlackResource`."""

    resource_key: str = Field(default="slack_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    token_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Slack bot token (xoxb-...). Omit if using webhook_url_env_var instead.")
    webhook_url_env_var: Optional[str] = Field(default=None, description="Environment variable holding a Slack incoming-webhook URL. Omit if using token_env_var instead.")
    connection_uri_env_var: Optional[str] = Field(default=None, description="Env var holding a JSON blob ('{\"token\": \"xoxb-...\"}' or '{\"webhook_url\": \"https://...\"}') as an alternative to token_env_var/webhook_url_env_var. A Slack bot token or webhook URL doesn't map onto an Airflow-style connection URI the way other resources' fields do, so this is JSON rather than a URI. Explicit token_env_var/webhook_url_env_var, if also set, win over the blob's corresponding value.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Slack Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        token: Any = dg.EnvVar(self.token_env_var) if self.token_env_var else None
        webhook_url: Any = dg.EnvVar(self.webhook_url_env_var) if self.webhook_url_env_var else None
        if self.connection_uri_env_var:
            blob_value = dg.EnvVar(self.connection_uri_env_var).get_value()
            if blob_value:
                parsed = _parse_connection_json(blob_value)
                if token is None and parsed["token"]:
                    token = parsed["token"]
                if webhook_url is None and parsed["webhook_url"]:
                    webhook_url = parsed["webhook_url"]
        resource = SlackPostResource(token=token, webhook_url=webhook_url)
        return dg.Definitions(resources={self.resource_key: resource})
