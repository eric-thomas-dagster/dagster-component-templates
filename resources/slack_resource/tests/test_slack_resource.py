import pytest

from .conftest import run_probe


class FakeSlackResponse(dict):
    @property
    def data(self):
        return dict(self)


class FakeWebClient:
    instances = []

    def __init__(self, token):
        self.token = token
        self.calls = []
        FakeWebClient.instances.append(self)

    def chat_postMessage(self, **kwargs):
        self.calls.append(kwargs)
        return FakeSlackResponse(ok=True, channel=kwargs.get("channel"))


def test_token_path_calls_real_chat_post_message(mod, monkeypatch):
    import slack_sdk
    FakeWebClient.instances = []
    monkeypatch.setattr(slack_sdk, "WebClient", FakeWebClient)
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-fake")

    component = mod.SlackResourceComponent(token_env_var="SLACK_TOKEN")
    result = run_probe(mod, component, {"channel": "#noise", "text": "hello"})

    assert result["ok"] is True
    client = FakeWebClient.instances[-1]
    assert client.token == "xoxb-fake"
    assert client.calls[0]["channel"] == "#noise"
    assert client.calls[0]["text"] == "hello"


def test_webhook_path_posts_json(mod, monkeypatch):
    posted = {}

    class FakeResponse:
        status_code = 200
        text = "ok"

    def fake_post(url, json=None, timeout=None):
        posted["url"] = url
        posted["json"] = json
        return FakeResponse()

    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setenv("SLACK_WEBHOOK", "https://hooks.slack.com/services/T/B/X")

    component = mod.SlackResourceComponent(webhook_url_env_var="SLACK_WEBHOOK")
    result = run_probe(mod, component, {"text": "hello via webhook"})

    assert result == {"ok": True, "via": "webhook"}
    assert posted["url"] == "https://hooks.slack.com/services/T/B/X"
    assert posted["json"] == {"text": "hello via webhook"}


def test_webhook_failure_raises(mod, monkeypatch):
    class FakeResponse:
        status_code = 500
        text = "server_error"

    monkeypatch.setattr("requests.post", lambda *a, **k: FakeResponse())
    monkeypatch.setenv("SLACK_WEBHOOK", "https://hooks.slack.com/services/T/B/X")

    component = mod.SlackResourceComponent(webhook_url_env_var="SLACK_WEBHOOK")
    with pytest.raises(Exception, match="Slack webhook post failed"):
        run_probe(mod, component, {"text": "hello"})


def test_token_path_without_channel_raises(mod, monkeypatch):
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-fake")
    component = mod.SlackResourceComponent(token_env_var="SLACK_TOKEN")
    with pytest.raises(Exception, match="requires `channel`"):
        run_probe(mod, component, {"text": "hello"})


def test_get_client_returns_real_webclient_type(mod, monkeypatch):
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-fake")
    component = mod.SlackResourceComponent(token_env_var="SLACK_TOKEN")
    defs = component.build_defs(context=None)
    resource = defs.resources["slack_resource"]
    client = resource.get_client()
    from slack_sdk import WebClient
    assert isinstance(client, WebClient)


def test_component_registers_resource(mod):
    component = mod.SlackResourceComponent(token_env_var="SLACK_TOKEN")
    defs = component.build_defs(context=None)
    assert isinstance(defs.resources["slack_resource"], mod.SlackPostResource)
