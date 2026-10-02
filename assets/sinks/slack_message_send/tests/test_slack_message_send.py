"""Committed regression tests for SlackMessageSendComponent.

The real Slack Web API call is never made here -- `_post_slack_message`
(the one external boundary) is monkeypatched wholesale, while template
rendering, mode handling, validation, auth-path selection, and per-row
failure isolation are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeSlackResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_post(client, channel, text, thread_ts=None):
        calls.append({"client": client, "channel": channel, "text": text, "thread_ts": thread_ts})
        return {"ok": True, "ts": f"169999{len(calls)}.000100"}

    monkeypatch.setattr(mod, "_post_slack_message", _fake_post)
    return calls


def _materialize(component, upstream_df, resource_key="slack_resource", resource=None):
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    resources = {resource_key: resource} if resource is not None else {}
    return dg.materialize([asset_def, upstream_asset], resources=resources)


# --- template rendering (pure) -------------------------------------------

def test_render_substitutes_columns(mod):
    out = mod._render("{customer_name} crossed {health_score}", {"customer_name": "Acme", "health_score": 42})
    assert out == "Acme crossed 42"


def test_render_missing_column_renders_empty(mod):
    out = mod._render("{customer_name} - {missing_col}", {"customer_name": "Acme"})
    assert out == "Acme - "


# --- validation ------------------------------------------------------------

def test_neither_resource_key_nor_token_env_var_raises(mod):
    with pytest.raises(ValueError, match="resource_key.*OR token_env_var"):
        mod.SlackMessageSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            channel_template="#general",
            message_template="hi",
        ).build_defs(context=None)


def test_unknown_mode_raises(mod, recorded_calls):
    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.SlackMessageSendComponent(
        asset_name="slack_out",
        upstream_asset_key="upstream_customers",
        resource_key="slack_resource",
        channel_template="#general",
        message_template="hi",
        mode="bogus",
    )
    with pytest.raises(Exception, match="unknown mode"):
        _materialize(component, df, resource=FakeSlackResource())


# --- end-to-end send behavior, against the monkeypatched API call ---------

def test_per_row_send_end_to_end(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "customer_name": ["Acme", "Globex"],
            "health_score": [42, 58],
        }
    )
    component = mod.SlackMessageSendComponent(
        asset_name="slack_out",
        upstream_asset_key="upstream_customers",
        resource_key="slack_resource",
        channel_template="#customer-success",
        message_template="{customer_name} crossed {health_score}",
    )
    result = _materialize(component, df, resource=FakeSlackResource())
    assert result.success

    assert len(recorded_calls) == 2
    assert recorded_calls[0]["channel"] == "#customer-success"
    assert recorded_calls[0]["text"] == "Acme crossed 42"
    assert recorded_calls[1]["text"] == "Globex crossed 58"

    out = metadata_for(result, "slack_out")
    assert out["messages_sent"] == 2
    assert out["messages_failed"] == 0
    assert out["dry_run"] is False


def test_per_row_channel_routing(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "customer_name": ["Acme", "Globex"],
            "slack_channel": ["#region-us", "#region-eu"],
        }
    )
    component = mod.SlackMessageSendComponent(
        asset_name="slack_out",
        upstream_asset_key="upstream_customers",
        resource_key="slack_resource",
        channel_template="{slack_channel}",
        message_template="{customer_name}",
    )
    _materialize(component, df, resource=FakeSlackResource())
    assert recorded_calls[0]["channel"] == "#region-us"
    assert recorded_calls[1]["channel"] == "#region-eu"


def test_thread_ts_column_used(mod, recorded_calls):
    df = pd.DataFrame({"customer_name": ["Acme"], "parent_thread_ts": ["1699999999.0001"]})
    component = mod.SlackMessageSendComponent(
        asset_name="slack_out",
        upstream_asset_key="upstream_customers",
        resource_key="slack_resource",
        channel_template="#general",
        message_template="{customer_name}",
        thread_ts_column="parent_thread_ts",
    )
    _materialize(component, df, resource=FakeSlackResource())
    assert recorded_calls[0]["thread_ts"] == "1699999999.0001"


def test_summary_mode(mod, recorded_calls):
    df = pd.DataFrame({"customer_name": ["Acme", "Globex", "Initech"]})
    component = mod.SlackMessageSendComponent(
        asset_name="slack_out",
        upstream_asset_key="upstream_customers",
        resource_key="slack_resource",
        mode="summary",
        channel_template="#customer-success",
        summary_template="{row_count} customers crossed the threshold today.",
    )
    result = _materialize(component, df, resource=FakeSlackResource())
    assert result.success
    assert len(recorded_calls) == 1
    assert recorded_calls[0]["text"] == "3 customers crossed the threshold today."
    out = metadata_for(result, "slack_out")
    assert out["messages_sent"] == 1


def test_dry_run_sends_nothing(mod, recorded_calls):
    df = pd.DataFrame({"customer_name": ["Acme", "Globex"]})
    component = mod.SlackMessageSendComponent(
        asset_name="slack_out",
        upstream_asset_key="upstream_customers",
        resource_key="slack_resource",
        channel_template="#general",
        message_template="{customer_name}",
        dry_run=True,
    )
    result = _materialize(component, df, resource=FakeSlackResource())
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "slack_out")
    assert out["messages_sent"] == 2
    assert out["dry_run"] is True


def test_max_send_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"customer_name": [f"c{i}" for i in range(10)]})
    component = mod.SlackMessageSendComponent(
        asset_name="slack_out",
        upstream_asset_key="upstream_customers",
        resource_key="slack_resource",
        channel_template="#general",
        message_template="{customer_name}",
        max_send=3,
    )
    result = _materialize(component, df, resource=FakeSlackResource())
    assert result.success
    assert len(recorded_calls) == 3
    out = metadata_for(result, "slack_out")
    assert out["messages_sent"] == 3


def test_missing_channel_counted_as_failed(mod, recorded_calls):
    df = pd.DataFrame({"customer_name": ["Acme"], "slack_channel": [None]})
    component = mod.SlackMessageSendComponent(
        asset_name="slack_out",
        upstream_asset_key="upstream_customers",
        resource_key="slack_resource",
        channel_template="{slack_channel}",
        message_template="{customer_name}",
    )
    result = _materialize(component, df, resource=FakeSlackResource())
    assert result.success  # a missing channel must not crash the whole run
    assert recorded_calls == []
    out = metadata_for(result, "slack_out")
    assert out["messages_sent"] == 0
    assert out["messages_failed"] == 1


# --- per-row failure isolation --------------------------------------------

def test_one_row_failing_does_not_abort_run(mod, monkeypatch):
    def _flaky_post(client, channel, text, thread_ts=None):
        if "Globex" in text:
            raise RuntimeError("channel_not_found")
        return {"ok": True}

    monkeypatch.setattr(mod, "_post_slack_message", _flaky_post)

    df = pd.DataFrame({"customer_name": ["Acme", "Globex", "Initech"]})
    component = mod.SlackMessageSendComponent(
        asset_name="slack_out",
        upstream_asset_key="upstream_customers",
        resource_key="slack_resource",
        channel_template="#general",
        message_template="{customer_name}",
    )
    result = _materialize(component, df, resource=FakeSlackResource())
    assert result.success

    out = metadata_for(result, "slack_out")
    assert out["messages_sent"] == 2
    assert out["messages_failed"] == 1
    assert "first_errors" in out


# --- auth-path selection ----------------------------------------------------

def test_token_env_var_fallback_builds_real_webclient(mod, recorded_calls, monkeypatch):
    """When resource_key is not set, the component should build a real
    slack_sdk.WebClient from token_env_var -- its constructor makes no
    network call, and _post_slack_message is still the monkeypatched
    boundary, so this exercises the fallback auth path for real."""
    monkeypatch.setenv("MY_SLACK_TOKEN", "xoxb-fake-token")
    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.SlackMessageSendComponent(
        asset_name="slack_out",
        upstream_asset_key="upstream_customers",
        token_env_var="MY_SLACK_TOKEN",
        channel_template="#general",
        message_template="{customer_name}",
    )
    result = _materialize(component, df, resource_key="slack_resource", resource=None)
    assert result.success
    assert len(recorded_calls) == 1
    # the client passed through is a real slack_sdk.WebClient instance
    from slack_sdk.web.client import WebClient
    assert isinstance(recorded_calls[0]["client"], WebClient)
