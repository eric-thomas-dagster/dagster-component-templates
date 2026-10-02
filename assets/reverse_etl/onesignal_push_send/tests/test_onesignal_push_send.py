"""Committed regression tests for OneSignalPushSendComponent.

The real OneSignal REST call is never made here -- `_call_onesignal_api`
(the one external boundary) is monkeypatched wholesale, while template
rendering, dual source resolution, validation, batching, and per-row
failure isolation are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeOneSignalResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, subscription_id, heading, contents):
        calls.append(
            {
                "resource": resource,
                "subscription_id": subscription_id,
                "heading": heading,
                "contents": contents,
            }
        )
        return {"id": f"notif-{len(calls)}"}

    monkeypatch.setattr(mod, "_call_onesignal_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_users", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"onesignal_resource": resource})


# --- template rendering (pure) -------------------------------------------

def test_render_template_substitutes_columns(mod):
    out = mod._render_template("{first_name}, your cart has {item_count} items", {"first_name": "Jane", "item_count": 3})
    assert out == "Jane, your cart has 3 items"


def test_render_template_missing_column_renders_empty(mod):
    out = mod._render_template("{first_name} - {missing_col}", {"first_name": "Jane"})
    assert out == "Jane - "


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.OneSignalPushSendComponent(
            asset_name="x",
            recipient_column="subscription_id",
            message_template="hi",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.OneSignalPushSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            recipient_column="subscription_id",
            message_template="hi",
        ).build_defs(context=None)


def test_empty_message_template_raises(mod):
    with pytest.raises(ValueError, match="message_template must be non-empty"):
        mod.OneSignalPushSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            recipient_column="subscription_id",
            message_template="",
        ).build_defs(context=None)


def test_missing_recipient_column_raises_failure(mod, recorded_calls):
    df = pd.DataFrame({"other_col": ["a"]})
    resource = FakeOneSignalResource()
    component = mod.OneSignalPushSendComponent(
        asset_name="onesignal_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="subscription_id",
        message_template="hi",
    )
    with pytest.raises(dg.Failure, match="recipient_column"):
        _materialize(component, df, resource)


# --- end-to-end send behavior, against the monkeypatched API call ---------

def test_send_end_to_end_with_heading(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "subscription_id": ["sub1", "sub2"],
            "first_name": ["Jane", "Bob"],
            "item_count": [3, 1],
        }
    )
    resource = FakeOneSignalResource()
    component = mod.OneSignalPushSendComponent(
        asset_name="onesignal_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="subscription_id",
        heading_template="You left something behind!",
        message_template="{first_name}, your cart has {item_count} item(s) waiting.",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(recorded_calls) == 2
    assert recorded_calls[0]["subscription_id"] == "sub1"
    assert recorded_calls[0]["heading"] == "You left something behind!"
    assert recorded_calls[0]["contents"] == "Jane, your cart has 3 item(s) waiting."
    assert recorded_calls[1]["contents"] == "Bob, your cart has 1 item(s) waiting."

    out = metadata_for(result, "onesignal_push_out")
    assert out["messages_sent"] == 2
    assert out["messages_failed"] == 0


def test_send_without_heading_template(mod, recorded_calls):
    df = pd.DataFrame({"subscription_id": ["sub1"]})
    resource = FakeOneSignalResource()
    component = mod.OneSignalPushSendComponent(
        asset_name="onesignal_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="subscription_id",
        message_template="just a body",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls[0]["heading"] is None
    assert recorded_calls[0]["contents"] == "just a body"


def test_empty_upstream_sends_nothing(mod, recorded_calls):
    df = pd.DataFrame({"subscription_id": []})
    resource = FakeOneSignalResource()
    component = mod.OneSignalPushSendComponent(
        asset_name="onesignal_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="subscription_id",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "onesignal_push_out")
    assert out["messages_sent"] == 0


def test_blank_and_null_recipients_skipped_and_counted(mod, recorded_calls):
    df = pd.DataFrame({"subscription_id": ["sub1", None, "  ", ""]})
    resource = FakeOneSignalResource()
    component = mod.OneSignalPushSendComponent(
        asset_name="onesignal_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="subscription_id",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 1
    out = metadata_for(result, "onesignal_push_out")
    assert out["messages_sent"] == 1
    assert out["rows_skipped_no_recipient"] == 3


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"subscription_id": [f"sub{i}" for i in range(10)]})
    resource = FakeOneSignalResource()
    component = mod.OneSignalPushSendComponent(
        asset_name="onesignal_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="subscription_id",
        message_template="hi",
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 3
    out = metadata_for(result, "onesignal_push_out")
    assert out["messages_sent"] == 3


def test_source_inline_mode(mod, recorded_calls):
    resource = FakeOneSignalResource()
    component = mod.OneSignalPushSendComponent(
        asset_name="onesignal_push_out",
        source={"kind": "inline", "rows": [{"subscription_id": "sub1"}, {"subscription_id": "sub2"}]},
        recipient_column="subscription_id",
        message_template="hi",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"onesignal_resource": resource})
    assert result.success
    out = metadata_for(result, "onesignal_push_out")
    assert out["messages_sent"] == 2


# --- per-row failure isolation --------------------------------------------

def test_one_row_failing_does_not_abort_run(mod, monkeypatch):
    def _flaky_call(resource, subscription_id, heading, contents):
        if subscription_id == "sub2":
            raise RuntimeError("All included players are not subscribed")
        return {"id": "notif-1"}

    monkeypatch.setattr(mod, "_call_onesignal_api", _flaky_call)

    df = pd.DataFrame({"subscription_id": ["sub1", "sub2", "sub3"]})
    resource = FakeOneSignalResource()
    component = mod.OneSignalPushSendComponent(
        asset_name="onesignal_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="subscription_id",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success  # a per-row failure must not crash the whole run

    out = metadata_for(result, "onesignal_push_out")
    assert out["messages_sent"] == 2
    assert out["messages_failed"] == 1
    assert "first_errors" in out


def test_all_rows_failing_still_succeeds_with_zero_sent(mod, monkeypatch):
    def _always_fail(resource, subscription_id, heading, contents):
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "_call_onesignal_api", _always_fail)

    df = pd.DataFrame({"subscription_id": ["sub1", "sub2"]})
    resource = FakeOneSignalResource()
    component = mod.OneSignalPushSendComponent(
        asset_name="onesignal_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="subscription_id",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "onesignal_push_out")
    assert out["messages_sent"] == 0
    assert out["messages_failed"] == 2


# --- real HTTP-call isolation: payload shape (not network, just structure) -

def test_call_onesignal_api_builds_expected_payload(mod, monkeypatch):
    captured = {}

    class _FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"id": "notif-1"}

    def _fake_post(url, json, headers, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        return _FakeResponse()

    import requests
    monkeypatch.setattr(requests, "post", _fake_post)

    resource = FakeOneSignalResource(app_id="app-123", api_base_url="https://api.onesignal.com")
    mod._call_onesignal_api(resource, subscription_id="sub1", heading="Hi", contents="Body")

    assert captured["url"] == "https://api.onesignal.com/notifications"
    assert captured["json"]["app_id"] == "app-123"
    assert captured["json"]["include_subscription_ids"] == ["sub1"]
    assert captured["json"]["contents"] == {"en": "Body"}
    assert captured["json"]["headings"] == {"en": "Hi"}
    assert captured["json"]["target_channel"] == "push"
