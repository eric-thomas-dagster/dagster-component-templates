"""Committed regression tests for FirebaseFcmSendComponent.

The real `firebase-admin` package is never installed or imported here --
`_call_firebase_api` (the one external boundary) is monkeypatched
wholesale, while template rendering, dual source resolution, validation,
batching, and per-row failure isolation are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeFirebaseResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, token, title, body):
        calls.append({"resource": resource, "token": token, "title": title, "body": body})
        return f"projects/x/messages/{len(calls)}"

    monkeypatch.setattr(mod, "_call_firebase_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_users", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"firebase_resource": resource})


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
        mod.FirebaseFcmSendComponent(
            asset_name="x",
            recipient_column="token",
            message_template="hi",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.FirebaseFcmSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            recipient_column="token",
            message_template="hi",
        ).build_defs(context=None)


def test_empty_message_template_raises(mod):
    with pytest.raises(ValueError, match="message_template must be non-empty"):
        mod.FirebaseFcmSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            recipient_column="token",
            message_template="",
        ).build_defs(context=None)


def test_missing_recipient_column_raises_failure(mod, recorded_calls):
    df = pd.DataFrame({"other_col": ["a"]})
    resource = FakeFirebaseResource()
    component = mod.FirebaseFcmSendComponent(
        asset_name="firebase_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="device_token",
        message_template="hi",
    )
    with pytest.raises(dg.Failure, match="recipient_column"):
        _materialize(component, df, resource)


# --- end-to-end send behavior, against the monkeypatched API call ---------

def test_send_end_to_end_with_title(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "device_token": ["tok1", "tok2"],
            "first_name": ["Jane", "Bob"],
            "item_count": [3, 1],
        }
    )
    resource = FakeFirebaseResource()
    component = mod.FirebaseFcmSendComponent(
        asset_name="firebase_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="device_token",
        title_template="You left something behind!",
        message_template="{first_name}, your cart has {item_count} item(s) waiting.",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(recorded_calls) == 2
    assert recorded_calls[0]["token"] == "tok1"
    assert recorded_calls[0]["title"] == "You left something behind!"
    assert recorded_calls[0]["body"] == "Jane, your cart has 3 item(s) waiting."
    assert recorded_calls[1]["body"] == "Bob, your cart has 1 item(s) waiting."

    out = metadata_for(result, "firebase_push_out")
    assert out["messages_sent"] == 2
    assert out["messages_failed"] == 0


def test_send_without_title_template(mod, recorded_calls):
    df = pd.DataFrame({"device_token": ["tok1"]})
    resource = FakeFirebaseResource()
    component = mod.FirebaseFcmSendComponent(
        asset_name="firebase_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="device_token",
        message_template="just a body",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls[0]["title"] is None
    assert recorded_calls[0]["body"] == "just a body"


def test_empty_upstream_sends_nothing(mod, recorded_calls):
    df = pd.DataFrame({"device_token": []})
    resource = FakeFirebaseResource()
    component = mod.FirebaseFcmSendComponent(
        asset_name="firebase_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="device_token",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "firebase_push_out")
    assert out["messages_sent"] == 0


def test_blank_and_null_recipients_skipped_and_counted(mod, recorded_calls):
    df = pd.DataFrame({"device_token": ["tok1", None, "  ", ""]})
    resource = FakeFirebaseResource()
    component = mod.FirebaseFcmSendComponent(
        asset_name="firebase_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="device_token",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 1
    out = metadata_for(result, "firebase_push_out")
    assert out["messages_sent"] == 1
    assert out["rows_skipped_no_recipient"] == 3


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"device_token": [f"tok{i}" for i in range(10)]})
    resource = FakeFirebaseResource()
    component = mod.FirebaseFcmSendComponent(
        asset_name="firebase_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="device_token",
        message_template="hi",
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 3
    out = metadata_for(result, "firebase_push_out")
    assert out["messages_sent"] == 3


def test_source_inline_mode(mod, recorded_calls):
    resource = FakeFirebaseResource()
    component = mod.FirebaseFcmSendComponent(
        asset_name="firebase_push_out",
        source={"kind": "inline", "rows": [{"device_token": "tok1"}, {"device_token": "tok2"}]},
        recipient_column="device_token",
        message_template="hi",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"firebase_resource": resource})
    assert result.success
    out = metadata_for(result, "firebase_push_out")
    assert out["messages_sent"] == 2


# --- per-row failure isolation --------------------------------------------

def test_one_row_failing_does_not_abort_run(mod, monkeypatch):
    def _flaky_call(resource, token, title, body):
        if token == "tok2":
            raise RuntimeError("Requested entity was not found (unregistered token)")
        return "id1"

    monkeypatch.setattr(mod, "_call_firebase_api", _flaky_call)

    df = pd.DataFrame({"device_token": ["tok1", "tok2", "tok3"]})
    resource = FakeFirebaseResource()
    component = mod.FirebaseFcmSendComponent(
        asset_name="firebase_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="device_token",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success  # a per-row failure must not crash the whole run

    out = metadata_for(result, "firebase_push_out")
    assert out["messages_sent"] == 2
    assert out["messages_failed"] == 1
    assert "first_errors" in out


def test_all_rows_failing_still_succeeds_with_zero_sent(mod, monkeypatch):
    def _always_fail(resource, token, title, body):
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "_call_firebase_api", _always_fail)

    df = pd.DataFrame({"device_token": ["tok1", "tok2"]})
    resource = FakeFirebaseResource()
    component = mod.FirebaseFcmSendComponent(
        asset_name="firebase_push_out",
        upstream_asset_key="upstream_users",
        recipient_column="device_token",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "firebase_push_out")
    assert out["messages_sent"] == 0
    assert out["messages_failed"] == 2
