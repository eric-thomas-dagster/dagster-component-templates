"""Committed regression tests for TwilioSmsSendComponent.

The real `dagster-twilio` / `twilio` packages are never installed or
imported here -- `_call_twilio_api` (the one external, paid-API boundary)
is monkeypatched wholesale, while template rendering, dual source
resolution, validation, batching, and per-row failure isolation are all
exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeTwilioResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, to, from_, body):
        calls.append({"resource": resource, "to": to, "from_": from_, "body": body})
        return {"sid": f"SM{len(calls)}"}

    monkeypatch.setattr(mod, "_call_twilio_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"twilio_resource": resource})


# --- template rendering (pure) -------------------------------------------

def test_render_template_substitutes_columns(mod):
    out = mod._render_template("Hi {first_name}, order {order_id} shipped!", {"first_name": "Jane", "order_id": 42})
    assert out == "Hi Jane, order 42 shipped!"


def test_render_template_missing_column_renders_empty(mod):
    out = mod._render_template("Hi {first_name}, bonus {missing_col}!", {"first_name": "Jane"})
    assert out == "Hi Jane, bonus !"


def test_render_template_none_value_renders_empty(mod):
    out = mod._render_template("Hi {first_name}!", {"first_name": None})
    assert out == "Hi !"


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.TwilioSmsSendComponent(
            asset_name="x",
            from_number="+15551234567",
            recipient_column="phone",
            message_template="hi",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.TwilioSmsSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            from_number="+15551234567",
            recipient_column="phone",
            message_template="hi",
        ).build_defs(context=None)


def test_empty_message_template_raises(mod):
    with pytest.raises(ValueError, match="message_template must be non-empty"):
        mod.TwilioSmsSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            from_number="+15551234567",
            recipient_column="phone",
            message_template="",
        ).build_defs(context=None)


def test_missing_recipient_column_raises_failure(mod, recorded_calls):
    df = pd.DataFrame({"other_col": ["a"]})
    resource = FakeTwilioResource()
    component = mod.TwilioSmsSendComponent(
        asset_name="twilio_sms_out",
        upstream_asset_key="upstream_customers",
        from_number="+15551234567",
        recipient_column="phone",
        message_template="hi",
    )
    with pytest.raises(dg.Failure, match="recipient_column"):
        _materialize(component, df, resource)


# --- end-to-end send behavior, against the monkeypatched API call ---------

def test_send_end_to_end(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "phone": ["+14155550001", "+14155550002"],
            "first_name": ["Jane", "Bob"],
            "order_id": [1, 2],
        }
    )
    resource = FakeTwilioResource()
    component = mod.TwilioSmsSendComponent(
        asset_name="twilio_sms_out",
        upstream_asset_key="upstream_customers",
        from_number="+15551234567",
        recipient_column="phone",
        message_template="Hi {first_name}, order {order_id} shipped!",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(recorded_calls) == 2
    assert recorded_calls[0]["to"] == "+14155550001"
    assert recorded_calls[0]["from_"] == "+15551234567"
    assert recorded_calls[0]["body"] == "Hi Jane, order 1 shipped!"
    assert recorded_calls[1]["body"] == "Hi Bob, order 2 shipped!"

    out = metadata_for(result, "twilio_sms_out")
    assert out["messages_sent"] == 2
    assert out["messages_failed"] == 0
    assert out["rows_skipped_no_recipient"] == 0


def test_empty_upstream_sends_nothing(mod, recorded_calls):
    df = pd.DataFrame({"phone": []})
    resource = FakeTwilioResource()
    component = mod.TwilioSmsSendComponent(
        asset_name="twilio_sms_out",
        upstream_asset_key="upstream_customers",
        from_number="+15551234567",
        recipient_column="phone",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "twilio_sms_out")
    assert out["messages_sent"] == 0


def test_blank_and_null_recipients_skipped_and_counted(mod, recorded_calls):
    df = pd.DataFrame({"phone": ["+14155550001", None, "  ", ""]})
    resource = FakeTwilioResource()
    component = mod.TwilioSmsSendComponent(
        asset_name="twilio_sms_out",
        upstream_asset_key="upstream_customers",
        from_number="+15551234567",
        recipient_column="phone",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 1
    out = metadata_for(result, "twilio_sms_out")
    assert out["messages_sent"] == 1
    assert out["rows_skipped_no_recipient"] == 3


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"phone": [f"+1415555{i:04d}" for i in range(10)]})
    resource = FakeTwilioResource()
    component = mod.TwilioSmsSendComponent(
        asset_name="twilio_sms_out",
        upstream_asset_key="upstream_customers",
        from_number="+15551234567",
        recipient_column="phone",
        message_template="hi",
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 3
    out = metadata_for(result, "twilio_sms_out")
    assert out["messages_sent"] == 3


def test_source_inline_mode(mod, recorded_calls):
    resource = FakeTwilioResource()
    component = mod.TwilioSmsSendComponent(
        asset_name="twilio_sms_out",
        source={"kind": "inline", "rows": [{"phone": "+14155550001"}, {"phone": "+14155550002"}]},
        from_number="+15551234567",
        recipient_column="phone",
        message_template="hi",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"twilio_resource": resource})
    assert result.success
    out = metadata_for(result, "twilio_sms_out")
    assert out["messages_sent"] == 2


# --- per-row failure isolation --------------------------------------------

def test_one_row_failing_does_not_abort_run(mod, monkeypatch):
    def _flaky_call(resource, to, from_, body):
        if to == "+14155550002":
            raise RuntimeError("Twilio: invalid phone number")
        return {"sid": "SM1"}

    monkeypatch.setattr(mod, "_call_twilio_api", _flaky_call)

    df = pd.DataFrame({"phone": ["+14155550001", "+14155550002", "+14155550003"]})
    resource = FakeTwilioResource()
    component = mod.TwilioSmsSendComponent(
        asset_name="twilio_sms_out",
        upstream_asset_key="upstream_customers",
        from_number="+15551234567",
        recipient_column="phone",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success  # a per-row failure must not crash the whole run

    out = metadata_for(result, "twilio_sms_out")
    assert out["messages_sent"] == 2
    assert out["messages_failed"] == 1
    assert "first_errors" in out


def test_all_rows_failing_still_succeeds_with_zero_sent(mod, monkeypatch):
    def _always_fail(resource, to, from_, body):
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "_call_twilio_api", _always_fail)

    df = pd.DataFrame({"phone": ["+14155550001", "+14155550002"]})
    resource = FakeTwilioResource()
    component = mod.TwilioSmsSendComponent(
        asset_name="twilio_sms_out",
        upstream_asset_key="upstream_customers",
        from_number="+15551234567",
        recipient_column="phone",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "twilio_sms_out")
    assert out["messages_sent"] == 0
    assert out["messages_failed"] == 2


def test_from_number_in_metadata(mod, recorded_calls):
    df = pd.DataFrame({"phone": ["+14155550001"]})
    resource = FakeTwilioResource()
    component = mod.TwilioSmsSendComponent(
        asset_name="twilio_sms_out",
        upstream_asset_key="upstream_customers",
        from_number="+15551234567",
        recipient_column="phone",
        message_template="hi",
    )
    result = _materialize(component, df, resource)
    out = metadata_for(result, "twilio_sms_out")
    assert out["from_number"] == "+15551234567"
