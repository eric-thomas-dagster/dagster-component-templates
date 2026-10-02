"""Committed regression tests for MSTeamsMessageSendComponent.

The real MS Teams webhook POST is never made here -- `_post_teams_message`
(the one external boundary) is monkeypatched wholesale, while template
rendering, card building, mode handling, validation, auth-path selection,
and per-row failure isolation are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeMSTeamsResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_post(client, payload):
        calls.append({"client": client, "payload": payload})
        return True

    monkeypatch.setattr(mod, "_post_teams_message", _fake_post)
    return calls


def _materialize(component, upstream_df, resource_key="msteams", resource=None):
    upstream_asset = make_upstream_asset("upstream_customers", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    resources = {resource_key: resource} if resource is not None else {}
    return dg.materialize([asset_def, upstream_asset], resources=resources)


# --- template rendering + card building (pure) ----------------------------

def test_render_substitutes_columns(mod):
    out = mod._render("{customer_name} crossed {health_score}", {"customer_name": "Acme", "health_score": 42})
    assert out == "Acme crossed 42"


def test_render_missing_column_renders_empty(mod):
    out = mod._render("{customer_name} - {missing_col}", {"customer_name": "Acme"})
    assert out == "Acme - "


def test_build_hero_card_with_title():
    mod = load_component_module()
    payload = mod._build_hero_card("My Title", "My Text")
    assert payload == {
        "type": "message",
        "attachments": [
            {"contentType": "application/vnd.microsoft.card.hero", "content": {"text": "My Text", "title": "My Title"}}
        ],
    }


def test_build_hero_card_without_title():
    mod = load_component_module()
    payload = mod._build_hero_card(None, "My Text")
    content = payload["attachments"][0]["content"]
    assert content == {"text": "My Text"}
    assert "title" not in content


# --- validation ------------------------------------------------------------

def test_neither_resource_key_nor_webhook_env_var_raises(mod):
    with pytest.raises(ValueError, match="resource_key.*OR webhook_url_env_var"):
        mod.MSTeamsMessageSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            message_template="hi",
        ).build_defs(context=None)


def test_unknown_mode_raises(mod, recorded_calls):
    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.MSTeamsMessageSendComponent(
        asset_name="teams_out",
        upstream_asset_key="upstream_customers",
        resource_key="msteams",
        message_template="hi",
        mode="bogus",
    )
    with pytest.raises(Exception, match="unknown mode"):
        _materialize(component, df, resource=FakeMSTeamsResource())


# --- end-to-end send behavior, against the monkeypatched API call ---------

def test_per_row_send_end_to_end(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "customer_name": ["Acme", "Globex"],
            "health_score": [42, 58],
        }
    )
    component = mod.MSTeamsMessageSendComponent(
        asset_name="teams_out",
        upstream_asset_key="upstream_customers",
        resource_key="msteams",
        title_template="Health score alert: {customer_name}",
        message_template="{customer_name} crossed {health_score}",
    )
    result = _materialize(component, df, resource=FakeMSTeamsResource())
    assert result.success

    assert len(recorded_calls) == 2
    assert recorded_calls[0]["payload"]["attachments"][0]["content"]["title"] == "Health score alert: Acme"
    assert recorded_calls[0]["payload"]["attachments"][0]["content"]["text"] == "Acme crossed 42"
    assert recorded_calls[1]["payload"]["attachments"][0]["content"]["text"] == "Globex crossed 58"

    out = metadata_for(result, "teams_out")
    assert out["messages_sent"] == 2
    assert out["messages_failed"] == 0
    assert out["dry_run"] is False


def test_summary_mode(mod, recorded_calls):
    df = pd.DataFrame({"customer_name": ["Acme", "Globex", "Initech"]})
    component = mod.MSTeamsMessageSendComponent(
        asset_name="teams_out",
        upstream_asset_key="upstream_customers",
        resource_key="msteams",
        mode="summary",
        summary_template="{row_count} customers crossed the threshold today.",
    )
    result = _materialize(component, df, resource=FakeMSTeamsResource())
    assert result.success
    assert len(recorded_calls) == 1
    assert recorded_calls[0]["payload"]["attachments"][0]["content"]["text"] == "3 customers crossed the threshold today."
    out = metadata_for(result, "teams_out")
    assert out["messages_sent"] == 1


def test_dry_run_sends_nothing(mod, recorded_calls):
    df = pd.DataFrame({"customer_name": ["Acme", "Globex"]})
    component = mod.MSTeamsMessageSendComponent(
        asset_name="teams_out",
        upstream_asset_key="upstream_customers",
        resource_key="msteams",
        message_template="{customer_name}",
        dry_run=True,
    )
    result = _materialize(component, df, resource=FakeMSTeamsResource())
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "teams_out")
    assert out["messages_sent"] == 2
    assert out["dry_run"] is True


def test_max_send_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"customer_name": [f"c{i}" for i in range(10)]})
    component = mod.MSTeamsMessageSendComponent(
        asset_name="teams_out",
        upstream_asset_key="upstream_customers",
        resource_key="msteams",
        message_template="{customer_name}",
        max_send=3,
    )
    result = _materialize(component, df, resource=FakeMSTeamsResource())
    assert result.success
    assert len(recorded_calls) == 3
    out = metadata_for(result, "teams_out")
    assert out["messages_sent"] == 3


# --- per-row failure isolation --------------------------------------------

def test_one_row_failing_does_not_abort_run(mod, monkeypatch):
    def _flaky_post(client, payload):
        if "Globex" in payload["attachments"][0]["content"]["text"]:
            raise RuntimeError("webhook rejected payload")
        return True

    monkeypatch.setattr(mod, "_post_teams_message", _flaky_post)

    df = pd.DataFrame({"customer_name": ["Acme", "Globex", "Initech"]})
    component = mod.MSTeamsMessageSendComponent(
        asset_name="teams_out",
        upstream_asset_key="upstream_customers",
        resource_key="msteams",
        message_template="{customer_name}",
    )
    result = _materialize(component, df, resource=FakeMSTeamsResource())
    assert result.success

    out = metadata_for(result, "teams_out")
    assert out["messages_sent"] == 2
    assert out["messages_failed"] == 1
    assert "first_errors" in out


# --- auth-path selection ----------------------------------------------------

def test_webhook_url_env_var_fallback_builds_real_teams_client(mod, recorded_calls, monkeypatch):
    """When resource_key is not set, the component should build a real
    dagster_msteams.TeamsClient from webhook_url_env_var -- its
    constructor makes no network call, and _post_teams_message is still
    the monkeypatched boundary, so this exercises the fallback auth path
    for real."""
    monkeypatch.setenv("MY_TEAMS_WEBHOOK", "https://example.webhook.office.com/hook")
    df = pd.DataFrame({"customer_name": ["Acme"]})
    component = mod.MSTeamsMessageSendComponent(
        asset_name="teams_out",
        upstream_asset_key="upstream_customers",
        webhook_url_env_var="MY_TEAMS_WEBHOOK",
        message_template="{customer_name}",
    )
    result = _materialize(component, df, resource_key="msteams", resource=None)
    assert result.success
    assert len(recorded_calls) == 1
    from dagster_msteams.client import TeamsClient
    assert isinstance(recorded_calls[0]["client"], TeamsClient)
