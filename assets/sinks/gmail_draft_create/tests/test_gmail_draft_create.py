"""Committed regression tests for GmailDraftCreateComponent.

The real Gmail API call is never made here -- `requests.post` (the one
external boundary) is monkeypatched wholesale via monkeypatch.setattr,
while upstream resolution, template rendering, MIME construction,
base64url encoding, validation, and metadata are all exercised for real.
"""
import base64
from email import message_from_bytes

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeResponse, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_posts(mod, monkeypatch):
    calls = []

    def _fake_post(url, headers=None, json=None, **kwargs):
        calls.append({"url": url, "headers": headers, "json": json})
        return FakeResponse(200, {"id": "draft123", "message": {"id": "msg456", "threadId": "thread789"}})

    monkeypatch.setattr(mod.requests, "post", _fake_post)
    return calls


def _materialize(component, upstream_value, upstream_name="upstream_record"):
    upstream_asset = make_upstream_asset(upstream_name, upstream_value)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset])


# --- template rendering (pure) -------------------------------------------

def test_render_substitutes_columns(mod):
    out = mod._render("Hi {name}, score {score}", {"name": "Acme", "score": 42})
    assert out == "Hi Acme, score 42"


def test_render_missing_column_renders_empty(mod):
    out = mod._render("{name} - {missing_col}", {"name": "Acme"})
    assert out == "Acme - "


def test_render_leaves_non_identifier_braces_untouched(mod):
    # literal "{" with a space inside isn't a \w+ identifier, so it survives unresolved
    out = mod._render("note: {not a field} but {name} is", {"name": "Acme"})
    assert out == "note: {not a field} but Acme is"


# --- upstream resolution (dict vs DataFrame) -------------------------------

def test_resolve_template_fields_from_dict(mod):
    row = mod._resolve_template_fields({"customer_name": "Acme", "score": 7})
    assert row == {"customer_name": "Acme", "score": 7}


def test_resolve_template_fields_from_dataframe_default_row(mod):
    df = pd.DataFrame({"customer_name": ["Acme", "Globex"], "score": [1, 2]})
    row = mod._resolve_template_fields(df)
    assert row == {"customer_name": "Acme", "score": 1}


def test_resolve_template_fields_from_dataframe_row_index(mod):
    df = pd.DataFrame({"customer_name": ["Acme", "Globex"], "score": [1, 2]})
    row = mod._resolve_template_fields(df, row_index=1)
    assert row == {"customer_name": "Globex", "score": 2}


def test_resolve_template_fields_empty_dataframe_raises(mod):
    with pytest.raises(ValueError, match="empty"):
        mod._resolve_template_fields(pd.DataFrame())


def test_resolve_template_fields_row_index_out_of_range_raises(mod):
    df = pd.DataFrame({"customer_name": ["Acme"]})
    with pytest.raises(ValueError, match="out of range"):
        mod._resolve_template_fields(df, row_index=5)


def test_resolve_template_fields_unsupported_type_raises(mod):
    with pytest.raises(ValueError, match="dict or pandas DataFrame"):
        mod._resolve_template_fields(42)


# --- MIME message construction (pure, before base64) -----------------------

def test_build_mime_message_headers_and_body(mod):
    msg = mod._build_mime_message(
        "me@example.com", "Nightly brief", "Here is the summary.",
        cc_emails=["cc1@example.com", "cc2@example.com"],
        bcc_emails=["bcc@example.com"],
    )
    assert msg["To"] == "me@example.com"
    assert msg["Subject"] == "Nightly brief"
    assert msg["Cc"] == "cc1@example.com, cc2@example.com"
    assert msg["Bcc"] == "bcc@example.com"
    assert msg.get_payload(decode=True).decode("utf-8") == "Here is the summary."


def test_build_mime_message_no_cc_bcc_omits_headers(mod):
    msg = mod._build_mime_message("me@example.com", "Subj", "Body")
    assert msg["To"] == "me@example.com"
    assert msg["Cc"] is None
    assert msg["Bcc"] is None


# --- base64url encoding round-trip -----------------------------------------

def test_encode_mime_message_round_trips(mod):
    msg = mod._build_mime_message("me@example.com", "Subj", "Hello world", cc_emails=["cc@example.com"])
    encoded = mod._encode_mime_message(msg)

    # must be valid base64url
    decoded_bytes = base64.urlsafe_b64decode(encoded.encode())
    parsed = message_from_bytes(decoded_bytes)
    assert parsed["To"] == "me@example.com"
    assert parsed["Subject"] == "Subj"
    assert parsed["Cc"] == "cc@example.com"
    assert parsed.get_payload(decode=True).decode("utf-8") == "Hello world"


# --- validation: required fields raise ValueError at construction time ----

def test_missing_asset_name_raises_value_error(mod):
    with pytest.raises(ValueError):
        mod.GmailDraftCreateComponent(
            access_token="tok",
            upstream_asset_key="upstream_record",
            to_email="me@example.com",
            subject_template="s",
            body_template="b",
        )


def test_missing_access_token_raises_value_error(mod):
    with pytest.raises(ValueError):
        mod.GmailDraftCreateComponent(
            asset_name="draft_out",
            upstream_asset_key="upstream_record",
            to_email="me@example.com",
            subject_template="s",
            body_template="b",
        )


def test_missing_upstream_asset_key_raises_value_error(mod):
    with pytest.raises(ValueError):
        mod.GmailDraftCreateComponent(
            asset_name="draft_out",
            access_token="tok",
            to_email="me@example.com",
            subject_template="s",
            body_template="b",
        )


def test_missing_to_email_raises_value_error(mod):
    with pytest.raises(ValueError):
        mod.GmailDraftCreateComponent(
            asset_name="draft_out",
            access_token="tok",
            upstream_asset_key="upstream_record",
            subject_template="s",
            body_template="b",
        )


def test_missing_subject_template_raises_value_error(mod):
    with pytest.raises(ValueError):
        mod.GmailDraftCreateComponent(
            asset_name="draft_out",
            access_token="tok",
            upstream_asset_key="upstream_record",
            to_email="me@example.com",
            body_template="b",
        )


def test_missing_body_template_raises_value_error(mod):
    with pytest.raises(ValueError):
        mod.GmailDraftCreateComponent(
            asset_name="draft_out",
            access_token="tok",
            upstream_asset_key="upstream_record",
            to_email="me@example.com",
            subject_template="s",
        )


# --- end-to-end draft creation, against the monkeypatched POST call -------

def test_end_to_end_dict_upstream(mod, recorded_posts):
    component = mod.GmailDraftCreateComponent(
        asset_name="draft_out",
        access_token="my-access-token",
        upstream_asset_key="upstream_record",
        to_email="me@example.com",
        subject_template="Nightly brief -- {report_date}",
        body_template="Summary: {summary_text}",
    )
    upstream_value = {"report_date": "2026-10-27", "summary_text": "all green"}
    result = _materialize(component, upstream_value)
    assert result.success

    assert len(recorded_posts) == 1
    call = recorded_posts[0]
    assert call["url"] == "https://gmail.googleapis.com/gmail/v1/users/me/drafts"
    assert call["headers"]["Authorization"] == "Bearer my-access-token"
    assert "message" in call["json"] and "raw" in call["json"]["message"]

    # the raw payload round-trips to the rendered subject/body
    decoded = base64.urlsafe_b64decode(call["json"]["message"]["raw"].encode())
    parsed = message_from_bytes(decoded)
    assert parsed["Subject"] == "Nightly brief -- 2026-10-27"
    assert parsed["To"] == "me@example.com"
    assert parsed.get_payload(decode=True).decode("utf-8") == "Summary: all green"

    out = metadata_for(result, "draft_out")
    assert out["draft_id"] == "draft123"
    assert out["message_id"] == "msg456"
    assert out["to"] == "me@example.com"
    assert out["dry_run"] is False


def test_end_to_end_dataframe_upstream_with_row_index(mod, recorded_posts):
    component = mod.GmailDraftCreateComponent(
        asset_name="draft_out",
        access_token="tok",
        upstream_asset_key="upstream_record",
        row_index=1,
        to_email="me@example.com",
        subject_template="Brief for {customer_name}",
        body_template="Score: {score}",
    )
    df = pd.DataFrame({"customer_name": ["Acme", "Globex"], "score": [1, 2]})
    result = _materialize(component, df)
    assert result.success

    decoded = base64.urlsafe_b64decode(recorded_posts[0]["json"]["message"]["raw"].encode())
    parsed = message_from_bytes(decoded)
    assert parsed["Subject"] == "Brief for Globex"
    assert parsed.get_payload(decode=True).decode("utf-8") == "Score: 2"


def test_cc_bcc_templated_and_included(mod, recorded_posts):
    component = mod.GmailDraftCreateComponent(
        asset_name="draft_out",
        access_token="tok",
        upstream_asset_key="upstream_record",
        to_email="me@example.com",
        subject_template="s",
        body_template="b",
        cc_emails=["{cc_addr}"],
        bcc_emails=["bcc@example.com"],
    )
    result = _materialize(component, {"cc_addr": "manager@example.com"})
    assert result.success

    decoded = base64.urlsafe_b64decode(recorded_posts[0]["json"]["message"]["raw"].encode())
    parsed = message_from_bytes(decoded)
    assert parsed["Cc"] == "manager@example.com"
    assert parsed["Bcc"] == "bcc@example.com"


def test_dry_run_skips_api_call(mod, recorded_posts):
    component = mod.GmailDraftCreateComponent(
        asset_name="draft_out",
        access_token="tok",
        upstream_asset_key="upstream_record",
        to_email="me@example.com",
        subject_template="s",
        body_template="b",
        dry_run=True,
    )
    result = _materialize(component, {"x": 1})
    assert result.success
    assert recorded_posts == []
    out = metadata_for(result, "draft_out")
    assert out["dry_run"] is True
    assert "draft_id" not in out


def test_empty_to_email_raises_clear_error(mod, recorded_posts):
    component = mod.GmailDraftCreateComponent(
        asset_name="draft_out",
        access_token="tok",
        upstream_asset_key="upstream_record",
        to_email="{missing_field}",
        subject_template="s",
        body_template="b",
    )
    with pytest.raises(Exception, match="to_email rendered to an empty string"):
        _materialize(component, {"other": "val"})


# --- non-2xx response raises a clear error ---------------------------------

def test_non_2xx_response_raises_clear_error(mod, monkeypatch):
    def _fake_post_error(url, headers=None, json=None, **kwargs):
        return FakeResponse(403, {"error": {"message": "insufficient scope"}}, text="insufficient scope")

    monkeypatch.setattr(mod.requests, "post", _fake_post_error)

    component = mod.GmailDraftCreateComponent(
        asset_name="draft_out",
        access_token="bad-token",
        upstream_asset_key="upstream_record",
        to_email="me@example.com",
        subject_template="s",
        body_template="b",
    )
    with pytest.raises(Exception, match="Gmail draft creation failed"):
        _materialize(component, {"x": 1})
