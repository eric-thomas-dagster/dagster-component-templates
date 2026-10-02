"""Committed regression tests for HelpScoutConversationUpsertComponent.

FakeHelpScoutResource (conftest.py) stands in for the real
`help_scout_resource` -- the one external, network-calling boundary --
while everything this component actually owns (dual source resolution,
create-vs-update branching, tag parsing, body construction, no-op
detection, error handling, metadata) is exercised for real via
`dg.materialize`.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeHelpScoutResource, load_component_module, make_upstream_asset


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, resource, asset_name="upstream_conversations"):
    upstream_asset = make_upstream_asset(asset_name, upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"help_scout_resource": resource})


def _metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- read the event's
    metadata instead of output_for_node()."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- dual-source validation -----------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.HelpScoutConversationUpsertComponent(
            asset_name="x",
            mailbox_id=1,
            customer_email_column="email",
            subject_column="subject",
            body_column="body",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.HelpScoutConversationUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            mailbox_id=1,
            customer_email_column="email",
            subject_column="subject",
            body_column="body",
        ).build_defs(context=None)


def test_partial_subject_body_raises(mod):
    with pytest.raises(ValueError, match="subject_column"):
        mod.HelpScoutConversationUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            mailbox_id=1,
            customer_email_column="email",
            subject_column="subject",
            # body_column intentionally omitted
        ).build_defs(context=None)


# --- create path (no conversation_id) --------------------------------------

def test_no_conversation_id_creates_conversation(mod):
    df = pd.DataFrame({
        "conversation_id": [None],
        "email": ["jane@example.com"],
        "subject": ["Billing question"],
        "body": ["Customer asks about invoice #123"],
        "tags": ["billing,vip"],
    })
    resource = FakeHelpScoutResource()
    component = mod.HelpScoutConversationUpsertComponent(
        asset_name="hs_out",
        upstream_asset_key="upstream_conversations",
        mailbox_id=85,
        conversation_id_column="conversation_id",
        customer_email_column="email",
        subject_column="subject",
        body_column="body",
        tags_column="tags",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(resource.create_calls) == 1
    call = resource.create_calls[0]
    assert call["mailbox_id"] == 85
    assert call["customer_email"] == "jane@example.com"
    assert call["subject"] == "Billing question"
    assert call["tags"] == ["billing", "vip"]

    assert resource.update_tags_calls == []
    assert resource.add_note_calls == []
    assert resource.patch_status_calls == []

    out = _metadata_for(result, "hs_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 0


# --- update path (conversation_id present) ---------------------------------

def test_conversation_id_present_updates_tags_not_create(mod):
    resource = FakeHelpScoutResource()
    resource.seed_conversation(555)

    df = pd.DataFrame({
        "conversation_id": [555],
        "email": ["bob@example.com"],
        "subject": [None],
        "body": [None],
        "tags": ["escalated,billing"],
    })
    component = mod.HelpScoutConversationUpsertComponent(
        asset_name="hs_out",
        upstream_asset_key="upstream_conversations",
        mailbox_id=85,
        conversation_id_column="conversation_id",
        customer_email_column="email",
        subject_column="subject",
        body_column="body",
        tags_column="tags",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert resource.create_calls == []
    assert len(resource.update_tags_calls) == 1
    assert resource.update_tags_calls[0]["conversation_id"] == 555
    assert resource.update_tags_calls[0]["tags"] == ["escalated", "billing"]

    out = _metadata_for(result, "hs_out")
    assert out["rows_created"] == 0
    assert out["rows_updated"] == 1


def test_conversation_id_with_no_update_fields_is_noop(mod):
    resource = FakeHelpScoutResource()
    resource.seed_conversation(555)

    df = pd.DataFrame({
        "conversation_id": [555],
        "email": ["bob@example.com"],
        "subject": [None],
        "body": [None],
    })
    component = mod.HelpScoutConversationUpsertComponent(
        asset_name="hs_out",
        upstream_asset_key="upstream_conversations",
        mailbox_id=85,
        conversation_id_column="conversation_id",
        customer_email_column="email",
        subject_column="subject",
        body_column="body",
        # no tags_column / note_column / status_column configured
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert resource.create_calls == []
    assert resource.update_tags_calls == []
    assert resource.add_note_calls == []
    assert resource.patch_status_calls == []

    out = _metadata_for(result, "hs_out")
    assert out["rows_updated"] == 0
    assert out["rows_skipped_noop"] == 1


def test_note_and_status_columns_trigger_add_note_and_patch_status(mod):
    resource = FakeHelpScoutResource()
    resource.seed_conversation(777)

    df = pd.DataFrame({
        "conversation_id": [777],
        "email": ["x@example.com"],
        "subject": [None],
        "body": [None],
        "note": ["Escalating to tier 2"],
        "status": ["pending"],
    })
    component = mod.HelpScoutConversationUpsertComponent(
        asset_name="hs_out",
        upstream_asset_key="upstream_conversations",
        mailbox_id=85,
        conversation_id_column="conversation_id",
        customer_email_column="email",
        subject_column="subject",
        body_column="body",
        note_column="note",
        status_column="status",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.add_note_calls) == 1
    assert resource.add_note_calls[0]["text"] == "Escalating to tier 2"
    assert len(resource.patch_status_calls) == 1
    assert resource.patch_status_calls[0]["status"] == "pending"


# --- missing email rows (create path, no conversation_id) ------------------

def test_missing_email_rows_skipped_and_counted(mod):
    df = pd.DataFrame({
        "email": ["jane@example.com", None, ""],
        "subject": ["s1", "s2", "s3"],
        "body": ["b1", "b2", "b3"],
    })
    resource = FakeHelpScoutResource()
    component = mod.HelpScoutConversationUpsertComponent(
        asset_name="hs_out",
        upstream_asset_key="upstream_conversations",
        mailbox_id=85,
        customer_email_column="email",
        subject_column="subject",
        body_column="body",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = _metadata_for(result, "hs_out")
    assert out["rows_skipped_no_key"] == 2
    assert out["rows_created"] == 1


# --- max_rows cap ------------------------------------------------------------

def test_max_rows_caps_processed_rows(mod):
    df = pd.DataFrame({
        "email": [f"user{i}@example.com" for i in range(10)],
        "subject": [f"s{i}" for i in range(10)],
        "body": [f"b{i}" for i in range(10)],
    })
    resource = FakeHelpScoutResource()
    component = mod.HelpScoutConversationUpsertComponent(
        asset_name="hs_out",
        upstream_asset_key="upstream_conversations",
        mailbox_id=85,
        customer_email_column="email",
        subject_column="subject",
        body_column="body",
        max_rows=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(resource.create_calls) == 3
    out = _metadata_for(result, "hs_out")
    assert out["rows_created"] == 3


# --- inline source shape ------------------------------------------------------

def test_source_inline_mode(mod):
    resource = FakeHelpScoutResource()
    component = mod.HelpScoutConversationUpsertComponent(
        asset_name="hs_out",
        source={"kind": "inline", "rows": [
            {"email": "a@b.com", "subject": "s1", "body": "b1"},
            {"email": "c@d.com", "subject": "s2", "body": "b2"},
        ]},
        mailbox_id=85,
        customer_email_column="email",
        subject_column="subject",
        body_column="body",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"help_scout_resource": resource})
    assert result.success
    out = _metadata_for(result, "hs_out")
    assert out["rows_created"] == 2


# --- error handling ------------------------------------------------------------

def test_create_error_is_caught_counted_and_does_not_blow_up_run(mod, monkeypatch):
    resource = FakeHelpScoutResource()

    def _boom(**kwargs):
        raise RuntimeError("Help Scout API 500")

    monkeypatch.setattr(resource, "create_conversation", _boom)

    df = pd.DataFrame({
        "email": ["jane@example.com", "bob@example.com"],
        "subject": ["s1", "s2"],
        "body": ["b1", "b2"],
    })
    component = mod.HelpScoutConversationUpsertComponent(
        asset_name="hs_out",
        upstream_asset_key="upstream_conversations",
        mailbox_id=85,
        customer_email_column="email",
        subject_column="subject",
        body_column="body",
    )
    result = _materialize(component, df, resource)
    assert result.success  # the asset itself still succeeds -- errors are collected, not raised
    out = _metadata_for(result, "hs_out")
    assert out["rows_errored"] == 2
    assert out["rows_created"] == 0
    assert "first_errors" in out
