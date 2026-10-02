"""Committed regression tests for DocuSignEnvelopeSendComponent.

The real `pyjwt` / `cryptography` / network path is never exercised here
-- `_call_docusign_api` (the one external API boundary) is monkeypatched
wholesale, while template rendering, dual source resolution, role/tabs
construction, validation, batching, and per-row failure isolation are all
exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeDocuSignResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, template_id, template_roles, email_subject, status):
        calls.append(
            {
                "resource": resource,
                "template_id": template_id,
                "template_roles": template_roles,
                "email_subject": email_subject,
                "status": status,
            }
        )
        return {"envelopeId": f"env-{len(calls)}", "status": status}

    monkeypatch.setattr(mod, "_call_docusign_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_contracts", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"docusign_resource": resource})


# --- template rendering (pure) -------------------------------------------

def test_render_template_substitutes_columns(mod):
    out = mod._render_template("Please sign your {contract_type} agreement", {"contract_type": "MSA"})
    assert out == "Please sign your MSA agreement"


def test_render_template_missing_column_renders_empty(mod):
    out = mod._render_template("Hi {missing_col}!", {})
    assert out == "Hi !"


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.DocuSignEnvelopeSendComponent(
            asset_name="x",
            template_id="tpl-123",
            role_name="Signer1",
            recipient_email_column="email",
            recipient_name_column="name",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.DocuSignEnvelopeSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            template_id="tpl-123",
            role_name="Signer1",
            recipient_email_column="email",
            recipient_name_column="name",
        ).build_defs(context=None)


def test_empty_template_id_raises(mod):
    with pytest.raises(ValueError, match="template_id must be non-empty"):
        mod.DocuSignEnvelopeSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            template_id="",
            role_name="Signer1",
            recipient_email_column="email",
            recipient_name_column="name",
        ).build_defs(context=None)


def test_empty_role_name_raises(mod):
    with pytest.raises(ValueError, match="role_name must be non-empty"):
        mod.DocuSignEnvelopeSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            template_id="tpl-123",
            role_name="",
            recipient_email_column="email",
            recipient_name_column="name",
        ).build_defs(context=None)


def test_invalid_status_raises(mod):
    with pytest.raises(ValueError, match="status must be"):
        mod.DocuSignEnvelopeSendComponent(
            asset_name="x",
            upstream_asset_key="foo",
            template_id="tpl-123",
            role_name="Signer1",
            recipient_email_column="email",
            recipient_name_column="name",
            status="draft",
        ).build_defs(context=None)


def test_missing_recipient_columns_raises_failure(mod, recorded_calls):
    df = pd.DataFrame({"other_col": ["a"]})
    resource = FakeDocuSignResource()
    component = mod.DocuSignEnvelopeSendComponent(
        asset_name="docusign_out",
        upstream_asset_key="upstream_contracts",
        template_id="tpl-123",
        role_name="Signer1",
        recipient_email_column="email",
        recipient_name_column="name",
    )
    with pytest.raises(dg.Failure, match="Columns not in upstream"):
        _materialize(component, df, resource)


def test_missing_tabs_map_column_raises_failure(mod, recorded_calls):
    df = pd.DataFrame({"email": ["a@test.com"], "name": ["A"]})
    resource = FakeDocuSignResource()
    component = mod.DocuSignEnvelopeSendComponent(
        asset_name="docusign_out",
        upstream_asset_key="upstream_contracts",
        template_id="tpl-123",
        role_name="Signer1",
        recipient_email_column="email",
        recipient_name_column="name",
        tabs_map={"contract_value": "ContractValue"},
    )
    with pytest.raises(dg.Failure, match="tabs_map columns not in upstream"):
        _materialize(component, df, resource)


# --- end-to-end send behavior, against the monkeypatched API call ---------

def test_send_end_to_end(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "email": ["jane@test.com", "bob@test.com"],
            "name": ["Jane Doe", "Bob Smith"],
            "contract_type": ["MSA", "NDA"],
        }
    )
    resource = FakeDocuSignResource()
    component = mod.DocuSignEnvelopeSendComponent(
        asset_name="docusign_out",
        upstream_asset_key="upstream_contracts",
        template_id="tpl-123",
        role_name="Signer1",
        recipient_email_column="email",
        recipient_name_column="name",
        email_subject_template="Please sign your {contract_type} agreement",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(recorded_calls) == 2
    assert recorded_calls[0]["template_id"] == "tpl-123"
    assert recorded_calls[0]["template_roles"] == [
        {"roleName": "Signer1", "name": "Jane Doe", "email": "jane@test.com"}
    ]
    assert recorded_calls[0]["email_subject"] == "Please sign your MSA agreement"
    assert recorded_calls[0]["status"] == "sent"

    out = metadata_for(result, "docusign_out")
    assert out["envelopes_sent"] == 2
    assert out["envelopes_failed"] == 0
    assert out["rows_skipped_no_recipient"] == 0
    assert "first_envelope_ids" in out


def test_tabs_map_prefills_text_tabs(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "email": ["jane@test.com"],
            "name": ["Jane Doe"],
            "contract_value": [50000],
        }
    )
    resource = FakeDocuSignResource()
    component = mod.DocuSignEnvelopeSendComponent(
        asset_name="docusign_out",
        upstream_asset_key="upstream_contracts",
        template_id="tpl-123",
        role_name="Signer1",
        recipient_email_column="email",
        recipient_name_column="name",
        tabs_map={"contract_value": "ContractValue"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    role = recorded_calls[0]["template_roles"][0]
    assert role["tabs"] == {"textTabs": [{"tabLabel": "ContractValue", "value": "50000"}]}


def test_status_created_passed_through(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@test.com"], "name": ["Jane Doe"]})
    resource = FakeDocuSignResource()
    component = mod.DocuSignEnvelopeSendComponent(
        asset_name="docusign_out",
        upstream_asset_key="upstream_contracts",
        template_id="tpl-123",
        role_name="Signer1",
        recipient_email_column="email",
        recipient_name_column="name",
        status="created",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls[0]["status"] == "created"


def test_empty_upstream_sends_nothing(mod, recorded_calls):
    df = pd.DataFrame({"email": [], "name": []})
    resource = FakeDocuSignResource()
    component = mod.DocuSignEnvelopeSendComponent(
        asset_name="docusign_out",
        upstream_asset_key="upstream_contracts",
        template_id="tpl-123",
        role_name="Signer1",
        recipient_email_column="email",
        recipient_name_column="name",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "docusign_out")
    assert out["envelopes_sent"] == 0


def test_blank_and_null_recipients_skipped_and_counted(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "email": ["jane@test.com", None, "  ", ""],
            "name": ["Jane Doe", "Bob", "Carl", "Dana"],
        }
    )
    resource = FakeDocuSignResource()
    component = mod.DocuSignEnvelopeSendComponent(
        asset_name="docusign_out",
        upstream_asset_key="upstream_contracts",
        template_id="tpl-123",
        role_name="Signer1",
        recipient_email_column="email",
        recipient_name_column="name",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 1
    out = metadata_for(result, "docusign_out")
    assert out["envelopes_sent"] == 1
    assert out["rows_skipped_no_recipient"] == 3


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "email": [f"user{i}@test.com" for i in range(10)],
            "name": [f"User {i}" for i in range(10)],
        }
    )
    resource = FakeDocuSignResource()
    component = mod.DocuSignEnvelopeSendComponent(
        asset_name="docusign_out",
        upstream_asset_key="upstream_contracts",
        template_id="tpl-123",
        role_name="Signer1",
        recipient_email_column="email",
        recipient_name_column="name",
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 3


def test_source_inline_mode(mod, recorded_calls):
    resource = FakeDocuSignResource()
    component = mod.DocuSignEnvelopeSendComponent(
        asset_name="docusign_out",
        source={"kind": "inline", "rows": [{"email": "jane@test.com", "name": "Jane Doe"}]},
        template_id="tpl-123",
        role_name="Signer1",
        recipient_email_column="email",
        recipient_name_column="name",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"docusign_resource": resource})
    assert result.success
    out = metadata_for(result, "docusign_out")
    assert out["envelopes_sent"] == 1


# --- per-row failure isolation --------------------------------------------

def test_one_row_failing_does_not_abort_run(mod, monkeypatch):
    def _flaky_call(resource, template_id, template_roles, email_subject, status):
        if template_roles[0]["email"] == "bob@test.com":
            raise RuntimeError("DocuSign: invalid role")
        return {"envelopeId": "env-1"}

    monkeypatch.setattr(mod, "_call_docusign_api", _flaky_call)

    df = pd.DataFrame(
        {
            "email": ["jane@test.com", "bob@test.com", "carl@test.com"],
            "name": ["Jane", "Bob", "Carl"],
        }
    )
    resource = FakeDocuSignResource()
    component = mod.DocuSignEnvelopeSendComponent(
        asset_name="docusign_out",
        upstream_asset_key="upstream_contracts",
        template_id="tpl-123",
        role_name="Signer1",
        recipient_email_column="email",
        recipient_name_column="name",
    )
    result = _materialize(component, df, resource)
    assert result.success  # a per-row failure must not crash the whole run

    out = metadata_for(result, "docusign_out")
    assert out["envelopes_sent"] == 2
    assert out["envelopes_failed"] == 1
    assert "first_errors" in out
