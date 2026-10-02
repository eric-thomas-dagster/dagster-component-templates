"""Committed regression tests for PandaDocDocumentCreateComponent.

The real `requests` network path is never exercised here --
`_call_pandadoc_api` (the one external API boundary) is monkeypatched
wholesale, while template rendering, dual source resolution,
recipient/tokens construction, validation, batching, and per-row failure
isolation are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakePandaDocResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(
        resource, name, template_uuid, recipients, tokens, auto_send,
        send_message, poll_interval_seconds, poll_timeout_seconds,
    ):
        calls.append(
            {
                "resource": resource,
                "name": name,
                "template_uuid": template_uuid,
                "recipients": recipients,
                "tokens": tokens,
                "auto_send": auto_send,
                "send_message": send_message,
            }
        )
        return {"id": f"doc-{len(calls)}", "status": "document.uploaded"}

    monkeypatch.setattr(mod, "_call_pandadoc_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_contracts", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"pandadoc_resource": resource})


# --- template rendering (pure) -------------------------------------------

def test_render_template_substitutes_columns(mod):
    out = mod._render_template("{contract_type} Agreement - {customer_name}", {"contract_type": "MSA", "customer_name": "Acme"})
    assert out == "MSA Agreement - Acme"


def test_render_template_missing_column_renders_empty(mod):
    out = mod._render_template("Hi {missing_col}!", {})
    assert out == "Hi !"


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.PandaDocDocumentCreateComponent(
            asset_name="x",
            template_uuid="tpl-123",
            recipient_email_column="email",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.PandaDocDocumentCreateComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            template_uuid="tpl-123",
            recipient_email_column="email",
        ).build_defs(context=None)


def test_empty_template_uuid_raises(mod):
    with pytest.raises(ValueError, match="template_uuid must be non-empty"):
        mod.PandaDocDocumentCreateComponent(
            asset_name="x",
            upstream_asset_key="foo",
            template_uuid="",
            recipient_email_column="email",
        ).build_defs(context=None)


def test_missing_recipient_email_column_raises_failure(mod, recorded_calls):
    df = pd.DataFrame({"other_col": ["a"]})
    resource = FakePandaDocResource()
    component = mod.PandaDocDocumentCreateComponent(
        asset_name="pandadoc_out",
        upstream_asset_key="upstream_contracts",
        template_uuid="tpl-123",
        recipient_email_column="email",
    )
    with pytest.raises(dg.Failure, match="Columns not in upstream"):
        _materialize(component, df, resource)


def test_missing_tokens_map_column_raises_failure(mod, recorded_calls):
    df = pd.DataFrame({"email": ["a@test.com"]})
    resource = FakePandaDocResource()
    component = mod.PandaDocDocumentCreateComponent(
        asset_name="pandadoc_out",
        upstream_asset_key="upstream_contracts",
        template_uuid="tpl-123",
        recipient_email_column="email",
        tokens_map={"contract_value": "contract.value"},
    )
    with pytest.raises(dg.Failure, match="Columns not in upstream"):
        _materialize(component, df, resource)


# --- end-to-end create behavior, against the monkeypatched API call ------

def test_create_end_to_end(mod, recorded_calls):
    df = pd.DataFrame(
        {
            "email": ["jane@test.com", "bob@test.com"],
            "first_name": ["Jane", "Bob"],
            "last_name": ["Doe", "Smith"],
            "contract_type": ["MSA", "NDA"],
            "customer_name": ["Acme", "Globex"],
        }
    )
    resource = FakePandaDocResource()
    component = mod.PandaDocDocumentCreateComponent(
        asset_name="pandadoc_out",
        upstream_asset_key="upstream_contracts",
        template_uuid="tpl-123",
        name_template="{contract_type} Agreement - {customer_name}",
        recipient_email_column="email",
        recipient_first_name_column="first_name",
        recipient_last_name_column="last_name",
        recipient_role="Client",
    )
    result = _materialize(component, df, resource)
    assert result.success

    assert len(recorded_calls) == 2
    assert recorded_calls[0]["template_uuid"] == "tpl-123"
    assert recorded_calls[0]["name"] == "MSA Agreement - Acme"
    assert recorded_calls[0]["recipients"] == [
        {"email": "jane@test.com", "first_name": "Jane", "last_name": "Doe", "role": "Client"}
    ]
    assert recorded_calls[0]["auto_send"] is True

    out = metadata_for(result, "pandadoc_out")
    assert out["documents_created"] == 2
    assert out["documents_failed"] == 0
    assert out["rows_skipped_no_recipient"] == 0
    assert "first_document_ids" in out


def test_tokens_map_prefills_tokens(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@test.com"], "contract_value": [50000]})
    resource = FakePandaDocResource()
    component = mod.PandaDocDocumentCreateComponent(
        asset_name="pandadoc_out",
        upstream_asset_key="upstream_contracts",
        template_uuid="tpl-123",
        recipient_email_column="email",
        tokens_map={"contract_value": "contract.value"},
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls[0]["tokens"] == [{"name": "contract.value", "value": "50000"}]


def test_auto_send_false_passed_through(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@test.com"]})
    resource = FakePandaDocResource()
    component = mod.PandaDocDocumentCreateComponent(
        asset_name="pandadoc_out",
        upstream_asset_key="upstream_contracts",
        template_uuid="tpl-123",
        recipient_email_column="email",
        auto_send=False,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls[0]["auto_send"] is False
    out = metadata_for(result, "pandadoc_out")
    assert out["auto_send"] is False


def test_empty_upstream_creates_nothing(mod, recorded_calls):
    df = pd.DataFrame({"email": []})
    resource = FakePandaDocResource()
    component = mod.PandaDocDocumentCreateComponent(
        asset_name="pandadoc_out",
        upstream_asset_key="upstream_contracts",
        template_uuid="tpl-123",
        recipient_email_column="email",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "pandadoc_out")
    assert out["documents_created"] == 0


def test_blank_and_null_recipients_skipped_and_counted(mod, recorded_calls):
    df = pd.DataFrame({"email": ["jane@test.com", None, "  ", ""]})
    resource = FakePandaDocResource()
    component = mod.PandaDocDocumentCreateComponent(
        asset_name="pandadoc_out",
        upstream_asset_key="upstream_contracts",
        template_uuid="tpl-123",
        recipient_email_column="email",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 1
    out = metadata_for(result, "pandadoc_out")
    assert out["documents_created"] == 1
    assert out["rows_skipped_no_recipient"] == 3


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"email": [f"user{i}@test.com" for i in range(10)]})
    resource = FakePandaDocResource()
    component = mod.PandaDocDocumentCreateComponent(
        asset_name="pandadoc_out",
        upstream_asset_key="upstream_contracts",
        template_uuid="tpl-123",
        recipient_email_column="email",
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 3


def test_source_inline_mode(mod, recorded_calls):
    resource = FakePandaDocResource()
    component = mod.PandaDocDocumentCreateComponent(
        asset_name="pandadoc_out",
        source={"kind": "inline", "rows": [{"email": "jane@test.com"}]},
        template_uuid="tpl-123",
        recipient_email_column="email",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"pandadoc_resource": resource})
    assert result.success
    out = metadata_for(result, "pandadoc_out")
    assert out["documents_created"] == 1


# --- per-row failure isolation --------------------------------------------

def test_one_row_failing_does_not_abort_run(mod, monkeypatch):
    def _flaky_call(resource, name, template_uuid, recipients, tokens, auto_send, send_message, poll_interval_seconds, poll_timeout_seconds):
        if recipients[0]["email"] == "bob@test.com":
            raise RuntimeError("PandaDoc: document.error_processing")
        return {"id": "doc-1"}

    monkeypatch.setattr(mod, "_call_pandadoc_api", _flaky_call)

    df = pd.DataFrame({"email": ["jane@test.com", "bob@test.com", "carl@test.com"]})
    resource = FakePandaDocResource()
    component = mod.PandaDocDocumentCreateComponent(
        asset_name="pandadoc_out",
        upstream_asset_key="upstream_contracts",
        template_uuid="tpl-123",
        recipient_email_column="email",
    )
    result = _materialize(component, df, resource)
    assert result.success  # a per-row failure must not crash the whole run

    out = metadata_for(result, "pandadoc_out")
    assert out["documents_created"] == 2
    assert out["documents_failed"] == 1
    assert "first_errors" in out


# --- real internal orchestration (create -> wait -> send), unmocked ------

def test_call_pandadoc_api_orchestrates_create_wait_send(mod):
    """Exercises `_call_pandadoc_api` itself (not monkeypatched) against a
    fake resource, so the create -> wait_until_draft -> send sequencing is
    verified for real rather than assumed."""
    calls = []

    class _TrackingResource(FakePandaDocResource):
        def create_document(self, **kwargs):
            calls.append(("create", kwargs))
            return {"id": "doc-xyz", "status": "document.uploaded"}

        def wait_until_draft(self, document_id, **kwargs):
            calls.append(("wait", document_id))
            return "document.draft"

        def send_document(self, document_id, **kwargs):
            calls.append(("send", document_id, kwargs))
            return {"id": document_id, "status": "document.sent"}

    result = mod._call_pandadoc_api(
        _TrackingResource(),
        name="Doc Name",
        template_uuid="tpl-123",
        recipients=[{"email": "jane@test.com"}],
        tokens=None,
        auto_send=True,
        send_message="hello",
        poll_interval_seconds=0.01,
        poll_timeout_seconds=1.0,
    )
    assert [c[0] for c in calls] == ["create", "wait", "send"]
    assert calls[1][1] == "doc-xyz"
    assert calls[2][1] == "doc-xyz"
    assert calls[2][2]["message"] == "hello"
    assert result["send_result"]["status"] == "document.sent"


def test_call_pandadoc_api_skips_wait_and_send_when_auto_send_false(mod):
    calls = []

    class _TrackingResource(FakePandaDocResource):
        def create_document(self, **kwargs):
            calls.append("create")
            return {"id": "doc-xyz"}

        def wait_until_draft(self, document_id, **kwargs):
            calls.append("wait")
            return "document.draft"

        def send_document(self, document_id, **kwargs):
            calls.append("send")
            return {}

    result = mod._call_pandadoc_api(
        _TrackingResource(),
        name="Doc Name",
        template_uuid="tpl-123",
        recipients=[{"email": "jane@test.com"}],
        tokens=None,
        auto_send=False,
        send_message=None,
        poll_interval_seconds=0.01,
        poll_timeout_seconds=1.0,
    )
    assert calls == ["create"]
    assert result == {"id": "doc-xyz"}
