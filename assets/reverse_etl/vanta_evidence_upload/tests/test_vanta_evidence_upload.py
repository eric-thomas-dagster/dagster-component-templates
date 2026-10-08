"""Committed regression tests for VantaEvidenceUploadComponent.

No real network/`requests` calls are made here -- `_create_document` /
`_upload_file` / `_submit_document` (the three external, paid-API
boundaries) are monkeypatched wholesale, while file resolution (reading
from disk / decoding inline base64), dual source resolution, validation,
and per-row aggregation metadata are all exercised for real.
"""
import base64

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeVantaResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = {"create": [], "upload": [], "submit": []}

    def _fake_create(session, api_base_url, title, description, time_sensitivity, cadence, reminder_window, is_sensitive, timeout):
        calls["create"].append({
            "title": title, "description": description, "time_sensitivity": time_sensitivity,
            "cadence": cadence, "reminder_window": reminder_window, "is_sensitive": is_sensitive,
        })
        return f"doc-{len(calls['create'])}"

    def _fake_upload(session, api_base_url, document_id, file_bytes, file_name, mime_type, description, effective_at_date, timeout):
        calls["upload"].append({
            "document_id": document_id, "file_bytes": file_bytes, "file_name": file_name,
            "mime_type": mime_type, "description": description, "effective_at_date": effective_at_date,
        })
        return f"upload-{len(calls['upload'])}"

    def _fake_submit(session, api_base_url, document_id, timeout):
        calls["submit"].append({"document_id": document_id})

    monkeypatch.setattr(mod, "_create_document", _fake_create)
    monkeypatch.setattr(mod, "_upload_file", _fake_upload)
    monkeypatch.setattr(mod, "_submit_document", _fake_submit)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_evidence", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"vanta_resource": resource})


# --- pure helpers ----------------------------------------------------------

def test_row_value_treats_none_nan_and_blank_as_absent(mod):
    import math
    row = {"a": None, "b": float("nan"), "c": "  ", "d": "ok"}
    assert mod._row_value(row, "a") is None
    assert mod._row_value(row, "b") is None
    assert mod._row_value(row, "c") is None
    assert mod._row_value(row, "d") == "ok"
    assert mod._row_value(row, None) is None
    assert mod._row_value(row, "missing") is None


def test_guess_mime_type_prefers_override(mod):
    assert mod._guess_mime_type("file.pdf", "application/custom") == "application/custom"


def test_guess_mime_type_guesses_from_filename(mod):
    assert mod._guess_mime_type("report.pdf", None) == "application/pdf"


def test_guess_mime_type_falls_back_to_octet_stream(mod):
    assert mod._guess_mime_type("mystery", None) == "application/octet-stream"


def test_decode_inline_content_handles_base64(mod):
    raw = b"hello evidence bytes"
    encoded = base64.b64encode(raw).decode("ascii")
    assert mod._decode_inline_content(encoded) == raw


def test_decode_inline_content_falls_back_to_utf8_for_non_base64(mod):
    # Not valid base64 (odd characters/length) -> raw utf-8 bytes.
    assert mod._decode_inline_content("not base64!!") == "not base64!!".encode("utf-8")


def test_decode_inline_content_passes_through_bytes(mod):
    assert mod._decode_inline_content(b"\x00\x01raw") == b"\x00\x01raw"


# --- validation --------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.VantaEvidenceUploadComponent(
            asset_name="x",
            document_name="Doc",
            file_path_column="path",
        ).build_defs(context=None)


def test_both_file_path_and_content_column_raises(mod):
    with pytest.raises(ValueError, match="file_path_column"):
        mod.VantaEvidenceUploadComponent(
            asset_name="x",
            upstream_asset_key="foo",
            document_name="Doc",
            file_path_column="path",
            file_content_column="content",
        ).build_defs(context=None)


def test_neither_file_path_nor_content_column_raises(mod):
    with pytest.raises(ValueError, match="file_path_column"):
        mod.VantaEvidenceUploadComponent(
            asset_name="x",
            upstream_asset_key="foo",
            document_name="Doc",
        ).build_defs(context=None)


def test_missing_document_identity_raises(mod):
    with pytest.raises(ValueError, match="document_id_column"):
        mod.VantaEvidenceUploadComponent(
            asset_name="x",
            upstream_asset_key="foo",
            file_path_column="path",
        ).build_defs(context=None)


def test_invalid_time_sensitivity_raises(mod):
    with pytest.raises(ValueError, match="time_sensitivity"):
        mod.VantaEvidenceUploadComponent(
            asset_name="x",
            upstream_asset_key="foo",
            document_name="Doc",
            file_path_column="path",
            time_sensitivity="SOMETIMES",
        ).build_defs(context=None)


def test_invalid_cadence_raises(mod):
    with pytest.raises(ValueError, match="cadence"):
        mod.VantaEvidenceUploadComponent(
            asset_name="x",
            upstream_asset_key="foo",
            document_name="Doc",
            file_path_column="path",
            cadence="P4M",
        ).build_defs(context=None)


def test_invalid_reminder_window_raises(mod):
    with pytest.raises(ValueError, match="reminder_window"):
        mod.VantaEvidenceUploadComponent(
            asset_name="x",
            upstream_asset_key="foo",
            document_name="Doc",
            file_path_column="path",
            reminder_window="P1Y",  # valid for cadence, not for reminder_window
        ).build_defs(context=None)


# --- full asset body, against monkeypatched API calls ----------------------

def test_create_upload_submit_end_to_end_via_file_path(mod, recorded_calls, tmp_path):
    evidence_file = tmp_path / "access_review.pdf"
    evidence_file.write_bytes(b"%PDF-1.4 fake evidence")

    df = pd.DataFrame({
        "control_title": ["Access Review Q3"],
        "evidence_summary": ["Quarterly access review export"],
        "evidence_path": [str(evidence_file)],
    })
    component = mod.VantaEvidenceUploadComponent(
        asset_name="vanta_evidence_out",
        upstream_asset_key="upstream_evidence",
        document_name_column="control_title",
        evidence_description_column="evidence_summary",
        file_path_column="evidence_path",
    )
    result = _materialize(component, df, FakeVantaResource())
    assert result.success

    assert len(recorded_calls["create"]) == 1
    assert recorded_calls["create"][0]["title"] == "Access Review Q3"
    assert recorded_calls["create"][0]["description"] == "Quarterly access review export"
    assert recorded_calls["create"][0]["cadence"] == "P1Y"

    assert len(recorded_calls["upload"]) == 1
    assert recorded_calls["upload"][0]["document_id"] == "doc-1"
    assert recorded_calls["upload"][0]["file_bytes"] == b"%PDF-1.4 fake evidence"
    assert recorded_calls["upload"][0]["file_name"] == "access_review.pdf"
    assert recorded_calls["upload"][0]["mime_type"] == "application/pdf"

    assert len(recorded_calls["submit"]) == 1
    assert recorded_calls["submit"][0]["document_id"] == "doc-1"

    out = metadata_for(result, "vanta_evidence_out")
    assert out["documents_created"] == 1
    assert out["uploads_succeeded"] == 1
    assert out["documents_submitted"] == 1
    assert out["rows_errored"] == 0


def test_file_content_column_base64(mod, recorded_calls):
    raw_bytes = b"inline evidence content"
    encoded = base64.b64encode(raw_bytes).decode("ascii")
    df = pd.DataFrame({
        "control_title": ["Policy Doc"],
        "content": [encoded],
    })
    component = mod.VantaEvidenceUploadComponent(
        asset_name="vanta_evidence_out",
        upstream_asset_key="upstream_evidence",
        document_name_column="control_title",
        file_content_column="content",
    )
    result = _materialize(component, df, FakeVantaResource())
    assert result.success
    assert recorded_calls["upload"][0]["file_bytes"] == raw_bytes
    assert recorded_calls["upload"][0]["file_name"] == "Policy Doc.bin"


def test_document_id_column_skips_create(mod, recorded_calls):
    df = pd.DataFrame({
        "doc_id": ["existing-doc-42"],
        "content": [base64.b64encode(b"data").decode("ascii")],
    })
    component = mod.VantaEvidenceUploadComponent(
        asset_name="vanta_evidence_out",
        upstream_asset_key="upstream_evidence",
        document_id_column="doc_id",
        file_content_column="content",
    )
    result = _materialize(component, df, FakeVantaResource())
    assert result.success
    assert recorded_calls["create"] == []
    assert recorded_calls["upload"][0]["document_id"] == "existing-doc-42"
    out = metadata_for(result, "vanta_evidence_out")
    assert out["documents_created"] == 0
    assert out["uploads_succeeded"] == 1


def test_auto_submit_false_skips_submit(mod, recorded_calls):
    df = pd.DataFrame({
        "control_title": ["Doc"],
        "content": [base64.b64encode(b"data").decode("ascii")],
    })
    component = mod.VantaEvidenceUploadComponent(
        asset_name="vanta_evidence_out",
        upstream_asset_key="upstream_evidence",
        document_name_column="control_title",
        file_content_column="content",
        auto_submit=False,
    )
    result = _materialize(component, df, FakeVantaResource())
    assert result.success
    assert recorded_calls["submit"] == []
    out = metadata_for(result, "vanta_evidence_out")
    assert out["documents_submitted"] == 0
    assert out["uploads_succeeded"] == 1


def test_rows_skipped_no_file_and_no_identifier(mod, recorded_calls):
    df = pd.DataFrame({
        "control_title": ["Has Title", None, "Also Has Title"],
        "content": [None, base64.b64encode(b"x").decode("ascii"), base64.b64encode(b"y").decode("ascii")],
    })
    component = mod.VantaEvidenceUploadComponent(
        asset_name="vanta_evidence_out",
        upstream_asset_key="upstream_evidence",
        document_name_column="control_title",
        file_content_column="content",
    )
    result = _materialize(component, df, FakeVantaResource())
    assert result.success
    out = metadata_for(result, "vanta_evidence_out")
    # Row 0: has title, no content -> skipped_no_file.
    # Row 1: no title, no doc id -> skipped_no_identifier.
    # Row 2: has title and content -> succeeds.
    assert out["rows_skipped_no_file"] == 1
    assert out["rows_skipped_no_identifier"] == 1
    assert out["uploads_succeeded"] == 1


def test_upload_failure_is_recorded_as_error(mod, monkeypatch):
    calls = {"create": []}

    def _fake_create(session, api_base_url, title, description, time_sensitivity, cadence, reminder_window, is_sensitive, timeout):
        calls["create"].append(title)
        return "doc-1"

    def _fake_upload_raises(session, api_base_url, document_id, file_bytes, file_name, mime_type, description, effective_at_date, timeout):
        raise RuntimeError("Vanta upload failed: HTTP 500")

    monkeypatch.setattr(mod, "_create_document", _fake_create)
    monkeypatch.setattr(mod, "_upload_file", _fake_upload_raises)
    monkeypatch.setattr(mod, "_submit_document", lambda *a, **k: None)

    df = pd.DataFrame({
        "control_title": ["Broken Doc"],
        "content": [base64.b64encode(b"data").decode("ascii")],
    })
    component = mod.VantaEvidenceUploadComponent(
        asset_name="vanta_evidence_out",
        upstream_asset_key="upstream_evidence",
        document_name_column="control_title",
        file_content_column="content",
    )
    result = _materialize(component, df, FakeVantaResource())
    assert result.success  # the asset itself doesn't fail the run
    out = metadata_for(result, "vanta_evidence_out")
    assert out["rows_errored"] == 1
    assert out["uploads_succeeded"] == 0
    # Document creation succeeded before the upload failed.
    assert out["documents_created"] == 1
    assert "Broken Doc" in out["first_errors"][0]


def test_source_inline_mode(mod, recorded_calls):
    encoded = base64.b64encode(b"inline row content").decode("ascii")
    component = mod.VantaEvidenceUploadComponent(
        asset_name="vanta_evidence_out",
        source={"kind": "inline", "rows": [
            {"control_title": "Inline Doc", "content": encoded},
        ]},
        document_name_column="control_title",
        file_content_column="content",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"vanta_resource": FakeVantaResource()})
    assert result.success
    out = metadata_for(result, "vanta_evidence_out")
    assert out["uploads_succeeded"] == 1
