"""Tests for CloudDocumentsComponent.

provider='s3' is exercised against a real local directory via fsspec's `file://` scheme --
a completely real, valid fsspec filesystem that needs no mocking (same convention
document_ingestion's own tests use). provider='onedrive' is exercised with
`requests.post`/`requests.get` mocked (the OAuth2 token exchange, the Graph listing call, and
the per-file content download). `dlt.pipeline(...)` is faked for every test (see conftest.py);
the REAL `@dlt.resource` object this component builds is still exercised by iterating
`fake_pipeline.run_calls[-1]`.
"""
import hashlib

import dagster as dg
import pandas as pd
import pydantic
import pytest

from .conftest import FakeResponse, install_fake_dlt, install_fake_requests, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def _write(path, text):
    path.write_text(text, encoding="utf-8")


# --- provider validation ---------------------------------------------------------------

def test_unknown_provider_raises(mod):
    with pytest.raises(pydantic.ValidationError):
        mod.CloudDocumentsComponent(asset_name="docs", provider="dropcloud")


def test_s3_without_source_path_raises(mod):
    with pytest.raises(pydantic.ValidationError, match="source_path"):
        mod.CloudDocumentsComponent(asset_name="docs", provider="s3")


def test_onedrive_missing_required_fields_raises(mod):
    with pytest.raises(pydantic.ValidationError, match="tenant_id"):
        mod.CloudDocumentsComponent(asset_name="docs", provider="onedrive")


def test_box_missing_required_fields_raises(mod):
    with pytest.raises(pydantic.ValidationError, match="box_subject"):
        mod.CloudDocumentsComponent(
            asset_name="docs", provider="box", client_id="c", client_secret="s"
        )


def test_webdav_without_base_url_raises(mod):
    with pytest.raises(pydantic.ValidationError, match="webdav_base_url"):
        mod.CloudDocumentsComponent(asset_name="docs", provider="webdav")


# --- s3 (via file:// -- real fsspec, no mocking) ----------------------------------------

def test_s3_document_source_contract_columns(mod, monkeypatch, tmp_path):
    _write(tmp_path / "a.md", "hello world")
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        include_preview_metadata=False,
    )
    result = _materialize(component)
    assert result.success

    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    row = rows[0]
    for col in ("doc_id", "content", "title", "url", "source_type", "updated_at", "content_hash"):
        assert col in row, col
    assert row["content"] == "hello world"
    assert row["title"] == "a"
    assert row["source_type"] == "s3"
    assert row["url"].startswith("file://")
    assert row["content_hash"] == hashlib.sha256(b"hello world").hexdigest()


def test_s3_kinds_include_rag_and_documents(mod, monkeypatch, tmp_path):
    _write(tmp_path / "a.md", "hello")
    install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
    )
    defs = component.build_defs(context=None)
    asset = defs.assets[0]
    key = next(iter(asset.keys))
    tags = asset.specs_by_key[key].tags
    assert "dagster/kind/rag" in tags
    assert "dagster/kind/documents" in tags
    assert "dagster/kind/s3" in tags


def test_s3_pdf_extraction_real_file(mod, monkeypatch, tmp_path):
    from reportlab.pdfgen import canvas

    pdf_path = tmp_path / "doc.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.drawString(100, 750, "Hello from a real PDF")
    c.save()

    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        include_preview_metadata=False,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert "Hello from a real PDF" in rows[0]["content"]


def test_s3_docx_extraction_real_file(mod, monkeypatch, tmp_path):
    import docx

    docx_path = tmp_path / "doc.docx"
    d = docx.Document()
    d.add_paragraph("Hello from a real docx file")
    d.save(str(docx_path))

    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        include_preview_metadata=False,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert "Hello from a real docx file" in rows[0]["content"]


def test_s3_html_extraction_strips_tags(mod, monkeypatch, tmp_path):
    _write(tmp_path / "doc.html", "<html><body><h1>Title</h1><p>Body text here.</p></body></html>")
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        include_preview_metadata=False,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert "Title" in rows[0]["content"]
    assert "Body text here." in rows[0]["content"]
    assert "<h1>" not in rows[0]["content"]


def test_s3_extraction_failure_skips_not_crashes(mod, monkeypatch, tmp_path):
    # A .pdf extension with bytes that are NOT a real PDF -- pdfplumber raises on open,
    # and the component must skip it (log + metadata), not crash the whole run.
    (tmp_path / "broken.pdf").write_bytes(b"not a real pdf")
    _write(tmp_path / "good.md", "this one is fine")
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        include_preview_metadata=False,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert rows[0]["title"] == "good"

    materialization = result.get_asset_materialization_events()[-1].materialization
    assert materialization.metadata["skipped_files"].value == 1
    assert materialization.metadata["documents_extracted"].value == 1


def test_s3_missing_source_path_dir_fails_loudly(mod, monkeypatch, tmp_path):
    install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path / 'does-not-exist'}",
    )
    asset_def = component.build_defs(context=None).assets[0]
    materialize_result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False
    )
    assert not materialize_result.success


# --- onedrive (requests.post/get mocked) ------------------------------------------------

def _build_onedrive_component(mod, **overrides):
    defaults = dict(
        asset_name="onedrive_docs",
        provider="onedrive",
        tenant_id="tenant-123",
        client_id="client-123",
        client_secret="secret-123",
        drive_id="drive-abc",
        include_preview_metadata=False,
    )
    defaults.update(overrides)
    return mod.CloudDocumentsComponent(**defaults)


def test_onedrive_document_source_contract_columns(mod, monkeypatch):
    def fake_post(url, data=None, **kwargs):
        assert "login.microsoftonline.com/tenant-123/oauth2/v2.0/token" in url
        assert data["client_id"] == "client-123"
        assert data["client_secret"] == "secret-123"
        return FakeResponse(json_body={"access_token": "graph-token-xyz"})

    def fake_get(url, headers=None, **kwargs):
        if "root/children" in url:
            assert headers["Authorization"] == "Bearer graph-token-xyz"
            return FakeResponse(json_body={"value": [
                {
                    "id": "item-1",
                    "name": "report.md",
                    "file": {"mimeType": "text/markdown"},
                    "webUrl": "https://contoso.sharepoint.com/report.md",
                    "lastModifiedDateTime": "2026-01-01T00:00:00Z",
                    "@microsoft.graph.downloadUrl": "https://download.example.com/report.md",
                },
                {
                    "id": "folder-1",
                    "name": "Subfolder",
                    "folder": {"childCount": 2},
                },
            ]})
        if url == "https://download.example.com/report.md":
            return FakeResponse(content=b"# Report\n\nhello from onedrive")
        raise AssertionError(f"unexpected GET {url}")

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_onedrive_component(mod)
    result = _materialize(component)
    assert result.success

    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    row = rows[0]
    for col in ("doc_id", "content", "title", "url", "source_type", "updated_at", "content_hash"):
        assert col in row, col
    assert row["doc_id"] == "item-1"
    assert row["content"] == "# Report\n\nhello from onedrive"
    assert row["title"] == "report"
    assert row["url"] == "https://contoso.sharepoint.com/report.md"
    assert row["source_type"] == "onedrive"
    assert row["updated_at"] == "2026-01-01T00:00:00Z"
    assert row["content_hash"] == hashlib.sha256(b"# Report\n\nhello from onedrive").hexdigest()


def test_onedrive_download_failure_skips_not_crashes(mod, monkeypatch):
    def fake_post(url, data=None, **kwargs):
        return FakeResponse(json_body={"access_token": "tok"})

    def fake_get(url, headers=None, **kwargs):
        if "root/children" in url:
            return FakeResponse(json_body={"value": [
                {
                    "id": "item-1",
                    "name": "broken.md",
                    "file": {},
                    "@microsoft.graph.downloadUrl": "https://download.example.com/broken.md",
                },
            ]})
        if url == "https://download.example.com/broken.md":
            import requests

            return FakeResponse(status_code=500, raise_exc=requests.HTTPError("boom"))
        raise AssertionError(f"unexpected GET {url}")

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_onedrive_component(mod)
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert rows == []
    materialization = result.get_asset_materialization_events()[-1].materialization
    assert materialization.metadata["skipped_files"].value == 1


# --- write_disposition: resource hints (fake-dlt, real .apply_hints()) -----------------

def test_default_write_disposition_is_replace(mod, monkeypatch, tmp_path):
    _write(tmp_path / "a.md", "hello world")
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        include_preview_metadata=False,
    )
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.write_disposition == "replace"
    materialization = result.get_asset_materialization_events()[-1].materialization
    assert materialization.metadata["write_disposition"].value == "replace"


def test_merge_write_disposition_sets_primary_key(mod, monkeypatch, tmp_path):
    _write(tmp_path / "a.md", "hello world")
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        include_preview_metadata=False, write_disposition="merge",
    )
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.write_disposition == "merge"
    assert resource.compute_table_schema()["columns"]["doc_id"]["primary_key"] is True


def test_append_write_disposition_is_explicit_opt_in(mod, monkeypatch, tmp_path):
    _write(tmp_path / "a.md", "hello world")
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        include_preview_metadata=False, write_disposition="append",
    )
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.write_disposition == "append"


# --- _dedupe_by_doc_id: pure-function safety net ---------------------------------------

def test_dedupe_by_doc_id_keeps_latest_updated_at(mod):
    df = pd.DataFrame([
        {"doc_id": "doc-a", "updated_at": "2024-01-01", "content": "old"},
        {"doc_id": "doc-a", "updated_at": "2024-02-01", "content": "new"},
        {"doc_id": "doc-b", "updated_at": "2024-01-01", "content": "b"},
    ])
    deduped, removed = mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 2
    assert deduped.set_index("doc_id").loc["doc-a", "content"] == "new"


def test_dedupe_by_doc_id_no_doc_id_column_is_a_no_op(mod):
    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    deduped, removed = mod._dedupe_by_doc_id(df)
    assert removed == 0
    assert len(deduped) == 2
