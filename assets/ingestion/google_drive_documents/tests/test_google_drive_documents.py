"""Committed tests for GoogleDriveDocumentsComponent.

Mocks only the Drive API network boundary (service_account.Credentials, googleapiclient
build/MediaIoBaseDownload -- see conftest.py); every extraction library (pdfplumber,
python-docx, BeautifulSoup, markitdown) runs for real against real fixture bytes (a real PDF
via reportlab, a real .docx via python-docx, a real .pptx via python-pptx, a real .xlsx via
openpyxl), never faked.

Real behavior exercised: native Google formats (Docs/Sheets/Slides) are EXPORTED via
`files().export(fileId=..., mimeType=...)`, never `get_media` -- Docs/Slides export as
text/plain, Sheets export as text/csv. Everything else downloads raw via `get_media` +
`MediaIoBaseDownload` and is routed through the same extraction dispatcher as
document_corpus/cloud_documents/document_ingestion.
"""
import io

import dagster as dg
import pytest

from .conftest import install_fake_google_drive, load_component_module

mod = load_component_module()
GoogleDriveDocumentsComponent = mod.GoogleDriveDocumentsComponent

FOLDER = "folder123"


def _component(**kw):
    attrs = dict(
        asset_name="drive_docs",
        credentials={"type": "service_account", "client_email": "sa@example.com"},
        drive_folder_id=FOLDER,
    )
    attrs.update(kw)
    return GoogleDriveDocumentsComponent(**attrs)


def _materialize(comp, **kwargs):
    kwargs.setdefault("raise_on_error", True)
    defs = comp.build_defs(None)
    return dg.materialize(defs.assets, **kwargs)


def _row(df, doc_id):
    hits = df[df["doc_id"] == doc_id]
    assert len(hits) == 1, f"expected exactly one row for {doc_id!r}, got {len(hits)}"
    return hits.iloc[0]


# ── Fixture builders (real bytes, real libraries) ──────────────────────────────────────────

def _make_pdf_bytes(text: str) -> bytes:
    from reportlab.pdfgen import canvas

    buf = io.BytesIO()
    c = canvas.Canvas(buf)
    c.drawString(100, 700, text)
    c.save()
    return buf.getvalue()


def _make_docx_bytes(paragraphs) -> bytes:
    import docx

    document = docx.Document()
    for p in paragraphs:
        document.add_paragraph(p)
    buf = io.BytesIO()
    document.save(buf)
    return buf.getvalue()


def _make_pptx_bytes(title: str) -> bytes:
    from pptx import Presentation

    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = title
    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


def _make_xlsx_bytes(sheet_name: str, rows) -> bytes:
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── Document source contract + native-format export routing ───────────────────────────────

def test_document_source_contract_columns(monkeypatch):
    files = [{
        "id": "doc1", "name": "Project Plan", "mimeType": "application/vnd.google-apps.document",
        "modifiedTime": "2026-01-01T00:00:00Z", "webViewLink": "https://docs.google.com/document/d/doc1/edit",
    }]
    install_fake_google_drive(
        monkeypatch, {FOLDER: files},
        export_by_id={("doc1", "text/plain"): b"Plan contents here."},
    )
    comp = _component()
    result = _materialize(comp)
    df = result.output_for_node("drive_docs")
    assert list(df.columns) == ["doc_id", "content", "title", "url", "source_type", "updated_at", "content_hash"]
    row = _row(df, "doc1")
    assert row["content"] == "Plan contents here."
    assert row["title"] == "Project Plan"
    assert row["url"] == "https://docs.google.com/document/d/doc1/edit"
    assert row["source_type"] == "google_drive"
    assert row["updated_at"] == "2026-01-01T00:00:00Z"
    import hashlib
    assert row["content_hash"] == hashlib.sha256(b"Plan contents here.").hexdigest()


def test_google_doc_and_slides_export_as_text_plain(monkeypatch):
    files = [
        {"id": "doc1", "name": "A Doc", "mimeType": "application/vnd.google-apps.document", "modifiedTime": "t"},
        {"id": "slides1", "name": "A Deck", "mimeType": "application/vnd.google-apps.presentation", "modifiedTime": "t"},
    ]
    install_fake_google_drive(
        monkeypatch, {FOLDER: files},
        export_by_id={
            ("doc1", "text/plain"): b"doc body",
            ("slides1", "text/plain"): b"slide body",
        },
    )
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    assert _row(df, "doc1")["content"] == "doc body"
    assert _row(df, "slides1")["content"] == "slide body"


def test_google_sheet_exports_as_csv(monkeypatch):
    files = [{"id": "sheet1", "name": "Budget", "mimeType": "application/vnd.google-apps.spreadsheet", "modifiedTime": "t"}]
    install_fake_google_drive(
        monkeypatch, {FOLDER: files},
        export_by_id={("sheet1", "text/csv"): b"col_a,col_b\n1,2\n"},
    )
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    assert "col_a,col_b" in _row(df, "sheet1")["content"]


def test_unsupported_native_mimetype_is_skipped(monkeypatch):
    files = [{"id": "form1", "name": "A Form", "mimeType": "application/vnd.google-apps.form", "modifiedTime": "t"}]
    install_fake_google_drive(monkeypatch, {FOLDER: files})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["skipped_files"].value == 1
    assert "unsupported native" in meta["skipped_files_detail"].value[0]["reason"]


# ── Raw downloads (get_media) + extraction dispatch ────────────────────────────────────────

def test_plain_text_file_downloaded_and_extracted(monkeypatch):
    files = [{"id": "f1", "name": "notes.txt", "mimeType": "text/plain", "size": "11", "modifiedTime": "t"}]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f1": b"hello world"})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    assert _row(df, "f1")["content"] == "hello world"


def test_pdf_extraction_real_pdf(monkeypatch):
    data = _make_pdf_bytes("Hello from a real PDF fixture")
    files = [{"id": "f2", "name": "report.pdf", "mimeType": "application/pdf", "size": str(len(data)), "modifiedTime": "t"}]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f2": data})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    assert "Hello from a real PDF fixture" in _row(df, "f2")["content"]


def test_docx_extraction_real_docx(monkeypatch):
    data = _make_docx_bytes(["First paragraph.", "Second paragraph."])
    files = [{
        "id": "f3", "name": "memo.docx",
        "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "size": str(len(data)), "modifiedTime": "t",
    }]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f3": data})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    content = _row(df, "f3")["content"]
    assert "First paragraph." in content and "Second paragraph." in content


def test_pptx_extraction_via_markitdown_real_file(monkeypatch):
    data = _make_pptx_bytes("A Real Slide Title")
    files = [{
        "id": "f4", "name": "deck.pptx",
        "mimeType": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "size": str(len(data)), "modifiedTime": "t",
    }]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f4": data})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    assert "A Real Slide Title" in _row(df, "f4")["content"]


def test_xlsx_extraction_via_markitdown_real_file(monkeypatch):
    data = _make_xlsx_bytes("Sheet1", [["name", "amount"], ["Acme Corp", 42]])
    files = [{
        "id": "f5", "name": "data.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "size": str(len(data)), "modifiedTime": "t",
    }]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f5": data})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    content = _row(df, "f5")["content"]
    assert "Acme Corp" in content and "42" in content


def test_html_extraction_via_beautifulsoup(monkeypatch):
    html = b"<html><body><h1>Title</h1><p>Body text.</p></body></html>"
    files = [{"id": "f6", "name": "page.html", "mimeType": "text/html", "size": str(len(html)), "modifiedTime": "t"}]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f6": html})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    content = _row(df, "f6")["content"]
    assert "Title" in content and "Body text." in content


# ── Error handling: skip, don't emit a null-content row ────────────────────────────────────

def test_corrupt_pdf_bytes_is_skipped_not_a_null_row(monkeypatch):
    files = [{"id": "f7", "name": "broken.pdf", "mimeType": "application/pdf", "size": "7", "modifiedTime": "t"}]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f7": b"garbage"})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["skipped_files"].value == 1


def test_empty_extracted_content_is_dropped(monkeypatch):
    files = [{"id": "f8", "name": "empty.txt", "mimeType": "text/plain", "size": "0", "modifiedTime": "t"}]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f8": b"   "})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["skipped_files"].value == 1
    assert meta["skipped_files_detail"].value[0]["reason"] == "empty extracted content"


def test_download_max_bytes_skips_oversized_file(monkeypatch):
    files = [{"id": "f9", "name": "huge.txt", "mimeType": "text/plain", "size": "99999999", "modifiedTime": "t"}]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f9": b"would be huge"})
    result = _materialize(_component(download_max_bytes=100))
    df = result.output_for_node("drive_docs")
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "download_max_bytes" in meta["skipped_files_detail"].value[0]["reason"]


def test_missing_extension_falls_back_to_mimetype_map(monkeypatch):
    files = [{"id": "f10", "name": "readme", "mimeType": "text/plain", "size": "5", "modifiedTime": "t"}]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f10": b"hi!!!"})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    assert _row(df, "f10")["content"] == "hi!!!"


def test_no_extension_and_no_mimetype_mapping_is_skipped(monkeypatch):
    files = [{"id": "f11", "name": "mystery", "mimeType": "application/x-unknown-weird-type", "size": "5", "modifiedTime": "t"}]
    install_fake_google_drive(monkeypatch, {FOLDER: files}, media_by_id={"f11": b"xxxxx"})
    result = _materialize(_component())
    df = result.output_for_node("drive_docs")
    assert len(df) == 0


# ── Recursive subfolder walk ────────────────────────────────────────────────────────────────

def test_non_recursive_by_default_ignores_subfolder_contents(monkeypatch):
    root_files = [
        {"id": "top.txt", "name": "top.txt", "mimeType": "text/plain", "size": "3", "modifiedTime": "t"},
        {"id": "sub", "name": "Subfolder", "mimeType": "application/vnd.google-apps.folder", "modifiedTime": "t"},
    ]
    sub_files = [{"id": "nested.txt", "name": "nested.txt", "mimeType": "text/plain", "size": "3", "modifiedTime": "t"}]
    install_fake_google_drive(
        monkeypatch, {FOLDER: root_files, "sub": sub_files},
        media_by_id={"top.txt": b"top", "nested.txt": b"deep"},
    )
    result = _materialize(_component())  # recursive defaults to False
    df = result.output_for_node("drive_docs")
    assert set(df["doc_id"]) == {"top.txt"}


def test_recursive_true_walks_subfolder_contents(monkeypatch):
    root_files = [
        {"id": "top.txt", "name": "top.txt", "mimeType": "text/plain", "size": "3", "modifiedTime": "t"},
        {"id": "sub", "name": "Subfolder", "mimeType": "application/vnd.google-apps.folder", "modifiedTime": "t"},
    ]
    sub_files = [{"id": "nested.txt", "name": "nested.txt", "mimeType": "text/plain", "size": "3", "modifiedTime": "t"}]
    install_fake_google_drive(
        monkeypatch, {FOLDER: root_files, "sub": sub_files},
        media_by_id={"top.txt": b"top", "nested.txt": b"deep"},
    )
    result = _materialize(_component(recursive=True))
    df = result.output_for_node("drive_docs")
    assert set(df["doc_id"]) == {"top.txt", "nested.txt"}


# ── Config validation ───────────────────────────────────────────────────────────────────────

def test_drive_folder_id_is_required():
    with pytest.raises(Exception):
        GoogleDriveDocumentsComponent(
            asset_name="x", credentials={"client_email": "sa@example.com"},
        )


def test_missing_credentials_raises_a_clear_error(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    comp = GoogleDriveDocumentsComponent(asset_name="x", drive_folder_id=FOLDER)
    with pytest.raises(ValueError, match="credentials"):
        comp.build_defs(None)
