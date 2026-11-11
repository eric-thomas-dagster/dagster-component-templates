"""Document source contract: every document-source component in this repo (document_corpus,
the new web-crawl/notion/confluence/cloud-storage document sources, ...) emits a DataFrame with
doc_id/content/title/url/source_type/updated_at/content_hash, tags itself kinds=['rag',
'documents'], and reports a `dagster/column_schema` metadata entry -- so a knowledge-base UI can
treat every source the same way. Also covers real .pdf/.docx/.html text extraction (pdfplumber/
python-docx/BeautifulSoup, not mocked) and the preview-default/fail-loudly changes.
"""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
DocumentCorpusComponent = mod.DocumentCorpusComponent


def _materialize(comp, **kwargs):
    kwargs.setdefault("raise_on_error", True)
    defs = comp.build_defs(None)
    return dg.materialize([*defs.assets, *(defs.asset_checks or [])], **kwargs)


def _write(path, text):
    path.write_text(text, encoding="utf-8")


def test_document_source_contract_columns_present(tmp_path):
    _write(tmp_path / "a.md", "# Hello\n\nWorld")
    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.md")
    result = _materialize(comp)
    df = result.asset_value("corpus")
    for col in ("doc_id", "content", "title", "url", "source_type", "updated_at", "content_hash"):
        assert col in df.columns, col
    assert df.iloc[0]["source_type"] == "local_file"
    assert df.iloc[0]["url"].startswith("file://")
    assert df.iloc[0]["title"] == "a"


def test_kinds_default_to_rag_and_documents(tmp_path):
    _write(tmp_path / "a.md", "hello")
    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.md")
    defs = comp.build_defs(None)
    asset = defs.assets[0]
    key = next(iter(asset.keys))
    tags = asset.specs_by_key[key].tags
    assert "dagster/kind/rag" in tags
    assert "dagster/kind/documents" in tags


def test_column_schema_metadata_present(tmp_path):
    _write(tmp_path / "a.md", "hello")
    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.md")
    result = _materialize(comp)
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "dagster/column_schema" in meta


def test_preview_metadata_defaults_to_enabled(tmp_path):
    _write(tmp_path / "a.md", "hello")
    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.md")
    result = _materialize(comp)
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert "preview" in meta


def test_empty_file_glob_raises(tmp_path):
    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="")
    with pytest.raises(ValueError, match="file_glob"):
        comp.build_defs(None)


def test_missing_source_dir_fails_loudly(tmp_path):
    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path / "nope"), file_glob="*.md")
    result = _materialize(comp, raise_on_error=False)
    assert not result.success


def test_pdf_extraction_real_file(tmp_path):
    from reportlab.pdfgen import canvas

    pdf_path = tmp_path / "doc.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.drawString(100, 750, "Hello from a real PDF")
    c.save()

    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.pdf")
    result = _materialize(comp)
    df = result.asset_value("corpus")
    assert len(df) == 1
    assert "Hello from a real PDF" in df.iloc[0]["content"]
    assert df.iloc[0]["source_type"] == "local_file"


def test_docx_extraction_real_file(tmp_path):
    import docx

    docx_path = tmp_path / "doc.docx"
    d = docx.Document()
    d.add_paragraph("Hello from a real docx file")
    d.save(str(docx_path))

    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.docx")
    result = _materialize(comp)
    df = result.asset_value("corpus")
    assert len(df) == 1
    assert "Hello from a real docx file" in df.iloc[0]["content"]


def test_html_extraction_strips_tags(tmp_path):
    _write(tmp_path / "doc.html", "<html><body><h1>Title</h1><p>Body text here.</p></body></html>")
    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.html")
    result = _materialize(comp)
    df = result.asset_value("corpus")
    assert "Title" in df.iloc[0]["content"]
    assert "Body text here." in df.iloc[0]["content"]
    assert "<h1>" not in df.iloc[0]["content"]


def test_updated_at_reflects_file_mtime_not_ingestion_time(tmp_path):
    import os
    import time

    p = tmp_path / "a.md"
    _write(p, "hello")
    old_mtime = time.time() - 3600 * 24 * 5  # 5 days ago
    os.utime(p, (old_mtime, old_mtime))

    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.md")
    result = _materialize(comp)
    df = result.asset_value("corpus")
    assert df.iloc[0]["updated_at"] != df.iloc[0]["ingested_at"]
