"""Real bugs fixed: (1) a missing/empty/unreachable source_path silently fell back to 5
hardcoded sample documents instead of raising -- now raises. (2) non-text files (.pdf/.docx/
.html) got a placeholder '[Document: name]' row with content_length=0 instead of real
extraction -- now uses pdfplumber/python-docx/BeautifulSoup (same as document_corpus),
skipping (not crashing on) a genuine extraction failure. (3) kinds' field description falsely
claimed auto-inference from the component name; it never did any such thing.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, source_path, **overrides):
    kwargs = dict(asset_name="docs", source_path=str(source_path))
    kwargs.update(overrides)
    return mod.DocumentIngestionComponent(**kwargs)


def _materialize(component, **kwargs):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, **kwargs)


def test_empty_source_path_raises_not_sample_fallback(mod):
    component = mod.DocumentIngestionComponent(asset_name="docs", source_path="")
    result = _materialize(component, raise_on_error=False)
    assert not result.success


def test_unreachable_source_path_raises_not_sample_fallback(mod, tmp_path):
    component = _make_component(mod, tmp_path / "does_not_exist")
    result = _materialize(component, raise_on_error=False)
    assert not result.success


def test_real_pdf_extraction_not_a_placeholder(mod, tmp_path):
    from reportlab.pdfgen import canvas

    pdf_path = tmp_path / "doc.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.drawString(100, 750, "Hello from a real PDF")
    c.save()

    component = _make_component(mod, tmp_path)
    result = _materialize(component, raise_on_error=True)
    df = result.output_for_node("docs")
    row = df[df["file_type"] == ".pdf"].iloc[0]
    assert "Hello from a real PDF" in row["content"]
    assert row["content_length"] > 0
    assert "needs_extraction" not in df.columns


def test_real_docx_extraction_not_a_placeholder(mod, tmp_path):
    import docx

    docx_path = tmp_path / "doc.docx"
    d = docx.Document()
    d.add_paragraph("Hello from a real docx file")
    d.save(str(docx_path))

    component = _make_component(mod, tmp_path)
    result = _materialize(component, raise_on_error=True)
    df = result.output_for_node("docs")
    row = df[df["file_type"] == ".docx"].iloc[0]
    assert "Hello from a real docx file" in row["content"]


def test_html_extraction_strips_tags(mod, tmp_path):
    (tmp_path / "doc.html").write_text("<html><body><h1>Title</h1><p>Body text.</p></body></html>")
    component = _make_component(mod, tmp_path)
    result = _materialize(component, raise_on_error=True)
    df = result.output_for_node("docs")
    row = df[df["file_type"] == ".html"].iloc[0]
    assert "Title" in row["content"]
    assert "Body text." in row["content"]
    assert "<h1>" not in row["content"]


def test_legacy_doc_is_skipped_not_crashed_and_counted(mod, tmp_path):
    (tmp_path / "legacy.doc").write_bytes(b"\xd0\xcf\x11\xe0 not real doc content")
    (tmp_path / "a.md").write_text("a real markdown doc")
    component = _make_component(mod, tmp_path)
    result = _materialize(component, raise_on_error=True)
    df = result.output_for_node("docs")
    assert ".doc" not in df["file_type"].tolist()
    assert len(df) == 1
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["skipped_files"].value == 1


def test_document_source_contract_columns_present(mod, tmp_path):
    (tmp_path / "a.md").write_text("hello")
    component = _make_component(mod, tmp_path)
    result = _materialize(component, raise_on_error=True)
    df = result.output_for_node("docs")
    for col in ("doc_id", "content", "title", "url", "source_type", "updated_at"):
        assert col in df.columns, col
    assert df.iloc[0]["source_type"] == "local_file"
    assert df.iloc[0]["title"] == "a"


def test_kinds_defaults_to_rag_and_documents(mod, tmp_path):
    (tmp_path / "a.md").write_text("hello")
    component = _make_component(mod, tmp_path)
    asset_def = component.build_defs(context=None).assets[0]
    key = next(iter(asset_def.keys))
    tags = asset_def.specs_by_key[key].tags
    assert "dagster/kind/rag" in tags
    assert "dagster/kind/documents" in tags


def test_empty_directory_returns_empty_df_not_sample_docs(mod, tmp_path):
    component = _make_component(mod, tmp_path)
    result = _materialize(component, raise_on_error=True)
    df = result.output_for_node("docs")
    assert len(df) == 0
    assert "sample" not in str(df.to_dict())
