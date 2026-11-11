"""PPTX/XLSX extraction via Microsoft's markitdown library -- formats pdfplumber/python-docx/
BeautifulSoup don't cover. provider='s3' exercised against a real local directory via fsspec's
`file://` scheme (same convention as test_cloud_documents.py), real python-pptx/openpyxl files,
real markitdown conversion -- nothing mocked except dlt.pipeline.
"""
import dagster as dg
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _markitdown_available():
    try:
        import markitdown  # noqa: F401
        import pptx  # noqa: F401
        import openpyxl  # noqa: F401
        return True
    except ImportError:
        return False


requires_markitdown = pytest.mark.skipif(not _markitdown_available(), reason="requires markitdown[pptx,xlsx]")


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


@requires_markitdown
def test_s3_pptx_extraction_real_file(mod, monkeypatch, tmp_path):
    from pptx import Presentation

    pptx_path = tmp_path / "deck.pptx"
    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = "A Real Slide Title"
    prs.save(str(pptx_path))

    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        include_preview_metadata=False,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert "A Real Slide Title" in rows[0]["content"]


@requires_markitdown
def test_s3_xlsx_extraction_real_file(mod, monkeypatch, tmp_path):
    import openpyxl

    xlsx_path = tmp_path / "data.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = "Name"
    ws["A2"] = "Alice"
    wb.save(str(xlsx_path))

    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.CloudDocumentsComponent(
        asset_name="docs", provider="s3", source_path=f"file://{tmp_path}",
        include_preview_metadata=False,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert "Alice" in rows[0]["content"]
