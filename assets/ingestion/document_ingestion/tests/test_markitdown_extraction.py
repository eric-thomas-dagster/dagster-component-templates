"""PPTX/XLSX extraction via Microsoft's markitdown library -- formats pdfplumber/python-docx/
BeautifulSoup don't cover. Real files built with python-pptx/openpyxl, real markitdown conversion,
nothing mocked.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


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


def _materialize(component, **kwargs):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, **kwargs)


@requires_markitdown
def test_real_pptx_extraction(mod, tmp_path):
    from pptx import Presentation

    pptx_path = tmp_path / "deck.pptx"
    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = "A Real Slide Title"
    prs.save(str(pptx_path))

    component = mod.DocumentIngestionComponent(asset_name="docs", source_path=str(tmp_path))
    result = _materialize(component)
    df = result.asset_value("docs")
    row = df[df["file_type"] == ".pptx"].iloc[0]
    assert "A Real Slide Title" in row["content"]


@requires_markitdown
def test_real_xlsx_extraction(mod, tmp_path):
    import openpyxl

    xlsx_path = tmp_path / "data.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = "Name"
    ws["A2"] = "Alice"
    wb.save(str(xlsx_path))

    component = mod.DocumentIngestionComponent(asset_name="docs", source_path=str(tmp_path))
    result = _materialize(component)
    df = result.asset_value("docs")
    row = df[df["file_type"] == ".xlsx"].iloc[0]
    assert "Alice" in row["content"]
