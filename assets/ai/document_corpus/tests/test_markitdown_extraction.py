"""PPTX/XLSX extraction via Microsoft's markitdown library -- formats pdfplumber/python-docx/
BeautifulSoup don't cover. Real files built with python-pptx/openpyxl, real markitdown conversion,
nothing mocked.
"""
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()
DocumentCorpusComponent = mod.DocumentCorpusComponent


def _markitdown_available():
    try:
        import markitdown  # noqa: F401
        import pptx  # noqa: F401
        import openpyxl  # noqa: F401
        return True
    except ImportError:
        return False


requires_markitdown = pytest.mark.skipif(not _markitdown_available(), reason="requires markitdown[pptx,xlsx]")


def _materialize(comp, **kwargs):
    kwargs.setdefault("raise_on_error", True)
    defs = comp.build_defs(None)
    return dg.materialize([*defs.assets, *(defs.asset_checks or [])], **kwargs)


@requires_markitdown
def test_pptx_extraction_real_file(tmp_path):
    from pptx import Presentation

    pptx_path = tmp_path / "deck.pptx"
    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = "A Real Slide Title"
    prs.save(str(pptx_path))

    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.pptx")
    result = _materialize(comp)
    df = result.output_for_node("corpus")
    assert len(df) == 1
    assert "A Real Slide Title" in df.iloc[0]["content"]


@requires_markitdown
def test_xlsx_extraction_real_file(tmp_path):
    import openpyxl

    xlsx_path = tmp_path / "data.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = "Name"
    ws["B1"] = "Score"
    ws["A2"] = "Alice"
    ws["B2"] = 95
    wb.save(str(xlsx_path))

    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.xlsx")
    result = _materialize(comp)
    df = result.output_for_node("corpus")
    assert len(df) == 1
    content = df.iloc[0]["content"]
    assert "Alice" in content and "95" in content


def test_missing_markitdown_raises_a_clear_import_error(tmp_path, monkeypatch):
    import builtins

    (tmp_path / "deck.pptx").write_bytes(b"not a real pptx, just needs the extension to route here")
    real_import = builtins.__import__

    def _blocked_import(name, *a, **kw):
        if name == "markitdown":
            raise ImportError("No module named 'markitdown'")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", _blocked_import)
    comp = DocumentCorpusComponent(asset_name="corpus", source_dir=str(tmp_path), file_glob="*.pptx")
    with pytest.raises(Exception, match="markitdown"):
        _materialize(comp)
