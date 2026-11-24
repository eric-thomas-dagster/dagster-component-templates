"""Committed tests for DocumentWordBoxesComponent.

Real PDFs generated with reportlab (never mocked -- see this file's
`_make_word_pdf` / `_make_multi_page_pdf` / `_make_blank_pdf`), real
pdfplumber word extraction (never mocked). Nothing in this component calls
an LLM, so there is nothing to mock beyond the PDF/pdfplumber boundary
itself.

Covers: single/multi-page word explosion, `page_range` filtering, the
empty-text-layer (scanned-PDF) skip path, passthrough columns,
`find_text_bbox` matching a real multi-word snippet (including one that
spans a line break) and returning `None` for absent text, partitioning /
backfill_policy parity with sibling components, and the schema-change asset
check.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

pdfplumber = pytest.importorskip("pdfplumber")

from reportlab.lib.pagesizes import letter  # noqa: E402
from reportlab.pdfgen import canvas  # noqa: E402


@pytest.fixture()
def mod():
    return load_component_module()


def _make_word_pdf(path, lines, pagesize=letter):
    """One real PDF page with each string in `lines` drawn on its own row via
    drawString, so pdfplumber's text layer has real, exact words at known
    (if not pixel-precise) positions."""
    c = canvas.Canvas(str(path), pagesize=pagesize)
    y = 700
    for line in lines:
        c.drawString(72, y, line)
        y -= 20
    c.save()


def _make_multi_page_pdf(path, pages_lines, pagesize=letter):
    """One real multi-page PDF; `pages_lines` is a list of per-page line lists."""
    c = canvas.Canvas(str(path), pagesize=pagesize)
    for lines in pages_lines:
        y = 700
        for line in lines:
            c.drawString(72, y, line)
            y -= 20
        c.showPage()
    c.save()


def _make_blank_pdf(path, pagesize=letter):
    """One real PDF page with nothing drawn on it at all -- a stand-in for a
    scanned/photographed page with no embedded text layer (pdfplumber has
    real pages to open, but zero words to extract)."""
    c = canvas.Canvas(str(path), pagesize=pagesize)
    c.showPage()
    c.save()


def _materialize_one_row(mod, component, pdf_path, extra_cols=None):
    defs = component.build_defs(load_context=None)
    row = {"pdf_path": pdf_path}
    row.update(extra_cols or {})
    upstream_df = pd.DataFrame([row])

    @dg.asset(name=component.upstream_asset_key)
    def _upstream():
        return upstream_df

    return dg.materialize([*defs.assets, _upstream])


# --- core extraction -------------------------------------------------------------------------

def test_single_page_word_explode_real_text_and_positions(mod, tmp_path):
    pdf_path = tmp_path / "simple.pdf"
    _make_word_pdf(pdf_path, ["Hello World Foo"])

    component = mod.DocumentWordBoxesComponent(
        asset_name="words_out", upstream_asset_key="docs_in", column="pdf_path",
    )
    result = _materialize_one_row(mod, component, str(pdf_path), extra_cols={"doc_id": "doc-1"})
    assert result.success
    df = result.output_for_node("words_out")

    assert len(df) == 3, "3 whitespace-separated words on one page must explode to 3 rows"
    assert list(df["text"]) == ["Hello", "World", "Foo"]
    assert list(df["page_number"]) == [1, 1, 1]
    assert list(df["word_index"]) == [0, 1, 2]
    assert list(df["doc_id"]) == ["doc-1", "doc-1", "doc-1"], "passthrough column must be carried to every exploded row"
    assert list(df["confidence"]) == [1.0, 1.0, 1.0], "confidence is always a fixed 1.0 -- no real OCR probability exists for text-layer extraction"

    # Real geometry: x0 < x1, top < bottom, and each word is positioned to the right of the previous one.
    for _, row in df.iterrows():
        assert row["x0"] < row["x1"]
        assert row["top"] < row["bottom"]
    assert df.loc[0, "x0"] < df.loc[1, "x0"] < df.loc[2, "x0"]

    assert "pdf_path" not in df.columns, "the PDF column itself is not a passthrough column by default"


def test_multi_page_word_explode_resets_word_index_per_page(mod, tmp_path):
    pdf_path = tmp_path / "multipage.pdf"
    _make_multi_page_pdf(pdf_path, [["Alpha Beta"], ["Gamma Delta Epsilon"]])

    component = mod.DocumentWordBoxesComponent(
        asset_name="words_out", upstream_asset_key="docs_in", column="pdf_path",
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    df = result.output_for_node("words_out")

    assert len(df) == 5, "2 + 3 words across 2 pages must explode to 5 rows"
    assert list(df["page_number"]) == [1, 1, 2, 2, 2]
    assert list(df["word_index"]) == [0, 1, 0, 1, 2], "word_index is 0-based WITHIN each page, not global"
    assert list(df["text"]) == ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"]

    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["documents_processed"].value == 1
    assert meta["documents_skipped_no_text_layer"].value == 0
    assert meta["documents_failed"].value == 0


# --- page_range --------------------------------------------------------------------------------

def test_page_range_filters_to_specific_pages(mod, tmp_path):
    pdf_path = tmp_path / "threepage.pdf"
    _make_multi_page_pdf(
        pdf_path,
        [["PageOneWord"], ["PageTwoWord"], ["PageThreeWord"]],
    )

    component = mod.DocumentWordBoxesComponent(
        asset_name="words_out", upstream_asset_key="docs_in", column="pdf_path",
        page_range="1,3",
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    df = result.output_for_node("words_out")

    assert len(df) == 2, "page_range='1,3' must skip page 2 entirely"
    assert set(df["page_number"]) == {1, 3}
    assert set(df["text"]) == {"PageOneWord", "PageThreeWord"}


def test_page_range_with_hyphen_span(mod, tmp_path):
    pdf_path = tmp_path / "fourpage.pdf"
    _make_multi_page_pdf(
        pdf_path,
        [["W1"], ["W2"], ["W3"], ["W4"]],
    )

    component = mod.DocumentWordBoxesComponent(
        asset_name="words_out", upstream_asset_key="docs_in", column="pdf_path",
        page_range="2-3",
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    df = result.output_for_node("words_out")

    assert set(df["page_number"]) == {2, 3}
    assert set(df["text"]) == {"W2", "W3"}


# --- empty text layer (scanned PDF stand-in) ----------------------------------------------------

def test_blank_page_with_no_text_layer_is_skipped_with_warning(mod, tmp_path):
    pdf_path = tmp_path / "blank.pdf"
    _make_blank_pdf(pdf_path)

    component = mod.DocumentWordBoxesComponent(
        asset_name="words_out", upstream_asset_key="docs_in", column="pdf_path",
    )
    result = _materialize_one_row(mod, component, str(pdf_path), extra_cols={"doc_id": "x"})
    assert result.success
    df = result.output_for_node("words_out")

    assert len(df) == 0
    assert "doc_id" in df.columns and "page_number" in df.columns and "text" in df.columns

    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["documents_skipped_no_text_layer"].value == 1
    assert meta["documents_processed"].value == 0
    assert meta["documents_failed"].value == 0


def test_mixed_batch_real_and_blank_pdfs(mod, tmp_path):
    """One document with real text and one with none, in the same batch --
    only the blank one is counted as skipped, and only the real one
    contributes rows."""
    real_pdf = tmp_path / "real.pdf"
    _make_word_pdf(real_pdf, ["Real Content Here"])
    blank_pdf = tmp_path / "blank.pdf"
    _make_blank_pdf(blank_pdf)

    component = mod.DocumentWordBoxesComponent(
        asset_name="words_out", upstream_asset_key="docs_in", column="pdf_path",
    )
    defs = component.build_defs(load_context=None)
    upstream_df = pd.DataFrame([
        {"pdf_path": str(real_pdf), "doc_id": "real"},
        {"pdf_path": str(blank_pdf), "doc_id": "blank"},
    ])

    @dg.asset(name="docs_in")
    def _upstream():
        return upstream_df

    result = dg.materialize([*defs.assets, _upstream])
    assert result.success
    df = result.output_for_node("words_out")

    assert len(df) == 3, "only the real document's 3 words should appear"
    assert set(df["doc_id"]) == {"real"}

    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["documents_processed"].value == 1
    assert meta["documents_skipped_no_text_layer"].value == 1


# --- passthrough_columns ----------------------------------------------------------------------

def test_passthrough_columns_explicit_override(mod, tmp_path):
    pdf_path = tmp_path / "one_word.pdf"
    _make_word_pdf(pdf_path, ["Solo"])

    component = mod.DocumentWordBoxesComponent(
        asset_name="words_out", upstream_asset_key="docs_in", column="pdf_path",
        passthrough_columns=["doc_id"],
    )
    result = _materialize_one_row(
        mod, component, str(pdf_path), extra_cols={"doc_id": "d1", "other_col": "ignored"},
    )
    df = result.output_for_node("words_out")
    assert "doc_id" in df.columns
    assert "other_col" not in df.columns


def test_passthrough_columns_default_is_all_upstream_columns_except_pdf_column(mod, tmp_path):
    pdf_path = tmp_path / "one_word.pdf"
    _make_word_pdf(pdf_path, ["Solo"])

    component = mod.DocumentWordBoxesComponent(
        asset_name="words_out", upstream_asset_key="docs_in", column="pdf_path",
    )
    result = _materialize_one_row(
        mod, component, str(pdf_path), extra_cols={"doc_id": "d1", "other_col": "kept"},
    )
    df = result.output_for_node("words_out")
    assert "doc_id" in df.columns
    assert "other_col" in df.columns
    assert "pdf_path" not in df.columns


# --- find_text_bbox ----------------------------------------------------------------------------

def test_find_text_bbox_matches_hand_built_multiword_run():
    mod_ = load_component_module()
    words = [
        {"text": "This", "x0": 10, "x1": 30, "top": 100, "bottom": 110, "page_number": 1},
        {"text": "is", "x0": 32, "x1": 40, "top": 100, "bottom": 110, "page_number": 1},
        {"text": "a", "x0": 42, "x1": 48, "top": 100, "bottom": 110, "page_number": 1},
        # simulates a wrapped "line" -- a new top/bottom band, same page
        {"text": "line", "x0": 10, "x1": 30, "top": 120, "bottom": 130, "page_number": 1},
        {"text": "that", "x0": 32, "x1": 50, "top": 120, "bottom": 130, "page_number": 1},
        {"text": "wraps", "x0": 52, "x1": 80, "top": 120, "bottom": 130, "page_number": 1},
    ]
    bbox = mod_.find_text_bbox(words, "a line that wraps")
    assert bbox == {"x0": 10, "x1": 80, "top": 100, "bottom": 130, "page": 1}

    # Case-insensitive
    bbox_ci = mod_.find_text_bbox(words, "A LINE")
    assert bbox_ci is not None
    assert bbox_ci["page"] == 1


def test_find_text_bbox_returns_none_for_absent_text():
    mod_ = load_component_module()
    words = [
        {"text": "Hello", "x0": 10, "x1": 30, "top": 100, "bottom": 110, "page_number": 1},
        {"text": "World", "x0": 32, "x1": 50, "top": 100, "bottom": 110, "page_number": 1},
    ]
    assert mod_.find_text_bbox(words, "this text is not here") is None
    assert mod_.find_text_bbox(words, "") is None
    assert mod_.find_text_bbox([], "Hello") is None


def test_find_text_bbox_on_real_pdf_extracted_words_spanning_a_line_break(mod, tmp_path):
    """A real PDF with two separately-drawn lines (two distinct drawString
    calls, so two different `top` bands) -- the needle spans both, proving
    the line-break caveat in the docstring is actually true, not just
    asserted."""
    pdf_path = tmp_path / "lines.pdf"
    _make_word_pdf(pdf_path, ["The quick brown fox", "jumps over the lazy dog"])

    component = mod.DocumentWordBoxesComponent(
        asset_name="words_out", upstream_asset_key="docs_in", column="pdf_path",
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    df = result.output_for_node("words_out")
    words = df.to_dict("records")

    bbox = mod.find_text_bbox(words, "fox jumps over")
    assert bbox is not None, "needle spanning the line break between the two drawn lines must still be found"
    assert bbox["page"] == 1
    assert bbox["x0"] < bbox["x1"]
    assert bbox["top"] < bbox["bottom"]

    assert mod.find_text_bbox(words, "nonexistent phrase not in document") is None


# --- partitioning / backfill_policy -----------------------------------------------------------

def test_backfill_policy_wiring(mod):
    cls = mod.DocumentWordBoxesComponent
    base = dict(
        asset_name="out_asset", upstream_asset_key="src", column="pdf_path",
        partition_type="daily", partition_start="2024-01-01", partition_date_column="event_date",
    )
    assets = cls(**base, backfill_policy="single_run").build_defs(None).assets
    assert assets[0].backfill_policy == dg.BackfillPolicy.single_run()

    assets = cls(**base, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=3).build_defs(None).assets
    assert assets[0].backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=3)

    assets = cls(**base).build_defs(None).assets
    assert assets[0].backfill_policy is None

    # Ignored for an unpartitioned asset
    unpartitioned = dict(asset_name="out_asset", upstream_asset_key="src", column="pdf_path")
    assets = cls(**unpartitioned, backfill_policy="single_run").build_defs(None).assets
    assert assets[0].backfill_policy is None


def test_partition_filtering_and_single_run_range_use_real_pdfs(mod, tmp_path):
    """Real per-row PDFs (one distinctive word each) across 3 daily partitions,
    so a partition-filtered run's output row COUNT genuinely differs."""
    days = ["2024-01-01", "2024-01-02", "2024-01-03"]
    rows = []
    for day in days:
        for i in range(3):
            pdf_path = tmp_path / f"{day}_{i}.pdf"
            _make_word_pdf(pdf_path, [f"word{day.replace('-', '')}{i}"])
            rows.append({"doc_id": f"{day}_{i}", "event_date": day, "pdf_path": str(pdf_path)})
    src_df = pd.DataFrame(rows)

    @dg.asset(name="src")
    def src():
        return src_df

    def _materialize(partitioned, partition_key=None, tags=None, **overrides):
        kw = dict(asset_name="out_asset", upstream_asset_key="src", column="pdf_path")
        if partitioned:
            kw.update(partition_type="daily", partition_start="2024-01-01", partition_date_column="event_date")
        kw.update(overrides)
        comp = mod.DocumentWordBoxesComponent(**kw)
        comp_assets = comp.build_defs(None).assets
        result = dg.materialize(
            [src, *comp_assets],
            resources={"io_manager": dg.InMemoryIOManager()},
            partition_key=partition_key,
            tags=tags or {},
        )
        assert result.success
        return result.output_for_node("out_asset")

    full = _materialize(False)
    one_day = _materialize(True, partition_key="2024-01-01")
    ranged = _materialize(
        True, backfill_policy="single_run",
        tags={
            "dagster/asset_partition_range_start": "2024-01-01",
            "dagster/asset_partition_range_end": "2024-01-02",
        },
    )

    assert len(full) == 9, "one word per PDF x 9 PDFs across 3 partitions"
    assert len(one_day) == 3, "one partition's worth of PDFs"
    assert len(ranged) == 6, "two partitions' worth via a single_run backfill range"


# --- asset-check wiring ------------------------------------------------------------------------

def test_schema_change_check_is_wired_and_passes_on_an_unchanged_schema(mod, tmp_path):
    pdf_path = tmp_path / "one_word.pdf"
    _make_word_pdf(pdf_path, ["Solo"])

    component = mod.DocumentWordBoxesComponent(
        asset_name="words_out", upstream_asset_key="docs_in", column="pdf_path",
    )
    defs = component.build_defs(load_context=None)
    assert len(list(defs.asset_checks)) >= 1, "build_column_schema_change_checks must produce at least one check"

    upstream_df = pd.DataFrame([{"pdf_path": str(pdf_path)}])

    @dg.asset(name="docs_in")
    def _upstream():
        return upstream_df

    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize([*defs.assets, *defs.asset_checks, _upstream], instance=instance)
    assert result.success
    result2 = dg.materialize([*defs.assets, *defs.asset_checks, _upstream], instance=instance)
    assert result2.success
    checks = result2.get_asset_check_evaluations()
    assert len(checks) >= 1
    assert all(c.passed for c in checks), "schema is unchanged between runs, the check must pass"
