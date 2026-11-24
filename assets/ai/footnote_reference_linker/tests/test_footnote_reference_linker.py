"""Committed tests for FootnoteReferenceLinkerComponent.

A real single/multi-page PDF is generated with `reportlab` (never mocked) so the
word-box bounding-box lookup runs against real pdfplumber geometry. Only the
`litellm.completion` call is mocked (this repo's "Committed test convention" --
mock only the paid call), following the same fake-completion shape used by
structured_document_extractor's and document_section_splitter's own test suites.
"""
import json

import dagster as dg
import pandas as pd
import pytest
import types
import sys

from .conftest import load_component_module

pdfplumber = pytest.importorskip("pdfplumber")

CLS = "FootnoteReferenceLinkerComponent"


@pytest.fixture()
def mod():
    return load_component_module()


def _make_pdf(path, pages):
    """One real PDF, one `reportlab` page per entry in `pages` (each entry a list of lines)."""
    from reportlab.pdfgen import canvas

    c = canvas.Canvas(str(path))
    for lines in pages:
        c.setFont("Helvetica", 12)
        y = 700
        for line in lines:
            c.drawString(72, y, line)
            y -= 20
        c.showPage()
    c.save()
    return str(path)


@pytest.fixture()
def fake_litellm(monkeypatch):
    """Returns a FIFO queue of canned footnote-detection replies, one per page the asset calls
    detect_page_footnotes for. Records every call for assertions."""
    calls = []
    state = {"replies": []}

    def fake_completion(model, messages, api_key=None, **kwargs):
        calls.append({"model": model, "messages": messages, "kwargs": kwargs})
        reply = state["replies"].pop(0) if state["replies"] else []
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=json.dumps(reply)))]
        )

    fake_module = types.ModuleType("litellm")
    fake_module.completion = fake_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_module)
    return calls, state


def _materialize(mod, component, upstream_df):
    defs = component.build_defs(load_context=None)

    @dg.asset(name=component.upstream_asset_key)
    def _upstream():
        return upstream_df

    result = dg.materialize([*defs.assets, *defs.asset_checks, _upstream])
    assert result.success
    return result


def _output(result, asset_name):
    return result.output_for_node(asset_name)


# --- real footnote + real bbox lookup -----------------------------------------------------------------------------

def test_single_footnote_produces_a_correct_bbox_for_a_real_body_snippet(mod, fake_litellm, tmp_path):
    calls, state = fake_litellm
    pdf_path = _make_pdf(
        tmp_path / "paper.pdf",
        [["Climate models show significant warming trends3 across most regions."]],
    )
    state["replies"] = [[
        {
            "marker": "3",
            "footnote_text": "See Smith et al. (2020) for full methodology details.",
            "body_reference_snippet": "warming trends3",
        },
    ]]

    component = getattr(mod, CLS)(
        asset_name="footnotes", upstream_asset_key="docs_in", column="file_path",
    )
    result = _materialize(mod, component, pd.DataFrame({"doc_id": ["doc1"], "file_path": [pdf_path]}))
    df = _output(result, "footnotes")

    assert len(df) == 1
    row = df.iloc[0]
    assert row["page_number"] == 1
    assert row["marker"] == "3"
    assert row["footnote_text"] == "See Smith et al. (2020) for full methodology details."
    assert row["body_reference_snippet"] == "warming trends3"
    assert row["doc_id"] == "doc1"

    bbox = json.loads(row["body_reference_bbox"])
    assert set(bbox.keys()) == {"x0", "x1", "top", "bottom"}
    assert bbox["x0"] < bbox["x1"]
    assert bbox["top"] < bbox["bottom"]

    assert len(calls) == 1, "exactly one LLM call per page"


def test_no_footnotes_on_page_produces_no_rows(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    pdf_path = _make_pdf(tmp_path / "plain.pdf", [["Nothing noteworthy happens on this page at all."]])
    state["replies"] = [[]]

    component = getattr(mod, CLS)(asset_name="footnotes", upstream_asset_key="docs_in", column="file_path")
    result = _materialize(mod, component, pd.DataFrame({"doc_id": ["doc1"], "file_path": [pdf_path]}))
    df = _output(result, "footnotes")
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["footnotes_found"].value == 0
    assert meta["pages_processed"].value == 1


# --- bbox lookup failure: row still emitted, bbox is None, never fabricated ----------------------------------------

def test_snippet_not_found_in_page_still_emits_row_with_null_bbox(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    pdf_path = _make_pdf(
        tmp_path / "paper2.pdf",
        [["Revenue grew substantially year over year4."]],
    )
    state["replies"] = [[
        {
            "marker": "4",
            "footnote_text": "Internal estimate, unaudited.",
            # Paraphrased by the model -- does NOT appear verbatim anywhere in the page.
            "body_reference_snippet": "a completely different phrase never printed on this page",
        },
    ]]

    component = getattr(mod, CLS)(asset_name="footnotes", upstream_asset_key="docs_in", column="file_path")
    result = _materialize(mod, component, pd.DataFrame({"doc_id": ["doc1"], "file_path": [pdf_path]}))
    df = _output(result, "footnotes")

    assert len(df) == 1, "a failed bbox lookup must not drop the footnote row"
    row = df.iloc[0]
    assert row["marker"] == "4"
    assert row["footnote_text"] == "Internal estimate, unaudited."
    assert pd.isna(row["body_reference_bbox"]) or row["body_reference_bbox"] is None

    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["body_reference_bbox_unmatched"].value == 1
    assert meta["body_reference_bbox_matched"].value == 0


def test_find_snippet_bbox_direct_match_and_no_match(mod, tmp_path):
    """Direct unit coverage of the matching heuristic against real pdfplumber word geometry."""
    pdf_path = _make_pdf(tmp_path / "words.pdf", [["The quick brown fox jumps over the lazy dog."]])
    with pdfplumber.open(pdf_path) as pdf:
        words = pdf.pages[0].extract_words()

    bbox = mod.find_snippet_bbox(words, "quick brown fox")
    assert bbox is not None
    assert bbox["x0"] < bbox["x1"]

    # Case-insensitive + whitespace-normalized still matches.
    bbox_ci = mod.find_snippet_bbox(words, "  QUICK   Brown FOX  ")
    assert bbox_ci is not None
    assert bbox_ci == bbox

    assert mod.find_snippet_bbox(words, "nonexistent phrase entirely") is None
    assert mod.find_snippet_bbox(words, "") is None
    assert mod.find_snippet_bbox([], "quick brown fox") is None


# --- passthrough columns ------------------------------------------------------------------------------------------

def test_passthrough_columns_default_excludes_only_the_pdf_column(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    pdf_path = _make_pdf(tmp_path / "a.pdf", [["Some body text with a marker5 right here."]])
    state["replies"] = [[
        {"marker": "5", "footnote_text": "A footnote.", "body_reference_snippet": "a marker5"},
    ]]

    component = getattr(mod, CLS)(asset_name="footnotes", upstream_asset_key="docs_in", column="file_path")
    df_in = pd.DataFrame({"doc_id": ["doc1"], "region": ["us"], "file_path": [pdf_path]})
    result = _materialize(mod, component, df_in)
    df = _output(result, "footnotes")
    assert set(df.columns) == {
        "doc_id", "region", "page_number", "marker", "footnote_text",
        "body_reference_snippet", "body_reference_bbox",
    }
    assert df.iloc[0]["doc_id"] == "doc1" and df.iloc[0]["region"] == "us"


def test_passthrough_columns_explicit_list_is_honored(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    pdf_path = _make_pdf(tmp_path / "a.pdf", [["Some body text with a marker5 right here."]])
    state["replies"] = [[
        {"marker": "5", "footnote_text": "A footnote.", "body_reference_snippet": "a marker5"},
    ]]

    component = getattr(mod, CLS)(
        asset_name="footnotes", upstream_asset_key="docs_in", column="file_path",
        passthrough_columns=["doc_id"],
    )
    df_in = pd.DataFrame({"doc_id": ["doc1"], "region": ["us"], "file_path": [pdf_path]})
    result = _materialize(mod, component, df_in)
    df = _output(result, "footnotes")
    assert "region" not in df.columns
    assert "doc_id" in df.columns


# --- malformed LLM replies raise clear errors, never silently return garbage ---------------------------------------

def test_bad_json_reply_raises_a_clear_error(mod):
    with pytest.raises(Exception):
        mod.parse_footnote_detection_response("not json at all")


def test_non_list_reply_raises_a_clear_error(mod):
    with pytest.raises(ValueError, match="expected a JSON array"):
        mod.parse_footnote_detection_response(json.dumps({"marker": "1"}))


def test_malformed_entry_missing_required_key_raises_a_clear_error(mod):
    with pytest.raises(ValueError, match="malformed entry"):
        mod.parse_footnote_detection_response(json.dumps([{"marker": "1", "footnote_text": "x"}]))


def test_code_fence_wrapped_reply_still_parses(mod):
    raw = "```json\n" + json.dumps([
        {"marker": "1", "footnote_text": "x", "body_reference_snippet": "y"},
    ]) + "\n```"
    out = mod.parse_footnote_detection_response(raw)
    assert out == [{"marker": "1", "footnote_text": "x", "body_reference_snippet": "y"}]


def test_asset_survives_a_malformed_reply_on_one_page_without_failing_the_run(mod, monkeypatch, tmp_path):
    """A malformed reply for one page must not crash the whole materialization -- same per-row/
    per-page catch-and-count convention as structured_document_extractor and document_section_splitter."""
    calls = []

    def fake_completion(model, messages, api_key=None, **kwargs):
        calls.append(messages)
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(content="not json at all"))]
        )

    fake_module = types.ModuleType("litellm")
    fake_module.completion = fake_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_module)

    pdf_path = _make_pdf(tmp_path / "a.pdf", [["Some body text with a marker5 right here."]])
    component = getattr(mod, CLS)(asset_name="footnotes", upstream_asset_key="docs_in", column="file_path")
    result = _materialize(mod, component, pd.DataFrame({"doc_id": ["doc1"], "file_path": [pdf_path]}))
    df = _output(result, "footnotes")

    assert len(df) == 0
    assert len(calls) == 1
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["llm_call_failures"].value == 1
    assert meta["pages_processed"].value == 0


# --- partitioning / backfill_policy (same shape as sibling components' suites) --------------------------------

def test_backfill_policy_wiring(mod):
    cls = getattr(mod, CLS)

    def _build(**overrides):
        kw = dict(
            asset_name="out_asset", upstream_asset_key="src", column="file_path",
            partition_type="daily", partition_start="2024-01-01", partition_date_column="event_date",
        )
        kw.update(overrides)
        return cls(**kw).build_defs(load_context=None).assets

    assert _build(backfill_policy="single_run")[0].backfill_policy == dg.BackfillPolicy.single_run()
    assert _build(backfill_policy="multi_run", backfill_policy_max_partitions_per_run=3)[0].backfill_policy == (
        dg.BackfillPolicy.multi_run(max_partitions_per_run=3)
    )
    assert _build()[0].backfill_policy is None
    # Ignored for an unpartitioned asset
    unpartitioned = cls(
        asset_name="out_asset", upstream_asset_key="src", column="file_path", backfill_policy="single_run",
    ).build_defs(load_context=None).assets
    assert unpartitioned[0].backfill_policy is None


def test_partition_filters_upstream_rows_to_the_current_partition_key(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["replies"] = []  # no footnotes needed for this test
    pdfs = {
        name: _make_pdf(tmp_path / f"{name}.pdf", [["SOME PAGE TEXT with nothing special."]])
        for name in ("a", "b", "c", "d")
    }
    df_in = pd.DataFrame({
        "doc_id": ["a", "b", "c", "d"],
        "event_date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
        "file_path": [pdfs["a"], pdfs["b"], pdfs["c"], pdfs["d"]],
    })

    component = getattr(mod, CLS)(
        asset_name="footnotes", upstream_asset_key="docs_in", column="file_path",
        partition_type="daily", partition_start="2024-01-01", partition_date_column="event_date",
    )
    defs = component.build_defs(load_context=None)

    @dg.asset(name="docs_in")
    def _upstream():
        return df_in

    result = dg.materialize(
        [*defs.assets, *defs.asset_checks, _upstream],
        partition_key="2024-01-01",
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["documents_processed"].value == 2


# --- asset-check wiring -------------------------------------------------------------------------------------------

def test_schema_change_check_is_wired_up(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["replies"] = []
    pdf_path = _make_pdf(tmp_path / "a.pdf", [["ONE PAGE OF TEXT with nothing notable."]])
    component = getattr(mod, CLS)(asset_name="footnotes", upstream_asset_key="docs_in", column="file_path")
    result = _materialize(mod, component, pd.DataFrame({"doc_id": ["doc1"], "file_path": [pdf_path]}))
    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) >= 1
    assert all(e.passed for e in evaluations)
