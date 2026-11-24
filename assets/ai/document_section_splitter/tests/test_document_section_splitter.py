"""Committed tests for DocumentSectionSplitterComponent.

Real multi-page PDFs are generated with reportlab (never mocked) so page-level
text extraction via pdfplumber is verified against real bytes. Only the
litellm.completion call is mocked (this repo's "Committed test convention" --
mock only the paid call), following the same fake-completion shape used by
structured_document_extractor's and document_chunker's own test suites.
"""
import json
import re
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

pdfplumber = pytest.importorskip("pdfplumber")

CLS = "DocumentSectionSplitterComponent"


@pytest.fixture()
def mod():
    return load_component_module()


def _make_pdf(path, page_texts):
    """One real multi-page PDF, one `reportlab` page per entry in `page_texts`."""
    from reportlab.pdfgen import canvas

    c = canvas.Canvas(str(path))
    for text in page_texts:
        c.setFont("Helvetica", 14)
        c.drawString(72, 700, text)
        c.showPage()
    c.save()
    return str(path)


@pytest.fixture()
def fake_litellm(monkeypatch):
    """Routes each completion() call to a canned classification or schema-suggestion
    reply based on the prompt shape, and records every call for assertions.

    state["classification_replies"]: FIFO queue of page-classification lists, one
    consumed per document (classification is one LLM call per document). When
    exhausted, a classification call auto-replies "other" for every page the
    prompt mentions -- good enough for tests that don't care about the specific
    classification (e.g. partition filtering).

    state["schema_replies"]: section_type -> canned {field, description} list for
    the schema-suggestion call.
    """
    calls = []
    state = {"classification_replies": [], "schema_replies": {}}

    def fake_completion(model, messages, api_key=None, **kwargs):
        content = messages[0]["content"]
        if "Classify EVERY page" in content:
            kind = "classification"
            queue = state["classification_replies"]
            if queue:
                reply = queue.pop(0)
            else:
                num_pages = content.count("[Page ")
                reply = [{"page": p, "section_type": "other"} for p in range(1, num_pages + 1)]
        else:
            kind = "schema"
            m = re.search(r"sample '([^']+)' section", content)
            section_type = m.group(1) if m else None
            reply = state["schema_replies"].get(section_type, [])
        calls.append({"model": model, "messages": messages, "kwargs": kwargs, "kind": kind})
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


# --- splitting + merging ----------------------------------------------------------------------------------------

def test_single_document_splits_into_sections_and_merges_consecutive_pages(mod, fake_litellm, tmp_path):
    calls, state = fake_litellm
    pdf_path = _make_pdf(
        tmp_path / "report.pdf",
        ["COVER PAGE TEXT", "MD AND A DISCUSSION", "BALANCE SHEET LINE ITEMS", "BALANCE SHEET CONTINUED"],
    )
    state["classification_replies"] = [[
        {"page": 1, "section_type": "cover"},
        {"page": 2, "section_type": "md_a"},
        {"page": 3, "section_type": "balance_sheet"},
        {"page": 4, "section_type": "balance_sheet"},
    ]]

    component = getattr(mod, CLS)(
        asset_name="sections", upstream_asset_key="docs_in", column="file_path",
        suggest_schema=False,
    )
    result = _materialize(mod, component, pd.DataFrame({"doc_id": ["doc1"], "file_path": [pdf_path]}))
    df = _output(result, "sections")

    assert len(df) == 3
    assert list(df["section_type"]) == ["cover", "md_a", "balance_sheet"]
    assert list(df["page_start"]) == [1, 2, 3]
    assert list(df["page_end"]) == [1, 2, 4]
    assert df.iloc[0]["section_text"].strip() == "COVER PAGE TEXT"
    merged_text = df.iloc[2]["section_text"]
    assert "BALANCE SHEET LINE ITEMS" in merged_text and "BALANCE SHEET CONTINUED" in merged_text
    assert list(df["doc_id"]) == ["doc1", "doc1", "doc1"]
    assert "suggested_schema_json" not in df.columns

    classification_calls = [c for c in calls if c["kind"] == "classification"]
    assert len(classification_calls) == 1, "exactly one LLM call per document"


def test_merge_consecutive_sections_is_a_pure_contiguous_run_merge(mod):
    classifications = [
        {"page": 1, "section_type": "cover"},
        {"page": 2, "section_type": "notes"},
        {"page": 3, "section_type": "notes"},
        {"page": 4, "section_type": "cover"},  # non-adjacent repeat -- must NOT merge with page 1
    ]
    page_texts = ["p1", "p2", "p3", "p4"]
    sections = mod.merge_consecutive_sections(classifications, page_texts)
    assert sections == [
        {"section_type": "cover", "page_start": 1, "page_end": 1, "section_text": "p1"},
        {"section_type": "notes", "page_start": 2, "page_end": 3, "section_text": "p2\n\np3"},
        {"section_type": "cover", "page_start": 4, "page_end": 4, "section_text": "p4"},
    ]


# --- passthrough columns ------------------------------------------------------------------------------------------

def test_passthrough_columns_default_excludes_only_the_pdf_column(mod, fake_litellm, tmp_path):
    pdf_path = _make_pdf(tmp_path / "a.pdf", ["ONE PAGE OF TEXT"])
    component = getattr(mod, CLS)(
        asset_name="sections", upstream_asset_key="docs_in", column="file_path", suggest_schema=False,
    )
    df_in = pd.DataFrame({"doc_id": ["doc1"], "region": ["us"], "file_path": [pdf_path]})
    result = _materialize(mod, component, df_in)
    df = _output(result, "sections")
    assert set(df.columns) == {"doc_id", "region", "section_type", "page_start", "page_end", "section_text"}
    assert df.iloc[0]["doc_id"] == "doc1" and df.iloc[0]["region"] == "us"


def test_passthrough_columns_explicit_list_is_honored(mod, fake_litellm, tmp_path):
    pdf_path = _make_pdf(tmp_path / "a.pdf", ["ONE PAGE OF TEXT"])
    component = getattr(mod, CLS)(
        asset_name="sections", upstream_asset_key="docs_in", column="file_path",
        passthrough_columns=["doc_id"], suggest_schema=False,
    )
    df_in = pd.DataFrame({"doc_id": ["doc1"], "region": ["us"], "file_path": [pdf_path]})
    result = _materialize(mod, component, df_in)
    df = _output(result, "sections")
    assert "region" not in df.columns
    assert "doc_id" in df.columns


# --- suggest_schema: dedup by unique section_type --------------------------------------------------------------

def test_suggest_schema_true_dedupes_by_unique_section_type(mod, fake_litellm, tmp_path):
    calls, state = fake_litellm
    pdf1 = _make_pdf(tmp_path / "doc1.pdf", ["COVER TEXT", "BALANCE A", "BALANCE B"])
    pdf2 = _make_pdf(tmp_path / "doc2.pdf", ["BALANCE C"])
    state["classification_replies"] = [
        [
            {"page": 1, "section_type": "cover"},
            {"page": 2, "section_type": "balance_sheet"},
            {"page": 3, "section_type": "balance_sheet"},
        ],
        [
            {"page": 1, "section_type": "balance_sheet"},
        ],
    ]
    state["schema_replies"] = {
        "cover": [{"field": "title", "description": "the document title"}],
        "balance_sheet": [{"field": "total_assets", "description": "total assets amount"}],
    }

    component = getattr(mod, CLS)(
        asset_name="sections", upstream_asset_key="docs_in", column="file_path",
        suggest_schema=True, max_schema_fields=5,
    )
    result = _materialize(
        mod, component,
        pd.DataFrame({"doc_id": ["doc1", "doc2"], "file_path": [pdf1, pdf2]}),
    )
    df = _output(result, "sections")

    assert len(df) == 3  # cover(doc1) + balance_sheet(doc1, merged) + balance_sheet(doc2)
    assert set(df["section_type"]) == {"cover", "balance_sheet"}

    schema_calls = [c for c in calls if c["kind"] == "schema"]
    assert len(schema_calls) == 2, "one schema call per UNIQUE section_type, not per row"

    cover_schema = json.loads(df[df["section_type"] == "cover"].iloc[0]["suggested_schema_json"])
    assert cover_schema == [{"field": "title", "description": "the document title"}]
    balance_rows = df[df["section_type"] == "balance_sheet"]
    assert len(balance_rows) == 2
    for _, row in balance_rows.iterrows():
        assert json.loads(row["suggested_schema_json"]) == [
            {"field": "total_assets", "description": "total assets amount"}
        ]


def test_suggest_schema_false_skips_the_call_entirely(mod, fake_litellm, tmp_path):
    calls, state = fake_litellm
    pdf_path = _make_pdf(tmp_path / "a.pdf", ["COVER TEXT", "BALANCE A"])
    state["classification_replies"] = [[
        {"page": 1, "section_type": "cover"},
        {"page": 2, "section_type": "balance_sheet"},
    ]]

    component = getattr(mod, CLS)(
        asset_name="sections", upstream_asset_key="docs_in", column="file_path", suggest_schema=False,
    )
    result = _materialize(mod, component, pd.DataFrame({"doc_id": ["doc1"], "file_path": [pdf_path]}))
    df = _output(result, "sections")

    assert "suggested_schema_json" not in df.columns
    schema_calls = [c for c in calls if c["kind"] == "schema"]
    assert len(schema_calls) == 0


# --- malformed LLM replies raise clear errors, never silently return garbage --------------------------------------

def test_classification_bad_json_raises_a_clear_error(mod):
    with pytest.raises(Exception):
        mod.parse_section_classification_response("not json at all", 2, ["cover", "other"])


def test_classification_non_list_reply_raises_a_clear_error(mod):
    with pytest.raises(ValueError, match="expected a JSON array"):
        mod.parse_section_classification_response(json.dumps({"page": 1}), 1, ["cover"])


def test_classification_omitted_page_carries_forward_previous_section_type(mod):
    # page 2 is missing from the reply entirely
    raw = json.dumps([{"page": 1, "section_type": "cover"}, {"page": 3, "section_type": "notes"}])
    out = mod.parse_section_classification_response(raw, 3, ["cover", "notes"])
    assert out == [
        {"page": 1, "section_type": "cover"},
        {"page": 2, "section_type": "cover"},  # carried forward, not dropped or invented
        {"page": 3, "section_type": "notes"},
    ]


def test_schema_suggestion_bad_json_raises_a_clear_error(mod):
    with pytest.raises(Exception):
        mod.parse_section_schema_suggestion_response("not json", 12)


def test_schema_suggestion_malformed_entry_raises_a_clear_error(mod):
    with pytest.raises(ValueError, match="malformed entry"):
        mod.parse_section_schema_suggestion_response(json.dumps([{"field": "x"}]), 12)


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
    pdfs = {
        name: _make_pdf(tmp_path / f"{name}.pdf", ["SOME PAGE TEXT"])
        for name in ("a", "b", "c", "d")
    }
    df_in = pd.DataFrame({
        "doc_id": ["a", "b", "c", "d"],
        "event_date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
        "file_path": [pdfs["a"], pdfs["b"], pdfs["c"], pdfs["d"]],
    })

    component = getattr(mod, CLS)(
        asset_name="sections", upstream_asset_key="docs_in", column="file_path",
        partition_type="daily", partition_start="2024-01-01", partition_date_column="event_date",
        suggest_schema=False,
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
    df = result.output_for_node("sections")
    assert set(df["doc_id"]) == {"a", "b"}


# --- asset-check wiring -------------------------------------------------------------------------------------------

def test_schema_change_check_is_wired_up(mod, fake_litellm, tmp_path):
    pdf_path = _make_pdf(tmp_path / "a.pdf", ["ONE PAGE OF TEXT"])
    component = getattr(mod, CLS)(
        asset_name="sections", upstream_asset_key="docs_in", column="file_path", suggest_schema=False,
    )
    result = _materialize(mod, component, pd.DataFrame({"doc_id": ["doc1"], "file_path": [pdf_path]}))
    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) >= 1
    assert all(e.passed for e in evaluations)
