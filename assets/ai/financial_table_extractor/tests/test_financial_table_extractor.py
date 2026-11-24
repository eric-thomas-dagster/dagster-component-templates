"""Committed tests for FinancialTableExtractorComponent.

Real PDFs generated with reportlab (never mocked -- see
tests/conftest.py's `_make_bordered_pdf` / `_make_borderless_pdf`), real
pdfplumber table detection (never mocked). Only `litellm.completion` is
mocked, and only in the `classify_tables=True` tests -- the PDF and
pdfplumber are real there too.

Covers: multi-table explode, vertical_strategy='text' vs 'lines' on a
borderless table, min_rows/min_cols filtering, None-vs-"" cell preservation,
classify_tables off (zero litellm calls) and on (one call per table),
partition filtering + backfill_policy wiring, and the schema-change asset
check.
"""
import json

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

pdfplumber = pytest.importorskip("pdfplumber")

from reportlab.lib import colors  # noqa: E402
from reportlab.lib.pagesizes import letter  # noqa: E402
from reportlab.pdfgen import canvas  # noqa: E402
from reportlab.platypus import SimpleDocTemplate, Spacer, Table, TableStyle  # noqa: E402


@pytest.fixture()
def mod():
    return load_component_module()


def _make_bordered_pdf(path, *tables_data, style_extra=None):
    """One real PDF with one bordered (GRID) reportlab Table per entry in `tables_data`,
    stacked with a spacer between them so pdfplumber sees them as separate tables."""
    doc = SimpleDocTemplate(str(path), pagesize=letter)
    flowables = []
    for i, data in enumerate(tables_data):
        t = Table(data)
        style = [("GRID", (0, 0), (-1, -1), 1, colors.black)]
        if style_extra:
            style.extend(style_extra)
        t.setStyle(TableStyle(style))
        flowables.append(t)
        if i < len(tables_data) - 1:
            flowables.append(Spacer(1, 50))
    doc.build(flowables)


def _make_borderless_pdf(path, rows, col_xs=(50, 150, 250), start_y=350):
    """One real PDF with whitespace-aligned columns of text and NO drawn lines --
    detectable by vertical_strategy='text' but not by the default 'lines'."""
    c = canvas.Canvas(str(path), pagesize=(400, 400))
    y = start_y
    for row in rows:
        for x, cell in zip(col_xs, row):
            c.drawString(x, y, cell)
        y -= 20
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

def test_multiple_real_tables_explode_to_multiple_rows(mod, tmp_path):
    pdf_path = tmp_path / "statement.pdf"
    _make_bordered_pdf(
        pdf_path,
        [["Year", "Revenue", "Expenses"], ["2023", "100", "80"], ["2024", "120", "90"]],
        [["Quarter", "Net Income"], ["Q1", "10"], ["Q2", "15"]],
    )

    component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path",
    )
    result = _materialize_one_row(mod, component, str(pdf_path), extra_cols={"doc_id": "doc-1"})
    assert result.success
    df = result.output_for_node("tables_out")

    assert len(df) == 2, "one real document with 2 real tables must explode to 2 rows"
    assert list(df["table_index"]) == [0, 1]
    assert list(df["page_number"]) == [1, 1]
    assert list(df["doc_id"]) == ["doc-1", "doc-1"], "passthrough column must be carried to every exploded row"

    first = json.loads(df.loc[0, "table_data"])
    assert first == [["Year", "Revenue", "Expenses"], ["2023", "100", "80"], ["2024", "120", "90"]]
    assert df.loc[0, "row_count"] == 3
    assert df.loc[0, "col_count"] == 3
    assert json.loads(df.loc[0, "header_row_json"]) == ["Year", "Revenue", "Expenses"]
    assert "Revenue" in df.loc[0, "table_markdown"] and "100" in df.loc[0, "table_markdown"]

    second = json.loads(df.loc[1, "table_data"])
    assert second == [["Quarter", "Net Income"], ["Q1", "10"], ["Q2", "15"]]

    assert "pdf_path" not in df.columns, "the PDF column itself is not a passthrough column by default"


def test_vertical_strategy_text_vs_lines_on_borderless_table(mod, tmp_path):
    pdf_path = tmp_path / "borderless.pdf"
    _make_borderless_pdf(
        pdf_path,
        [["Year", "Revenue", "Expenses"], ["2023", "100", "80"], ["2024", "120", "90"]],
    )

    lines_component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path",
        vertical_strategy="lines", horizontal_strategy="lines",
    )
    lines_result = _materialize_one_row(mod, lines_component, str(pdf_path))
    lines_df = lines_result.output_for_node("tables_out")
    assert len(lines_df) == 0, "default 'lines' strategy must find nothing on a borderless table (no drawn rules)"

    text_component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path",
        vertical_strategy="text", horizontal_strategy="text",
    )
    text_result = _materialize_one_row(mod, text_component, str(pdf_path))
    text_df = text_result.output_for_node("tables_out")
    assert len(text_df) >= 1, "'text' strategy must detect the whitespace-aligned borderless table"
    all_data = [cell for row in json.loads(text_df.loc[0, "table_data"]) for cell in row]
    assert "Year" in all_data and "Revenue" in all_data and "Expenses" in all_data


def test_min_rows_min_cols_filters_false_positive_small_tables(mod, tmp_path):
    pdf_path = tmp_path / "mixed.pdf"
    _make_bordered_pdf(
        pdf_path,
        [["Header Only", "X"]],  # 1 row, 2 cols -- a known pdfplumber false-positive shape
        [["Year", "Revenue", "Expenses"], ["2023", "100", "80"], ["2024", "120", "90"]],
    )

    component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path",
        min_rows=2, min_cols=2,
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    df = result.output_for_node("tables_out")
    assert len(df) == 1, "the 1-row false-positive table must be dropped, the real one kept"
    assert df.loc[0, "row_count"] == 3 and df.loc[0, "col_count"] == 3

    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["tables_detected"].value == 2
    assert meta["tables_kept"].value == 1


def test_table_data_preserves_none_for_genuinely_absent_cells(mod, tmp_path):
    """A SPANNED/merged cell is a structurally-absent cell in pdfplumber's own grid geometry
    (None) -- a different thing from a cell that exists but has no text (""). Both must survive
    into table_data exactly as pdfplumber reports them: None stays JSON null, "" stays "".
    """
    pdf_path = tmp_path / "spanned.pdf"
    doc = SimpleDocTemplate(str(pdf_path), pagesize=letter)
    data = [
        ["Year", "Revenue", "Expenses"],
        ["2023 Totals", "", ""],
        ["2024", "120", "90"],
    ]
    t = Table(data)
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ("SPAN", (0, 1), (2, 1)),  # merges the whole second row into one cell
    ]))
    doc.build([t])

    component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path",
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    df = result.output_for_node("tables_out")
    assert len(df) == 1
    data_out = json.loads(df.loc[0, "table_data"])
    assert data_out[1] == ["2023 Totals", None, None], "merged-away cells must be JSON null, not coerced to ''"
    assert data_out[1][1] is None and not isinstance(data_out[1][1], str)


def test_empty_dataframe_has_well_formed_columns_even_with_zero_tables(mod, tmp_path):
    pdf_path = tmp_path / "no_tables.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.drawString(100, 700, "Just a paragraph of prose, no table here at all.")
    c.save()

    component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path",
    )
    result = _materialize_one_row(mod, component, str(pdf_path), extra_cols={"doc_id": "x"})
    df = result.output_for_node("tables_out")
    assert len(df) == 0
    assert "doc_id" in df.columns and "table_index" in df.columns and "table_data" in df.columns


# --- passthrough_columns ----------------------------------------------------------------------

def test_passthrough_columns_explicit_override(mod, tmp_path):
    pdf_path = tmp_path / "one_table.pdf"
    _make_bordered_pdf(pdf_path, [["A", "B"], ["1", "2"], ["3", "4"]])

    component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path",
        passthrough_columns=["doc_id"],
    )
    result = _materialize_one_row(
        mod, component, str(pdf_path), extra_cols={"doc_id": "d1", "other_col": "ignored"},
    )
    df = result.output_for_node("tables_out")
    assert "doc_id" in df.columns
    assert "other_col" not in df.columns


# --- classify_tables (litellm mocked only) ----------------------------------------------------

@pytest.fixture()
def fake_litellm(monkeypatch):
    import sys
    import types

    calls = []
    state = {"response_text": '{"label": "income_statement"}'}

    def fake_completion(model, messages, api_key=None, **kwargs):
        calls.append({"model": model, "messages": messages, "api_key": api_key, "kwargs": kwargs})
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=state["response_text"]))]
        )

    fake_module = types.ModuleType("litellm")
    fake_module.completion = fake_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_module)
    return calls, state


def test_classify_tables_false_makes_zero_litellm_calls(mod, tmp_path, fake_litellm):
    calls, _ = fake_litellm
    pdf_path = tmp_path / "statement.pdf"
    _make_bordered_pdf(
        pdf_path,
        [["Year", "Revenue"], ["2023", "100"], ["2024", "120"]],
        [["Quarter", "Net Income"], ["Q1", "10"], ["Q2", "15"]],
    )

    component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path", classify_tables=False,
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    df = result.output_for_node("tables_out")
    assert len(df) == 2
    assert calls == [], "classify_tables=False must never call litellm"
    assert "table_label" not in df.columns


def test_classify_tables_true_makes_exactly_one_call_per_table(mod, tmp_path, fake_litellm):
    calls, state = fake_litellm
    pdf_path = tmp_path / "statement.pdf"
    _make_bordered_pdf(
        pdf_path,
        [["Year", "Revenue"], ["2023", "100"], ["2024", "120"]],
        [["Quarter", "Net Income"], ["Q1", "10"], ["Q2", "15"]],
    )

    component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path",
        classify_tables=True, classification_model="gpt-4o-mini",
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    df = result.output_for_node("tables_out")
    assert len(df) == 2
    assert len(calls) == 2, "exactly one litellm call per detected table"
    assert list(df["table_label"]) == ["income_statement", "income_statement"]
    assert all(c["model"] == "gpt-4o-mini" for c in calls)

    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["classification_calls"].value == 2
    assert meta["classification_failures"].value == 0


def test_classify_tables_bad_reply_is_null_not_guessed(mod, tmp_path, fake_litellm):
    _, state = fake_litellm
    state["response_text"] = "not json at all"
    pdf_path = tmp_path / "one_table.pdf"
    _make_bordered_pdf(pdf_path, [["A", "B"], ["1", "2"], ["3", "4"]])

    component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path", classify_tables=True,
    )
    result = _materialize_one_row(mod, component, str(pdf_path))
    df = result.output_for_node("tables_out")
    assert df.loc[0, "table_label"] is None
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["classification_failures"].value == 1


def test_parse_classification_response_rejects_unknown_label():
    mod_ = load_component_module()
    with pytest.raises(ValueError, match="not one of"):
        mod_.parse_classification_response('{"label": "not_a_real_label"}', labels=mod_.DEFAULT_TABLE_LABELS)


def test_build_classification_messages_shape():
    mod_ = load_component_module()
    msgs = mod_.build_classification_messages([["a", "b"], ["1", "2"]], labels=["x", "y"])
    assert msgs[0]["role"] == "user"
    assert "['x', 'y']" in msgs[0]["content"]
    assert "[[\"a\", \"b\"], [\"1\", \"2\"]]" in msgs[0]["content"]


# --- partitioning / backfill_policy -----------------------------------------------------------

def test_backfill_policy_wiring(mod):
    cls = mod.FinancialTableExtractorComponent
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
    """Real per-row PDFs (one small bordered table each) across 3 daily partitions, so a
    partition-filtered run's output row COUNT genuinely differs -- not just a filtered-but-
    still-uniform(empty) result, which a nonexistent-file fixture can't distinguish."""
    days = ["2024-01-01", "2024-01-02", "2024-01-03"]
    rows = []
    for day in days:
        for i in range(3):
            pdf_path = tmp_path / f"{day}_{i}.pdf"
            _make_bordered_pdf(pdf_path, [["id", "val"], [str(i), day], ["x", "y"]])
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
        comp = mod.FinancialTableExtractorComponent(**kw)
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

    assert len(full) == 9, "one table per PDF x 9 PDFs across 3 partitions"
    assert len(one_day) == 3, "one partition's worth of PDFs"
    assert len(ranged) == 6, "two partitions' worth via a single_run backfill range"


# --- asset-check wiring ------------------------------------------------------------------------

def test_schema_change_check_is_wired_and_passes_on_an_unchanged_schema(mod, tmp_path):
    pdf_path = tmp_path / "one_table.pdf"
    _make_bordered_pdf(pdf_path, [["A", "B"], ["1", "2"], ["3", "4"]])

    component = mod.FinancialTableExtractorComponent(
        asset_name="tables_out", upstream_asset_key="docs_in", column="pdf_path",
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
