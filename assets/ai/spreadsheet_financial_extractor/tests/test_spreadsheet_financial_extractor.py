"""Committed tests for SpreadsheetFinancialExtractorComponent.

Real `.xlsx` workbooks built with `openpyxl` itself (never mocked), real
openpyxl reads in both `data_only` modes (never mocked). A formula cell's
CACHED value and a genuinely-empty-string cell are both things openpyxl's own
write API cannot produce directly (confirmed against openpyxl's own reader
source: `worksheet/_reader.py`'s `parse_cell` resolves a formula's `<v>` via
`element.findtext(...) or None`, and an inline-string cell needs an explicit
present-but-empty `<is><t/></is>` to read back as `""` rather than `None`) --
both are created here via direct, minimal OOXML injection into a real
openpyxl-saved `.xlsx` zip (`_inject_cached_formula_value` /
`_inject_empty_string_cell`), which is still a 100% real file read back by
100% real openpyxl, not a mock.

Covers: multi-sheet explode, hidden sheet/row/col inclusion both ways,
use_raw_values differing for a real formula cell, trailing-empty-range
trimming, include_cell_formatting capturing real bold/italic/number_format
(and being absent when disabled), None-vs-"" cell preservation,
cell_data_mode='external' side-file writing, partition filtering +
backfill_policy wiring, and the schema-change asset check.
"""
import json
import re
import zipfile

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

openpyxl = pytest.importorskip("openpyxl")

from openpyxl.styles import Font, PatternFill  # noqa: E402


@pytest.fixture()
def mod():
    return load_component_module()


def _inject_cached_formula_value(path, cell_ref: str, cached_value, sheet_file="xl/worksheets/sheet1.xml"):
    """Fill in the empty <v></v> openpyxl already writes next to a formula's <f>,
    so data_only=True reads back a real cached value instead of None."""
    with zipfile.ZipFile(path, "r") as zf:
        data = {n: zf.read(n) for n in zf.namelist()}
    xml = data[sheet_file].decode("utf-8")
    pattern = re.compile(rf'(<c r="{re.escape(cell_ref)}"[^>]*>)(<f>.*?</f>)<v>\s*</v>(</c>)', re.DOTALL)
    new_xml, n = pattern.subn(lambda m: f"{m.group(1)}{m.group(2)}<v>{cached_value}</v>{m.group(3)}", xml)
    assert n == 1, f"expected exactly one formula cell match for {cell_ref}"
    data[sheet_file] = new_xml.encode("utf-8")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in data.items():
            zf.writestr(name, content)


def _inject_empty_string_cell(path, cell_ref: str, sheet_file="xl/worksheets/sheet1.xml"):
    """openpyxl writes a value-less string cell as `<c r=".." t="inlineStr"></c>` (no <is> at
    all) -- indistinguishable from a wholly-absent cell once read back. Injecting an explicit
    *present-but-empty* `<is><t/></is>` makes openpyxl's reader return "" instead of None."""
    with zipfile.ZipFile(path, "r") as zf:
        data = {n: zf.read(n) for n in zf.namelist()}
    xml = data[sheet_file].decode("utf-8")
    old = f'<c r="{cell_ref}" t="inlineStr"></c>'
    new = f'<c r="{cell_ref}" t="inlineStr"><is><t xml:space="preserve"></t></is></c>'
    assert old in xml, f"expected an empty inlineStr cell at {cell_ref}"
    xml = xml.replace(old, new)
    data[sheet_file] = xml.encode("utf-8")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in data.items():
            zf.writestr(name, content)


def _materialize_one_row(component, xlsx_path, extra_cols=None):
    defs = component.build_defs(load_context=None)
    row = {"xlsx_path": xlsx_path}
    row.update(extra_cols or {})
    upstream_df = pd.DataFrame([row])

    @dg.asset(name=component.upstream_asset_key)
    def _upstream():
        return upstream_df

    return dg.materialize([*defs.assets, _upstream])


# --- multi-sheet explode -------------------------------------------------------------------

def test_multi_sheet_workbook_explodes_to_multiple_rows(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws1 = wb.active
    ws1.title = "Revenue"
    ws1["A1"] = "Year"
    ws1["B1"] = "Revenue"
    ws1["A2"] = 2023
    ws1["B2"] = 100
    ws2 = wb.create_sheet("Expenses")
    ws2["A1"] = "Year"
    ws2["B1"] = "Expenses"
    ws2["A2"] = 2023
    ws2["B2"] = 80
    wb.save(path)

    component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
    )
    result = _materialize_one_row(component, str(path), extra_cols={"doc_id": "doc-1"})
    assert result.success
    df = result.output_for_node("sheets_out")

    assert len(df) == 2, "one real workbook with 2 real sheets must explode to 2 rows"
    assert list(df["sheet_name"]) == ["Revenue", "Expenses"]
    assert list(df["sheet_index"]) == [0, 1]
    assert list(df["doc_id"]) == ["doc-1", "doc-1"], "passthrough column must be carried to every exploded row"
    assert "xlsx_path" not in df.columns, "the file column itself is not a passthrough column by default"

    first = json.loads(df.loc[0, "cell_data"])
    assert first == [["Year", "Revenue"], [2023, 100]]
    assert df.loc[0, "row_count"] == 2 and df.loc[0, "col_count"] == 2
    assert "Revenue" in df.loc[0, "sheet_markdown"]

    second = json.loads(df.loc[1, "cell_data"])
    assert second == [["Year", "Expenses"], [2023, 80]]


# --- hidden sheets ---------------------------------------------------------------------------

def test_hidden_sheet_excluded_by_default_and_included_when_enabled(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws1 = wb.active
    ws1.title = "Visible"
    ws1["A1"] = "x"
    ws2 = wb.create_sheet("Scratch")
    ws2["A1"] = "y"
    ws2.sheet_state = "hidden"
    wb.save(path)

    default_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
    )
    result = _materialize_one_row(default_component, str(path))
    df = result.output_for_node("sheets_out")
    assert len(df) == 1, "a hidden sheet must be excluded by default"
    assert list(df["sheet_name"]) == ["Visible"]

    included_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        include_hidden_sheets=True,
    )
    result2 = _materialize_one_row(included_component, str(path))
    df2 = result2.output_for_node("sheets_out")
    assert len(df2) == 2, "include_hidden_sheets=True must include the hidden sheet"
    assert set(df2["sheet_name"]) == {"Visible", "Scratch"}


# --- hidden rows / cols ------------------------------------------------------------------------

def test_hidden_rows_excluded_by_default_and_included_when_enabled(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = "row1"
    ws["A2"] = "row2-hidden"
    ws["A3"] = "row3"
    ws.row_dimensions[2].hidden = True
    wb.save(path)

    default_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        only_data_rows=False, only_data_cols=False,
    )
    df = _materialize_one_row(default_component, str(path)).output_for_node("sheets_out")
    assert df.loc[0, "row_count"] == 2
    data = json.loads(df.loc[0, "cell_data"])
    flat = [cell for row in data for cell in row]
    assert "row2-hidden" not in flat and "row1" in flat and "row3" in flat

    included_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        include_hidden_rows=True, only_data_rows=False, only_data_cols=False,
    )
    df2 = _materialize_one_row(included_component, str(path)).output_for_node("sheets_out")
    assert df2.loc[0, "row_count"] == 3
    data2 = json.loads(df2.loc[0, "cell_data"])
    flat2 = [cell for row in data2 for cell in row]
    assert "row2-hidden" in flat2


def test_hidden_cols_excluded_by_default_and_included_when_enabled(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = "col-a"
    ws["B1"] = "col-b-hidden"
    ws["C1"] = "col-c"
    ws.column_dimensions["B"].hidden = True
    wb.save(path)

    default_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        only_data_rows=False, only_data_cols=False,
    )
    df = _materialize_one_row(default_component, str(path)).output_for_node("sheets_out")
    assert df.loc[0, "col_count"] == 2
    data = json.loads(df.loc[0, "cell_data"])
    assert "col-b-hidden" not in data[0] and "col-a" in data[0] and "col-c" in data[0]

    included_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        include_hidden_cols=True, only_data_rows=False, only_data_cols=False,
    )
    df2 = _materialize_one_row(included_component, str(path)).output_for_node("sheets_out")
    assert df2.loc[0, "col_count"] == 3
    data2 = json.loads(df2.loc[0, "cell_data"])
    assert "col-b-hidden" in data2[0]


# --- use_raw_values / data_only ----------------------------------------------------------------

def test_use_raw_values_true_vs_false_differ_for_formula_cell(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = 10
    ws["A2"] = 20
    ws["B2"] = "=SUM(A1:A2)"
    wb.save(path)
    _inject_cached_formula_value(path, "B2", 30)

    raw_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path", use_raw_values=True,
    )
    raw_df = _materialize_one_row(raw_component, str(path)).output_for_node("sheets_out")
    raw_data = json.loads(raw_df.loc[0, "cell_data"])
    assert raw_data[1][1] == 30, "data_only=True must return the formula's cached value"

    formula_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path", use_raw_values=False,
    )
    formula_df = _materialize_one_row(formula_component, str(path)).output_for_node("sheets_out")
    formula_data = json.loads(formula_df.loc[0, "cell_data"])
    assert formula_data[1][1] == "=SUM(A1:A2)", "data_only=False must return the formula string, never a computed value"


# --- trailing empty trimming -------------------------------------------------------------------

def test_trailing_empty_rows_and_cols_trimmed_by_default(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = "h1"
    ws["B1"] = "h2"
    ws["A2"] = "x"
    ws["B2"] = "y"
    # A styled-but-valueless cell far out -- extends openpyxl's own tracked
    # used range (max_row/max_column) without providing any real data.
    far_cell = ws.cell(row=5, column=4)
    far_cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    wb.save(path)

    trimmed_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
    )
    df = _materialize_one_row(trimmed_component, str(path)).output_for_node("sheets_out")
    assert df.loc[0, "row_count"] == 2, "trailing fully-empty rows must be trimmed by default"
    assert df.loc[0, "col_count"] == 2, "trailing fully-empty cols must be trimmed by default"

    untrimmed_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        only_data_rows=False, only_data_cols=False,
    )
    df2 = _materialize_one_row(untrimmed_component, str(path)).output_for_node("sheets_out")
    assert df2.loc[0, "row_count"] == 5, "only_data_rows=False must keep openpyxl's full tracked range"
    assert df2.loc[0, "col_count"] == 4, "only_data_cols=False must keep openpyxl's full tracked range"


# --- include_cell_formatting -------------------------------------------------------------------

def test_include_cell_formatting_captures_real_bold_italic_number_format(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = "Revenue"
    ws["A1"].font = Font(bold=True)
    ws["A2"] = "note"
    ws["A2"].font = Font(italic=True)
    ws["B1"] = 1234.5
    ws["B1"].number_format = "#,##0.00"
    ws["A3"] = 10
    ws["B3"] = "=A3*2"
    wb.save(path)
    _inject_cached_formula_value(path, "B3", 20)

    disabled_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        only_data_rows=False, only_data_cols=False,
    )
    df_disabled = _materialize_one_row(disabled_component, str(path)).output_for_node("sheets_out")
    assert "cell_formatting_json" not in df_disabled.columns, "must be absent entirely when disabled"

    enabled_component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        include_cell_formatting=True, only_data_rows=False, only_data_cols=False,
    )
    df_enabled = _materialize_one_row(enabled_component, str(path)).output_for_node("sheets_out")
    fmt = json.loads(df_enabled.loc[0, "cell_formatting_json"])

    assert fmt[0][0]["bold"] is True and fmt[0][0]["italic"] is False
    assert fmt[1][0]["italic"] is True and fmt[1][0]["bold"] is False
    assert fmt[0][1]["number_format"] == "#,##0.00"
    assert fmt[2][1]["formula"] == "=A3*2", "formula string must be captured alongside formatting"

    cell_data = json.loads(df_enabled.loc[0, "cell_data"])
    assert cell_data[2][1] == 20, "cell_data value still follows use_raw_values (default True) even with formatting on"


# --- None vs "" preservation ---------------------------------------------------------------------

def test_cell_data_preserves_none_vs_empty_string_distinctly(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = "present"
    ws["B1"] = ""
    # C1 is left entirely untouched -- genuinely absent.
    wb.save(path)
    _inject_empty_string_cell(path, "B1")

    component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        only_data_rows=False, only_data_cols=False,
    )
    # Force a real used range covering C1 too, so it isn't trimmed away.
    # (only_data_cols=False keeps openpyxl's own tracked max_column as-is.)
    df = _materialize_one_row(component, str(path)).output_for_node("sheets_out")
    data = json.loads(df.loc[0, "cell_data"])
    assert data[0][0] == "present"
    assert data[0][1] == "", "a genuinely-empty-string cell must stay \"\", not null"
    assert data[0][1] is not None and isinstance(data[0][1], str)


# --- cell_data_mode='external' -------------------------------------------------------------------

def test_cell_data_mode_external_writes_side_jsonl_file_over_threshold(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    for r in range(1, 4):
        for c in range(1, 4):
            ws.cell(row=r, column=c, value=r * 10 + c)
    wb.save(path)

    out_dir = tmp_path / "side_files"
    component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        cell_data_mode="external", external_threshold_cells=1, output_dir=str(out_dir),
    )
    result = _materialize_one_row(component, str(path))
    df = result.output_for_node("sheets_out")
    cell_data = json.loads(df.loc[0, "cell_data"])
    assert cell_data["is_external"] is True
    assert cell_data["count"] == 9
    side_path = cell_data["path"]
    assert side_path.startswith(str(out_dir))

    lines = open(side_path).read().strip().splitlines()
    assert len(lines) == 9
    parsed = [json.loads(ln) for ln in lines]
    assert {p["value"] for p in parsed} == {11, 12, 13, 21, 22, 23, 31, 32, 33}

    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["external_cell_files_written"].value == 1


def test_cell_data_mode_inline_under_threshold_stays_inline(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = 1
    ws["B1"] = 2
    wb.save(path)

    component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        cell_data_mode="external", external_threshold_cells=50000,
    )
    df = _materialize_one_row(component, str(path)).output_for_node("sheets_out")
    cell_data = json.loads(df.loc[0, "cell_data"])
    assert cell_data == [[1, 2]], "under threshold, external mode must still inline the grid"


# --- passthrough_columns ----------------------------------------------------------------------

def test_passthrough_columns_explicit_override(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = 1
    ws["B1"] = 2
    wb.save(path)

    component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
        passthrough_columns=["doc_id"],
    )
    df = _materialize_one_row(
        component, str(path), extra_cols={"doc_id": "d1", "other_col": "ignored"},
    ).output_for_node("sheets_out")
    assert "doc_id" in df.columns
    assert "other_col" not in df.columns


# --- partitioning / backfill_policy -----------------------------------------------------------

def test_backfill_policy_wiring(mod):
    cls = mod.SpreadsheetFinancialExtractorComponent
    base = dict(
        asset_name="out_asset", upstream_asset_key="src", column="xlsx_path",
        partition_type="daily", partition_start="2024-01-01", partition_date_column="event_date",
    )
    assets = cls(**base, backfill_policy="single_run").build_defs(None).assets
    assert assets[0].backfill_policy == dg.BackfillPolicy.single_run()

    assets = cls(**base, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=3).build_defs(None).assets
    assert assets[0].backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=3)

    assets = cls(**base).build_defs(None).assets
    assert assets[0].backfill_policy is None

    unpartitioned = dict(asset_name="out_asset", upstream_asset_key="src", column="xlsx_path")
    assets = cls(**unpartitioned, backfill_policy="single_run").build_defs(None).assets
    assert assets[0].backfill_policy is None


def test_partition_filtering_and_single_run_range_use_real_workbooks(mod, tmp_path):
    days = ["2024-01-01", "2024-01-02", "2024-01-03"]
    rows = []
    for day in days:
        for i in range(3):
            path = tmp_path / f"{day}_{i}.xlsx"
            wb = openpyxl.Workbook()
            ws = wb.active
            ws["A1"] = "id"
            ws["A2"] = i
            wb.save(path)
            rows.append({"doc_id": f"{day}_{i}", "event_date": day, "xlsx_path": str(path)})
    src_df = pd.DataFrame(rows)

    @dg.asset(name="src")
    def src():
        return src_df

    def _materialize(partitioned, partition_key=None, tags=None, **overrides):
        kw = dict(asset_name="out_asset", upstream_asset_key="src", column="xlsx_path")
        if partitioned:
            kw.update(partition_type="daily", partition_start="2024-01-01", partition_date_column="event_date")
        kw.update(overrides)
        comp = mod.SpreadsheetFinancialExtractorComponent(**kw)
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

    assert len(full) == 9, "one sheet per workbook x 9 workbooks across 3 partitions"
    assert len(one_day) == 3, "one partition's worth of workbooks"
    assert len(ranged) == 6, "two partitions' worth via a single_run backfill range"


# --- asset-check wiring --------------------------------------------------------------------------

def test_schema_change_check_is_wired_and_passes_on_an_unchanged_schema(mod, tmp_path):
    path = tmp_path / "book.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws["A1"] = 1
    ws["B1"] = 2
    wb.save(path)

    component = mod.SpreadsheetFinancialExtractorComponent(
        asset_name="sheets_out", upstream_asset_key="docs_in", column="xlsx_path",
    )
    defs = component.build_defs(load_context=None)
    assert len(list(defs.asset_checks)) >= 1, "build_column_schema_change_checks must produce at least one check"

    upstream_df = pd.DataFrame([{"xlsx_path": str(path)}])

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
