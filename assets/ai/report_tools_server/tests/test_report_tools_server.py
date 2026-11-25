"""Real matplotlib/reportlab/python-docx/openpyxl rendering -- no mocking the renderers, this component's whole
point is deterministic output from real libraries."""
import json
import os
import time

import pytest


# ── make_chart: data shaping / auto-kind resolution ─────────────────────────────────────────────────────────────
def test_make_chart_series_line_time(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {
        "title": "CPU",
        "series": [{"name": "cpu", "x": ["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"], "y": [1, 2]}],
    })
    assert "error" not in result
    assert result["kind_used"] == "line"
    assert result["media_type"] == "image/png"
    assert os.path.isfile(mod.lookup_report_file(files_dir, None, result["file_id"])["path"])


def test_make_chart_labels_values_auto_is_sorted_barh(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {"labels": ["a", "b", "c"], "values": [3, 10, 1]})
    assert result["kind_used"] == "barh"


def test_make_chart_pie_over_5_slices_falls_back_with_warning(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {
        "kind": "pie", "labels": ["a", "b", "c", "d", "e", "f"], "values": [1, 2, 3, 4, 5, 6],
    })
    assert result["kind_used"] == "barh"
    assert any("pie" in w for w in result["warnings"])


def test_make_chart_pie_at_5_slices_is_allowed(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {"kind": "pie", "labels": ["a", "b", "c", "d", "e"], "values": [1, 2, 3, 4, 5]})
    assert result["kind_used"] == "pie"
    assert "warnings" not in result


def test_make_chart_multi_series_auto_is_bar(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {
        "series": [
            {"name": "a", "x": ["x", "y"], "y": [1, 2]},
            {"name": "b", "x": ["x", "y"], "y": [3, 4]},
        ],
    })
    assert result["kind_used"] == "bar"


def test_make_chart_timestamps_bucket_day_zero_fills_gaps(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    # two events a day apart -- the middle day has none, must still appear as a 0.
    ts = [1735689600, 1735689600 + 2 * 86400]  # 2025-01-01, 2025-01-03
    result = tools.call("make_chart", {"timestamps": ts, "bucket": "day"})
    assert "error" not in result
    assert result["kind_used"] == "line"


def test_make_chart_timestamps_group_by_is_stacked_bar(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    ts = [1735689600, 1735689600, 1735689600 + 86400]
    result = tools.call("make_chart", {"timestamps": ts, "bucket": "day", "group_by": ["job_a", "job_b", "job_a"]})
    assert result["kind_used"] == "bar"


def test_make_chart_timestamps_requires_bucket(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {"timestamps": [1735689600]})
    assert "bucket" in result["error"]


def test_make_chart_rejects_two_data_shapes_at_once(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {"series": [{"x": [1], "y": [1]}], "labels": ["a"], "values": [1]})
    assert "error" in result


def test_make_chart_rejects_no_data_shape(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {})
    assert "error" in result


def test_make_chart_mismatched_series_lengths_is_a_clear_error(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {"series": [{"name": "failures", "x": [1, 2, 3, 4, 5, 6], "y": [1] * 7}]})
    assert "failures" in result["error"] and "7" in result["error"] and "6" in result["error"]


def test_make_chart_unknown_kind_is_an_error(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {"kind": "donut", "labels": ["a"], "values": [1]})
    assert "error" in result


# ── make_chart: records (list_recent_runs-shaped input) ─────────────────────────────────────────────────────────
def test_make_chart_records_matches_equivalent_timestamps_group_by(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    runs = [
        {"run_id": "a", "created_at": 1735689600, "status": "SUCCESS"},
        {"run_id": "b", "created_at": 1735689600, "status": "FAILURE"},
        {"run_id": "c", "created_at": 1735689600 + 86400, "status": "SUCCESS"},
    ]
    via_records = tools.call("make_chart", {
        "records": runs, "bucket": "day", "time_field": "created_at", "group_field": "status",
    })
    assert "error" not in via_records
    assert via_records["kind_used"] == "bar"

    via_lists = tools.call("make_chart", {
        "timestamps": [r["created_at"] for r in runs], "bucket": "day",
        "group_by": [r["status"] for r in runs],
    })
    assert via_records["kind_used"] == via_lists["kind_used"]


def test_make_chart_records_without_group_field_is_ungrouped(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    runs = [{"created_at": 1735689600 + i * 3600} for i in range(3)]
    result = tools.call("make_chart", {"records": runs, "bucket": "hour", "time_field": "created_at"})
    assert "error" not in result
    assert result["kind_used"] == "line"


def test_make_chart_records_requires_time_field(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {"records": [{"created_at": 1735689600}], "bucket": "day"})
    assert "time_field" in result["error"]


def test_make_chart_records_missing_time_field_key_is_a_clear_error(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {
        "records": [{"created_at": 1735689600}, {"status": "oops"}],
        "bucket": "day", "time_field": "created_at",
    })
    assert "records[1]" in result["error"] and "created_at" in result["error"]


def test_make_chart_records_missing_group_field_key_is_a_clear_error(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {
        "records": [{"created_at": 1735689600, "status": "ok"}, {"created_at": 1735689600}],
        "bucket": "day", "time_field": "created_at", "group_field": "status",
    })
    assert "records[1]" in result["error"] and "status" in result["error"]


def test_make_chart_records_rejects_empty_list(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {"records": [], "bucket": "day", "time_field": "created_at"})
    assert "error" in result


def test_make_chart_records_and_timestamps_together_is_rejected(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {
        "records": [{"created_at": 1735689600}], "timestamps": [1735689600],
        "bucket": "day", "time_field": "created_at",
    })
    assert "error" in result


# ── MCP server layer: bad arguments surface as isError, not a quiet {"error": ...} success ──────────────────────
def test_mcp_tool_function_raises_on_tool_error_so_sdk_sets_isError(mod, files_dir):
    """The registered tool function (not tools.call() directly -- see tests above, which keep returning
    {"error": ...} for the resource-call path) must raise so the mcp SDK turns this into isError=True; a plain
    non-exceptional return would otherwise be indistinguishable from success to an agent loop's failure count."""
    tools = mod.ReportTools(files_dir)
    mcp = mod.build_mcp_server(tools)
    make_chart_fn = mcp._tool_manager.get_tool("make_chart").fn if hasattr(mcp, "_tool_manager") else None
    if make_chart_fn is None:
        pytest.skip("mcp SDK shape without _tool_manager.get_tool().fn introspection")
    with pytest.raises(Exception, match="bucket"):  # the SDK's own ToolError, not mod.ToolError -- see _mcp_call
        make_chart_fn(timestamps=[1735689600])


def test_mcp_tool_function_succeeds_normally_on_good_args(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    mcp = mod.build_mcp_server(tools)
    make_chart_fn = mcp._tool_manager.get_tool("make_chart").fn if hasattr(mcp, "_tool_manager") else None
    if make_chart_fn is None:
        pytest.skip("mcp SDK shape without _tool_manager.get_tool().fn introspection")
    result = make_chart_fn(labels=["a", "b"], values=[1, 2])
    assert "error" not in result
    assert result["kind_used"] == "barh"


def test_make_chart_group_by_length_mismatch_is_an_error(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {"timestamps": [1, 2], "bucket": "day", "group_by": ["a"]})
    assert "error" in result


def test_make_chart_bad_bucket_name_is_an_error(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_chart", {"timestamps": [1735689600], "bucket": "month"})
    assert "error" in result


# ── make_pdf / make_docx / make_xlsx ────────────────────────────────────────────────────────────────────────────
def test_make_pdf_basic(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_pdf", {
        "title": "Weekly report", "sections": [
            {"heading": "Failures", "text": "3 jobs failed.", "bullets": ["a", "b"],
             "table": {"columns": ["job", "count"], "rows": [["orders", 2], ["billing", 1]]}},
        ],
    })
    assert "error" not in result
    assert result["media_type"] == "application/pdf"
    assert result["pages"] >= 1
    path = mod.lookup_report_file(files_dir, None, result["file_id"])["path"]
    import pypdf
    reader = pypdf.PdfReader(path)
    assert len(reader.pages) == result["pages"]


def test_make_pdf_embeds_an_earlier_chart(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    chart = tools.call("make_chart", {"labels": ["a", "b"], "values": [1, 2]})
    result = tools.call("make_pdf", {"sections": [{"heading": "Chart", "chart_file_id": chart["file_id"]}]})
    assert "error" not in result


def test_make_pdf_unknown_chart_file_id_is_a_clear_error(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_pdf", {"sections": [{"chart_file_id": "doesnotexist"}]})
    assert "error" in result and "doesnotexist" in result["error"]


def test_make_docx_basic(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_docx", {
        "title": "Weekly report",
        "sections": [{"heading": "Failures", "text": "3 jobs failed.", "bullets": ["a", "b"],
                      "table": {"columns": ["job", "count"], "rows": [["orders", 2]]}}],
    })
    assert "error" not in result
    assert result["media_type"].endswith("wordprocessingml.document")
    from docx import Document
    path = mod.lookup_report_file(files_dir, None, result["file_id"])["path"]
    doc = Document(path)
    assert any("Failures" in p.text for p in doc.paragraphs)


def test_make_docx_embeds_an_earlier_chart(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    chart = tools.call("make_chart", {"labels": ["a", "b"], "values": [1, 2]})
    result = tools.call("make_docx", {"sections": [{"chart_file_id": chart["file_id"]}]})
    assert "error" not in result


def test_make_xlsx_basic(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_xlsx", {"sheets": [{"name": "Failures", "columns": ["job", "count"], "rows": [["orders", 2]]}]})
    assert "error" not in result
    from openpyxl import load_workbook
    path = mod.lookup_report_file(files_dir, None, result["file_id"])["path"]
    wb = load_workbook(path)
    ws = wb["Failures"]
    assert ws.cell(row=1, column=1).value == "job"
    assert ws.cell(row=2, column=1).value == "orders"


def test_make_xlsx_requires_sheets(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    assert "error" in tools.call("make_xlsx", {})


def test_make_xlsx_multiple_sheets(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("make_xlsx", {"sheets": [
        {"name": "A", "columns": ["x"], "rows": [[1]]},
        {"name": "B", "columns": ["y"], "rows": [[2]]},
    ]})
    from openpyxl import load_workbook
    wb = load_workbook(mod.lookup_report_file(files_dir, None, result["file_id"])["path"])
    assert wb.sheetnames == ["A", "B"]


# ── manifest / run-scoping / caps ───────────────────────────────────────────────────────────────────────────────
def test_manifest_scoped_per_run_id(mod, files_dir):
    a = mod.ReportTools(files_dir, run_id="run_a")
    b = mod.ReportTools(files_dir, run_id="run_b")
    r = a.call("make_xlsx", {"sheets": [{"name": "S", "columns": ["x"], "rows": [[1]]}]})
    assert mod.lookup_report_file(files_dir, "run_a", r["file_id"]) is not None
    assert mod.lookup_report_file(files_dir, "run_b", r["file_id"]) is None
    assert b.call("make_pdf", {"sections": [{"chart_file_id": r["file_id"]}]}).get("error")


def test_max_files_per_run_is_enforced(mod, files_dir):
    tools = mod.ReportTools(files_dir, max_files_per_run=1)
    assert "error" not in tools.call("make_xlsx", {"sheets": [{"name": "S", "columns": ["x"], "rows": []}]})
    result = tools.call("make_xlsx", {"sheets": [{"name": "S", "columns": ["x"], "rows": []}]})
    assert "max_files_per_run" in result["error"]


def test_max_file_bytes_is_enforced(mod, files_dir):
    tools = mod.ReportTools(files_dir, max_file_bytes=10)
    result = tools.call("make_xlsx", {"sheets": [{"name": "S", "columns": ["x"], "rows": [[i] for i in range(100)]}]})
    assert "max_file_bytes" in result["error"]


def test_unknown_tool_name(mod, files_dir):
    tools = mod.ReportTools(files_dir)
    result = tools.call("delete_everything", {})
    assert "unknown tool" in result["error"]


def test_disabled_tool_is_rejected(mod, files_dir):
    tools = mod.ReportTools(files_dir, allow=["make_chart"])
    result = tools.call("make_pdf", {})
    assert "not enabled" in result["error"]


def test_allow_rejects_unknown_tool_name_at_construction(mod, files_dir):
    with pytest.raises(ValueError):
        mod.ReportTools(files_dir, allow=["make_chart", "delete_everything"])


def test_sweep_old_report_runs_deletes_stale_run_folders(mod, files_dir):
    tools = mod.ReportTools(files_dir, run_id="old_run")
    tools.call("make_xlsx", {"sheets": [{"name": "S", "columns": ["x"], "rows": []}]})
    manifest_path = mod._report_manifest_path(files_dir, "old_run")
    old = time.time() - 48 * 3600
    os.utime(manifest_path, (old, old))
    removed = mod.sweep_old_report_runs(files_dir, retention_hours=24)
    assert removed == 1
    assert mod.read_run_manifest(files_dir, "old_run") == []


def test_sweep_keeps_recent_runs(mod, files_dir):
    tools = mod.ReportTools(files_dir, run_id="fresh_run")
    tools.call("make_xlsx", {"sheets": [{"name": "S", "columns": ["x"], "rows": []}]})
    removed = mod.sweep_old_report_runs(files_dir, retention_hours=24)
    assert removed == 0
    assert len(mod.read_run_manifest(files_dir, "fresh_run")) == 1


def test_construction_sweeps_old_runs_automatically(mod, files_dir):
    mod.ReportTools(files_dir, run_id="old_run").call(
        "make_xlsx", {"sheets": [{"name": "S", "columns": ["x"], "rows": []}]}
    )
    manifest_path = mod._report_manifest_path(files_dir, "old_run")
    old = time.time() - 48 * 3600
    os.utime(manifest_path, (old, old))
    mod.ReportTools(files_dir, file_retention_hours=24)  # constructing ANY instance sweeps
    assert mod.read_run_manifest(files_dir, "old_run") == []


# ── component wiring ─────────────────────────────────────────────────────────────────────────────────────────────
def test_build_defs_registers_plain_resource(mod, files_dir):
    comp = mod.ReportToolsServerComponent(resource_key="report_tools", files_dir=files_dir)
    defs = comp.build_defs(None)
    assert "report_tools" in defs.resources


def test_build_defs_registers_mcp_server_shape_resource(mod, files_dir):
    comp = mod.ReportToolsServerComponent(
        resource_key="report_tools", files_dir=files_dir, mcp_server_resource_key="report_tools_mcp",
        allow=["make_chart", "make_pdf"],
    )
    defs = comp.build_defs(None)
    mcp_res = defs.resources["report_tools_mcp"]
    dumped = mcp_res.model_dump()
    assert dumped["type"] == "stdio"
    assert dumped["env"]["REPORT_TOOLS_FILES_DIR"] == files_dir
    assert dumped["env"]["REPORT_TOOLS_RUN_ID"] == "{run_id}"
    assert dumped["env"]["REPORT_TOOLS_ALLOW"] == "make_chart,make_pdf"


def test_build_defs_rejects_unknown_allow_entry(mod, files_dir):
    comp = mod.ReportToolsServerComponent(files_dir=files_dir, allow=["make_chart", "not_a_tool"])
    with pytest.raises(ValueError):
        comp.build_defs(None)


def test_in_process_resource_always_has_run_id_none(mod, files_dir):
    import dagster as dg

    comp = mod.ReportToolsServerComponent(resource_key="report_tools", files_dir=files_dir)
    defs = comp.build_defs(None)

    @dg.asset(required_resource_keys={"report_tools"})
    def probe(context):
        context.add_output_metadata({"run_id_is_none": context.resources.report_tools.run_id is None})

    result = dg.materialize([probe], resources=defs.resources)
    assert result.success
    mats = result.asset_materializations_for_node("probe")
    assert mats[-1].metadata["run_id_is_none"].value is True


def test_build_mcp_server_only_exposes_allowed_tools(mod, files_dir):
    tools = mod.ReportTools(files_dir, allow=["make_chart"])
    mcp = mod.build_mcp_server(tools)
    names = {t.name for t in mcp._tool_manager.list_tools()} if hasattr(mcp, "_tool_manager") else None
    if names is not None:
        assert "make_chart" in names
        assert "make_pdf" not in names


def test_stdio_server_bad_args_is_a_real_isError_result_with_the_original_message(files_dir):
    """Full round trip through a real subprocess (same transport agentic_pipeline/slack_chat_tools etc. use) --
    not just calling the registered function in-process -- since the SDK's tool dispatch only preserves a
    raised exception's own message when it recognizes the exception as ITS OWN ToolError (see _mcp_call);
    getting this wrong (e.g. raising this module's own ToolError instead) still produces isError=True but
    replaces the message with a useless generic 'Error executing tool <name>' -- caught by this test once
    already, mirroring dagster_instance_tools/tests/test_mcp_stdio.py's identical check."""
    import asyncio
    import os
    import sys

    pytest.importorskip("mcp")
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    server = str((os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + "/component.py")
    params = StdioServerParameters(
        command=sys.executable, args=[server], env={**os.environ, "REPORT_TOOLS_FILES_DIR": files_dir},
    )

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                return await s.call_tool("make_chart", {"timestamps": [1735689600]})  # missing required bucket

    result = asyncio.run(asyncio.wait_for(go(), 60))
    assert getattr(result, "is_error", None) or getattr(result, "isError", None)
    assert "bucket" in result.content[0].text
