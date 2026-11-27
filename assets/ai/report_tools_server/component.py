"""ReportToolsServerComponent -- deterministic chart / PDF / Word / Excel file builders for the chat agent.

Goal: someone asks the Slack (or Teams) bot to "chart failures over the last week" or "put this in a PDF", and
gets the file in the thread. The model never writes plotting or document code -- it fills in a schema (chart
kind + series, or report sections) and this server renders it deterministically with matplotlib / reportlab /
python-docx / openpyxl. No network, no credentials: every tool's output is a pure function of its arguments.

Same shape as dagster_instance_tools / knowledge_search_server: this file is BOTH
  1. an importable toolset (`ReportTools`, plus `ReportToolsServerComponent` that registers it as a Dagster
     resource), and
  2. a self-contained stdio MCP server (`python -m <dotted.path.to.this.module>`), so any mcp_servers-aware
     agent (anthropic_agent / openai_agent / litellm_agent / gemini_agent / agentic_pipeline) can call it.

Generated files are written under `files_dir/<dagster_run_id>/`, one JSON "manifest" per run recording every
file's id/name/media_type/path/size -- the same `report-manifest` block is inlined (byte-identical, see
tests/test_report_manifest_drift.py) into slack_reply / teams_reply, which read it after the run to post every
file into the thread (see those components' READMEs, "Delivering generated files"). A `chart_file_id` returned
by make_chart can be passed back into make_pdf / make_docx's `sections[].chart_file_id` to embed that same chart,
since it's still sitting in this run's manifest.

Tools (all return compact JSON-able dicts; errors come back as {"error": ...}, never exceptions):
  make_chart, make_pdf, make_docx, make_xlsx.
This is the contract for `ReportTools.call()` (used directly by assets/ops). The stdio MCP server built by
build_mcp_server() wraps every tool in `_mcp_call`, which re-raises that {"error": ...} as an exception instead
-- the `mcp` package turns a raised exception into a real isError=True tool result on both 1.x and 2.x, which an
agent loop's own failed-call accounting keys off; a plain non-exceptional {"error": ...} return is otherwise
indistinguishable from a successful result to that accounting (confirmed: it was silently treated as data).

Requirements: matplotlib, reportlab, python-docx, openpyxl (see requirements.txt / manifest.json's
dependencies.pip -- a code location missing one of these fails importing this module, same as any other
optional-dependency component in this repo).
"""

import io
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


# BEGIN report-manifest (inlined per component; keep copies identical across report_tools_server / slack_reply /
# teams_reply / slack_chat_tools -- see tests/test_report_manifest_drift.py)
import json as _rm_json  # noqa: E402,F401
import os as _rm_os  # noqa: E402,F401
import shutil as _rm_shutil  # noqa: E402,F401
import threading as _rm_threading  # noqa: E402,F401
import time as _rm_time  # noqa: E402,F401

_REPORT_MANIFEST_LOCK = _rm_threading.Lock()


def _report_run_dir(files_dir: str, run_id: Optional[str]) -> str:
    return _rm_os.path.join(_rm_os.path.expanduser(files_dir), run_id or "_unscoped")


def _report_manifest_path(files_dir: str, run_id: Optional[str]) -> str:
    return _rm_os.path.join(_report_run_dir(files_dir, run_id), "manifest.json")


def read_run_manifest(files_dir: str, run_id: Optional[str]) -> List[Dict[str, Any]]:
    """Every file {file_id, name, media_type, path, size_bytes, tool, created_at} report_tools_server has
    written for this run so far. Trusted, run-scoped lookup -- same reasoning as receipts_db_path's own
    receipt_ids_for_run: a caller only ever reads its OWN run's files by the run id Dagster itself put on the
    run (never an argument an agent could spoof)."""
    path = _report_manifest_path(files_dir, run_id)
    if not _rm_os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = _rm_json.load(f)
        return data if isinstance(data, list) else []
    except (OSError, ValueError):
        return []


def append_to_report_manifest(files_dir: str, run_id: Optional[str], entry: Dict[str, Any]) -> None:
    run_dir = _report_run_dir(files_dir, run_id)
    with _REPORT_MANIFEST_LOCK:
        _rm_os.makedirs(run_dir, exist_ok=True)
        existing = read_run_manifest(files_dir, run_id)
        existing.append(entry)
        manifest_path = _report_manifest_path(files_dir, run_id)
        tmp_path = manifest_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            _rm_json.dump(existing, f)
        _rm_os.replace(tmp_path, manifest_path)


def lookup_report_file(files_dir: str, run_id: Optional[str], file_id: str) -> Optional[Dict[str, Any]]:
    for entry in read_run_manifest(files_dir, run_id):
        if entry.get("file_id") == file_id:
            return entry
    return None


def sweep_old_report_runs(files_dir: str, retention_hours: float) -> int:
    """Delete run folders under files_dir whose manifest.json is older than retention_hours. Best-effort: a run
    folder with no manifest.json yet, or one that loses a race with a concurrent writer, is skipped rather than
    raised -- this runs opportunistically on every tool call, not as a scheduled job."""
    base = _rm_os.path.expanduser(files_dir)
    if not _rm_os.path.isdir(base):
        return 0
    cutoff = _rm_time.time() - max(0.0, retention_hours) * 3600
    removed = 0
    for name in list(_rm_os.listdir(base)):
        run_dir = _rm_os.path.join(base, name)
        manifest = _rm_os.path.join(run_dir, "manifest.json")
        if not _rm_os.path.isdir(run_dir) or not _rm_os.path.isfile(manifest):
            continue
        try:
            if _rm_os.path.getmtime(manifest) < cutoff:
                _rm_shutil.rmtree(run_dir, ignore_errors=True)
                removed += 1
        except OSError:
            continue
    return removed
# END report-manifest


_AUDIT = logging.getLogger("report_tools.audit")

ALL_TOOLS = ("make_chart", "make_pdf", "make_docx", "make_xlsx")

# Okabe-Ito colour-blind-safe palette (black dropped -- reserved for axes/text, not data series).
_PALETTE = ["#0072B2", "#E69F00", "#009E73", "#CC79A7", "#D55E00", "#56B4E9", "#F0E442"]

_BUCKET_STEP = {"hour": timedelta(hours=1), "day": timedelta(days=1), "week": timedelta(weeks=1)}
_BUCKET_FMT = {"hour": "%Y-%m-%d %H:%M", "day": "%Y-%m-%d", "week": "%Y-%m-%d"}


class ToolError(Exception):
    """A user-facing, safe error message (returned to the agent as {"error": ...})."""


# ── chart data shaping ────────────────────────────────────────────────────────────────────────────────────────────
def _parse_ts(v: Any) -> datetime:
    if isinstance(v, bool) or not isinstance(v, (int, float, str)):
        raise ToolError(f"timestamp must be a number or an ISO 8601 string, got {v!r}")
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=timezone.utc)
    s = v.strip()
    try:
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        raise ToolError(f"could not parse timestamp {v!r} (expected epoch seconds or an ISO 8601 string)")


def _floor_bucket(dt: datetime, bucket: str) -> datetime:
    if bucket not in _BUCKET_STEP:
        raise ToolError(f"bucket must be one of 'hour', 'day', 'week', got {bucket!r}")
    if bucket == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    if bucket == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    d = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return d - timedelta(days=d.weekday())  # week: Monday start


def _bucket_series(timestamps: List[Any], bucket: str, group_by: Optional[List[str]]) -> List[Dict[str, Any]]:
    if not timestamps:
        raise ToolError("timestamps must be a non-empty list")
    if group_by is not None and len(group_by) != len(timestamps):
        raise ToolError(f"group_by has {len(group_by)} entries but timestamps has {len(timestamps)}")
    floored = [_floor_bucket(_parse_ts(t), bucket) for t in timestamps]
    step = _BUCKET_STEP[bucket]
    lo, hi = min(floored), max(floored)
    axis: List[datetime] = []
    cur = lo
    while cur <= hi:  # zero-filled axis -- a chart from this never has gaps
        axis.append(cur)
        cur += step
    groups = [str(g) for g in group_by] if group_by is not None else ["count"] * len(timestamps)
    names: List[str] = []
    for g in groups:
        if g not in names:
            names.append(g)
    counts = {name: {a: 0 for a in axis} for name in names}
    for dt, g in zip(floored, groups):
        counts[g][dt] += 1
    fmt = _BUCKET_FMT[bucket]
    x_labels = [a.strftime(fmt) for a in axis]
    return [{"name": name, "x": x_labels, "y": [counts[name][a] for a in axis]} for name in names]


def _records_to_timestamps_group(
    records: List[Any], time_field: Optional[str], group_field: Optional[str]
) -> tuple:
    """Lets a model pass a list_recent_runs/list_recent_failures-shaped result straight through instead of
    hand-copying two parallel lists -- the exact failure mode this mode exists to remove: a long run history
    (60-100+ entries) transcribed by hand into separate `timestamps`/`group_by` lists reliably drifts out of
    sync (one list a couple of entries longer/shorter than the other) long before a human would notice, and
    `_bucket_series` only ever sees that mismatch after the fact. Deriving both lists from the same `records`
    by index makes a length mismatch between them structurally impossible."""
    if not isinstance(records, list) or not records:
        raise ToolError("records must be a non-empty list of objects")
    if not time_field:
        raise ToolError("time_field is required with records -- the key in each record holding its timestamp")
    timestamps: List[Any] = []
    for i, r in enumerate(records):
        if not isinstance(r, dict):
            raise ToolError(f"records[{i}] must be an object, got {type(r).__name__}")
        if time_field not in r:
            raise ToolError(f"records[{i}] is missing time_field {time_field!r}")
        timestamps.append(r[time_field])
    group_by: Optional[List[Any]] = None
    if group_field:
        group_by = []
        for i, r in enumerate(records):
            if group_field not in r:
                raise ToolError(f"records[{i}] is missing group_field {group_field!r}")
            group_by.append(r[group_field])
    return timestamps, group_by


def _resolve_chart_series(kind, series, labels, values, timestamps, bucket, group_by, records=None, time_field=None, group_field=None):
    modes = []
    if series is not None:
        modes.append("series")
    if records is not None:
        modes.append("records")
    if timestamps is not None:
        modes.append("timestamps")
    if labels is not None or values is not None:
        modes.append("labels_values")
    if not modes:
        raise ToolError("provide one of: series, records (+time_field), timestamps (+bucket), or labels+values")
    if len(modes) > 1:
        raise ToolError(f"provide exactly one data shape, got {modes}")
    mode = modes[0]

    is_time = False
    is_categorical = False

    if mode == "records":
        timestamps, group_by = _records_to_timestamps_group(records, time_field, group_field)
        mode = "timestamps"

    if mode == "timestamps":
        if not bucket:
            raise ToolError("bucket is required with timestamps/records: 'hour', 'day', or 'week'")
        series = _bucket_series(timestamps, bucket, group_by)
        is_time = True
    elif mode == "labels_values":
        if labels is None or values is None:
            raise ToolError("labels and values must both be provided")
        if len(labels) != len(values):
            raise ToolError(f"labels has {len(labels)} entries but values has {len(values)}")
        series = [{"name": None, "x": list(labels), "y": list(values)}]
        is_categorical = True
    else:
        if not isinstance(series, list) or not series:
            raise ToolError("series must be a non-empty list of {name, x, y}")
        for i, s in enumerate(series):
            if not isinstance(s, dict) or "x" not in s or "y" not in s:
                raise ToolError(f"series[{i}] must be a dict with 'x' and 'y'")
            if len(s["x"]) != len(s["y"]):
                raise ToolError(f"series[{i}] ({s.get('name', i)!r}) y has {len(s['y'])} values but x has {len(s['x'])}")
        try:
            for s in series:
                for v in s["x"]:
                    _parse_ts(v)
            is_time = True
        except ToolError:
            is_categorical = True

    resolved_kind = kind
    warnings: List[str] = []
    if resolved_kind == "auto":
        if mode == "timestamps":
            resolved_kind = "line" if len(series) <= 1 else "bar"
        elif is_time:
            resolved_kind = "line"
        elif len(series) > 1:
            resolved_kind = "bar"
        elif is_categorical:
            resolved_kind = "barh"
        else:
            resolved_kind = "line"

    if resolved_kind == "pie":
        n = len(series[0]["x"]) if series else 0
        if n > 5:
            warnings.append(f"pie requested with {n} slices (> 5); falling back to a sorted bar chart instead")
            resolved_kind = "barh"

    if resolved_kind == "barh" and len(series) == 1:
        pairs = sorted(zip(series[0]["x"], series[0]["y"]), key=lambda p: p[1], reverse=True)
        series = [{**series[0], "x": [p[0] for p in pairs], "y": [p[1] for p in pairs]}]

    return resolved_kind, series, warnings, is_time


def _render_chart_png(kind: str, title: str, subtitle: Optional[str], x_label: Optional[str], y_label: Optional[str],
                       unit: Optional[str], series: List[Dict[str, Any]], is_time_x: bool) -> bytes:
    import matplotlib
    matplotlib.use("Agg")  # no display backend needed
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7.5, 4.3), dpi=120)
    y_label_full = f"{y_label} ({unit})" if y_label and unit else (y_label or unit or "")

    if kind in ("line", "area", "scatter"):
        for i, s in enumerate(series):
            color = _PALETTE[i % len(_PALETTE)]
            xs = [_parse_ts(v) for v in s["x"]] if is_time_x else s["x"]
            if kind == "line":
                ax.plot(xs, s["y"], label=s.get("name"), color=color, marker="o" if len(xs) <= 30 else None)
            elif kind == "area":
                ax.fill_between(xs, s["y"], alpha=0.35, color=color)
                ax.plot(xs, s["y"], color=color, label=s.get("name"))
            else:
                ax.scatter(xs, s["y"], label=s.get("name"), color=color)
        if is_time_x:
            fig.autofmt_xdate()
            ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
        ax.set_xlabel(x_label or "")
        ax.set_ylabel(y_label_full)
        ax.grid(axis="y", alpha=0.3)
        if len(series) > 1:
            ax.legend()
    elif kind in ("bar", "barh"):
        categories = series[0]["x"]
        n_cat = len(categories)
        positions = list(range(n_cat))
        bottoms = [0.0] * n_cat
        for i, s in enumerate(series):
            color = _PALETTE[i % len(_PALETTE)]
            if kind == "barh":
                ax.barh(positions, s["y"], left=bottoms, color=color, label=s.get("name"))
            else:
                ax.bar(positions, s["y"], bottom=bottoms, color=color, label=s.get("name"), width=0.8)
            bottoms = [b + v for b, v in zip(bottoms, s["y"])]
        if kind == "barh":
            ax.set_yticks(positions)
            ax.set_yticklabels(categories)
            ax.invert_yaxis()
            ax.set_xlabel(y_label_full or x_label or "")
            if n_cat <= 12 and len(series) == 1:
                for pos, v in zip(positions, series[0]["y"]):
                    ax.text(v, pos, f" {v}", va="center")
            ax.grid(axis="x", alpha=0.3)
        else:
            ax.set_xticks(positions)
            ax.set_xticklabels(categories, rotation=45 if n_cat > 8 else 0, ha="right" if n_cat > 8 else "center")
            ax.set_xlabel(x_label or "")
            ax.set_ylabel(y_label_full)
            if n_cat <= 12 and len(series) == 1:
                for pos, v in zip(positions, series[0]["y"]):
                    ax.text(pos, v, str(v), ha="center", va="bottom")
            ax.grid(axis="y", alpha=0.3)
        if len(series) > 1:
            ax.legend()
    elif kind == "pie":
        ax.pie(series[0]["y"], labels=series[0]["x"], colors=_PALETTE, autopct="%1.0f%%")
        ax.axis("equal")
    else:
        raise ToolError(f"unknown chart kind {kind!r}")

    full_title = title or "Chart"
    if subtitle:
        fig.suptitle(full_title, fontsize=13, fontweight="bold")
        ax.set_title(subtitle, fontsize=10, color="#555555", loc="left")
    else:
        ax.set_title(full_title, fontsize=13, fontweight="bold")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


# ── document shaping (shared by make_pdf / make_docx) ────────────────────────────────────────────────────────────
def _validate_sections(sections: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if sections is None:
        return []
    if not isinstance(sections, list):
        raise ToolError("sections must be a list of {heading, text, bullets, table, chart_file_id}")
    for i, sec in enumerate(sections):
        if not isinstance(sec, dict):
            raise ToolError(f"sections[{i}] must be a dict")
    return sections


def _validate_sheets(sheets: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if not sheets or not isinstance(sheets, list):
        raise ToolError("sheets must be a non-empty list of {name, columns, rows}")
    for i, sheet in enumerate(sheets):
        if not isinstance(sheet, dict) or "columns" not in sheet:
            raise ToolError(f"sheets[{i}] must be a dict with at least 'columns'")
    return sheets


def _resolve_chart_image_path(files_dir: str, run_id: Optional[str], chart_file_id: str) -> str:
    entry = lookup_report_file(files_dir, run_id, chart_file_id)
    if entry is None:
        raise ToolError(f"chart_file_id {chart_file_id!r} was not found among this run's generated files")
    return entry["path"]


def _render_pdf_bytes(title, subtitle, footer, sections, files_dir, run_id):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import LETTER
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.platypus import (
            Image as RLImage, ListFlowable, ListItem, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
        )
    except ImportError as exc:
        raise ImportError("reportlab is required for make_pdf: pip install reportlab") from exc

    styles = getSampleStyleSheet()
    story = [Paragraph(title or "Report", styles["Title"])]
    if subtitle:
        story.append(Paragraph(subtitle, styles["Normal"]))
    story.append(Spacer(1, 12))
    for sec in sections:
        if sec.get("heading"):
            story.append(Paragraph(str(sec["heading"]), styles["Heading2"]))
        if sec.get("text"):
            story.append(Paragraph(str(sec["text"]), styles["Normal"]))
        if sec.get("bullets"):
            items = [ListItem(Paragraph(str(b), styles["Normal"])) for b in sec["bullets"]]
            story.append(ListFlowable(items, bulletType="bullet"))
        table = sec.get("table")
        if table:
            cols = table.get("columns") or []
            rows = table.get("rows") or []
            data = [[str(c) for c in cols]] + [[str(c) for c in r] for r in rows]
            tbl = Table(data, repeatRows=1)
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
            ]))
            story.append(tbl)
        if sec.get("chart_file_id"):
            img_path = _resolve_chart_image_path(files_dir, run_id, sec["chart_file_id"])
            story.append(RLImage(img_path, width=5.5 * inch, height=3.2 * inch, kind="proportional"))
        story.append(Spacer(1, 10))
    if footer:
        story.append(Spacer(1, 20))
        story.append(Paragraph(str(footer), styles["Normal"]))

    page_count = {"n": 0}

    class _CountingCanvas(rl_canvas.Canvas):
        def showPage(self):
            page_count["n"] += 1
            super().showPage()

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=LETTER)
    doc.build(story, canvasmaker=_CountingCanvas)
    return buf.getvalue(), max(page_count["n"], 1)


def _render_docx_bytes(title, subtitle, footer, sections, files_dir, run_id):
    try:
        from docx import Document
        from docx.shared import Inches
    except ImportError as exc:
        raise ImportError("python-docx is required for make_docx: pip install python-docx") from exc

    doc = Document()
    doc.add_heading(title or "Report", level=0)
    if subtitle:
        doc.add_paragraph(subtitle)
    for sec in sections:
        if sec.get("heading"):
            doc.add_heading(str(sec["heading"]), level=2)
        if sec.get("text"):
            doc.add_paragraph(str(sec["text"]))
        if sec.get("bullets"):
            for b in sec["bullets"]:
                doc.add_paragraph(str(b), style="List Bullet")
        table = sec.get("table")
        if table:
            cols = table.get("columns") or []
            rows = table.get("rows") or []
            if cols:
                tbl = doc.add_table(rows=1, cols=len(cols))
                try:
                    tbl.style = "Light Grid Accent 1"
                except KeyError:
                    pass
                for i, c in enumerate(cols):
                    tbl.rows[0].cells[i].text = str(c)
                for r in rows:
                    cells = tbl.add_row().cells
                    for i, v in enumerate(r):
                        if i < len(cells):
                            cells[i].text = str(v)
        if sec.get("chart_file_id"):
            img_path = _resolve_chart_image_path(files_dir, run_id, sec["chart_file_id"])
            doc.add_picture(img_path, width=Inches(5.5))
    if footer:
        doc.add_paragraph(str(footer))

    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


def _render_xlsx_bytes(sheets):
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except ImportError as exc:
        raise ImportError("openpyxl is required for make_xlsx: pip install openpyxl") from exc

    wb = Workbook()
    default_ws = wb.active
    first = True
    for sheet in sheets:
        name = str(sheet.get("name") or "Sheet")[:31]
        cols = sheet.get("columns") or []
        rows = sheet.get("rows") or []
        ws = default_ws if first else wb.create_sheet(title=name)
        if first:
            ws.title = name
            first = False
        for i, c in enumerate(cols, start=1):
            cell = ws.cell(row=1, column=i, value=c)
            cell.font = Font(bold=True)
        for r_i, row in enumerate(rows, start=2):
            for c_i, v in enumerate(row, start=1):
                ws.cell(row=r_i, column=c_i, value=v)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── the toolset ───────────────────────────────────────────────────────────────────────────────────────────────────
class ReportTools:
    """`call(tool, args)` is the one safe entry point (allow-list, per-run file/size caps, error shaping,
    manifest bookkeeping). The `t_*` methods are the raw implementations."""

    def __init__(
        self,
        files_dir: str,
        *,
        run_id: Optional[str] = None,
        allow: Optional[List[str]] = None,
        max_file_bytes: int = 5_000_000,
        max_files_per_run: int = 20,
        file_retention_hours: float = 24.0,
        audit: Optional[Any] = None,
    ):
        self.files_dir = files_dir
        self.run_id = run_id
        if allow is None:
            allow = list(ALL_TOOLS)
        unknown = sorted(set(allow) - set(ALL_TOOLS))
        if unknown:
            raise ValueError(f"unknown tool(s) in allow: {unknown}; valid: {list(ALL_TOOLS)}")
        self.allow = frozenset(allow)
        self.max_file_bytes = max(1024, int(max_file_bytes))
        self.max_files_per_run = max(1, int(max_files_per_run))
        self.file_retention_hours = float(file_retention_hours)
        self._audit = audit or (lambda line: _AUDIT.info(line))
        try:
            sweep_old_report_runs(self.files_dir, self.file_retention_hours)
        except OSError:  # a sweep failure must never block a tool call
            pass

    def tool_names(self) -> List[str]:
        return [t for t in ALL_TOOLS if t in self.allow]

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(args or {})
        try:
            self._audit(f"report_tools call tool={tool} args_keys={sorted(args.keys())}")
        except Exception:  # an audit sink must never break a call
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except ToolError as e:
            return {"error": str(e)}
        except ImportError as e:
            return {"error": str(e)}
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {e}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {e}"}
        return result

    # -- shared bookkeeping -------------------------------------------------------------------------------------
    def _write_file(self, tool: str, name: str, media_type: str, data: bytes) -> Dict[str, Any]:
        if len(read_run_manifest(self.files_dir, self.run_id)) >= self.max_files_per_run:
            raise ToolError(f"max_files_per_run ({self.max_files_per_run}) already reached for this run")
        if len(data) > self.max_file_bytes:
            raise ToolError(f"generated file is {len(data)} bytes, over the max_file_bytes cap ({self.max_file_bytes})")
        file_id = uuid.uuid4().hex[:12]
        run_dir = _report_run_dir(self.files_dir, self.run_id)
        os.makedirs(run_dir, exist_ok=True)
        path = os.path.join(run_dir, f"{file_id}_{name}")
        with open(path, "wb") as f:
            f.write(data)
        entry = {
            "file_id": file_id, "name": name, "media_type": media_type, "path": path,
            "size_bytes": len(data), "tool": tool,
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        append_to_report_manifest(self.files_dir, self.run_id, entry)
        return entry

    # -- tools ----------------------------------------------------------------------------------------------------
    def t_make_chart(
        self, kind: str = "auto", title: str = "", subtitle: Optional[str] = None, x_label: Optional[str] = None,
        y_label: Optional[str] = None, unit: Optional[str] = None, series: Optional[List[Dict[str, Any]]] = None,
        labels: Optional[List[Any]] = None, values: Optional[List[Any]] = None, timestamps: Optional[List[Any]] = None,
        bucket: Optional[str] = None, group_by: Optional[List[str]] = None,
        records: Optional[List[Dict[str, Any]]] = None, time_field: Optional[str] = None, group_field: Optional[str] = None,
    ) -> Dict[str, Any]:
        if kind not in ("auto", "line", "bar", "barh", "area", "scatter", "pie"):
            raise ToolError(f"kind must be one of auto/line/bar/barh/area/scatter/pie, got {kind!r}")
        resolved_kind, resolved_series, warnings, is_time_x = _resolve_chart_series(
            kind, series, labels, values, timestamps, bucket, group_by,
            records=records, time_field=time_field, group_field=group_field,
        )
        png = _render_chart_png(resolved_kind, title, subtitle, x_label, y_label, unit, resolved_series, is_time_x)
        entry = self._write_file("make_chart", "chart.png", "image/png", png)
        result = {
            "file_id": entry["file_id"], "name": entry["name"], "media_type": entry["media_type"],
            "bytes": entry["size_bytes"], "kind_used": resolved_kind,
        }
        if warnings:
            result["warnings"] = warnings
        return result

    def t_make_pdf(
        self, title: str = "Report", subtitle: Optional[str] = None, footer: Optional[str] = None,
        sections: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        sections = _validate_sections(sections)
        pdf_bytes, pages = _render_pdf_bytes(title, subtitle, footer, sections, self.files_dir, self.run_id)
        entry = self._write_file("make_pdf", "report.pdf", "application/pdf", pdf_bytes)
        return {
            "file_id": entry["file_id"], "name": entry["name"], "media_type": entry["media_type"],
            "bytes": entry["size_bytes"], "pages": pages,
        }

    def t_make_docx(
        self, title: str = "Report", subtitle: Optional[str] = None, footer: Optional[str] = None,
        sections: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        sections = _validate_sections(sections)
        docx_bytes = _render_docx_bytes(title, subtitle, footer, sections, self.files_dir, self.run_id)
        entry = self._write_file(
            "make_docx", "report.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document", docx_bytes,
        )
        return {
            "file_id": entry["file_id"], "name": entry["name"], "media_type": entry["media_type"],
            "bytes": entry["size_bytes"],
        }

    def t_make_xlsx(self, sheets: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        sheets = _validate_sheets(sheets)
        xlsx_bytes = _render_xlsx_bytes(sheets)
        entry = self._write_file(
            "make_xlsx", "report.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", xlsx_bytes,
        )
        return {
            "file_id": entry["file_id"], "name": entry["name"], "media_type": entry["media_type"],
            "bytes": entry["size_bytes"],
        }


def _this_module_name() -> str:
    """The real importable dotted path for the `python -m <this>` stdio server invocation -- same fix/rationale
    as dagster_instance_tools' own `_this_module_name` (duplicated here, not imported, per this repo's
    self-contained-component convention): `__name__` is literally `"__main__"` for both a bare `python
    component.py` and a `python -m dotted.path` invocation, so `__spec__.name` is used instead."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "report_tools_server's mcp_server_resource_key path must be run as `python -m "
        "<dotted.path.to.this.module>` (e.g. `python -m "
        "dagster_community_components.assets.ai.report_tools_server.component`), not `python "
        "component.py` directly. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource (name/type/command/url/env/headers/headers_env) -- inlined here
    rather than imported, byte-identical to dagster_instance_tools' / knowledge_search_server's own copies
    (structurally checked in tests/test_drift.py). `env`'s DAGSTER_HOME (and any live_env_passthrough names) is
    deliberately NOT baked in at construction time -- read fresh from the environment every time model_dump() is
    actually called (run time), since that can differ from code-location load time."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None
    live_env_passthrough: Optional[List[str]] = None

    def model_dump(self, *args, **kwargs):
        dumped = super().model_dump(*args, **kwargs)
        passthrough = dumped.pop("live_env_passthrough", None)
        if dumped.get("type") == "stdio" and passthrough:
            env = dict(dumped.get("env") or {})
            for var_name in passthrough:
                value = os.environ.get(var_name)
                if value:
                    env[var_name] = value
            dumped["env"] = env
        return dumped


# ── Dagster component: registers the toolset as a resource ──────────────────────────────────────────────────────
class ReportToolsServerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers deterministic chart/PDF/Word/Excel file builders as a resource (default key `report_tools`) and,
    when mcp_server_resource_key is set, ALSO as a stdio MCP server any mcp_servers-aware agent can call.

    Use inside assets/ops: `context.resources.report_tools.call("make_chart", {...})`.
    For a chat agent (MCP only) set `mcp_server_resource_key` -- see README.md."""

    resource_key: str = Field(default="report_tools", description="Resource key the toolset is registered under")
    files_dir: str = Field(
        description="Local directory root for generated files, one subfolder per dagster/run_id. Create it with "
                     "write access for whichever process runs the chat agent."
    )
    allow: Optional[List[str]] = Field(
        default=None, description=f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}."
    )
    max_file_bytes: int = Field(default=5_000_000, description="Per-file size cap in bytes. A file over this is rejected, not truncated.")
    max_files_per_run: int = Field(default=20, description="Max files a single run (dagster/run_id) may generate across all tools.")
    file_retention_hours: float = Field(
        default=24.0,
        description="A run's generated files (and its manifest) are deleted this many hours after they were "
                     "written -- swept opportunistically on each tool call, not a scheduled job.",
    )
    run_id_template: Optional[str] = Field(
        default="{run_id}",
        description=(
            "Template for the REPORT_TOOLS_RUN_ID env var set on the stdio MCP server (mcp_server_resource_key "
            "only) -- left as the literal string '{run_id}' here; the CALLING agent's own generic "
            "{run_id}/{tag.*} substitution resolves it to the real run id before the subprocess is spawned (same "
            "mechanism as knowledge_search_server's run_id_template). This is what scopes every generated file "
            "and its manifest to the run that made it, so slack_reply / teams_reply only ever post files from "
            "their OWN run."
        ),
    )
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/env shape as "
            "MCPServerResource) under this key, built from this component's own settings -- python -m this "
            "module, with REPORT_TOOLS_* env vars set from files_dir/allow/max_file_bytes/max_files_per_run/"
            "file_retention_hours/run_id_template. DAGSTER_HOME, DAGSTER_CLOUD_DEPLOYMENT_NAME, and anything "
            "listed in mcp_extra_env_vars are read fresh from the environment at run time, not baked in at "
            "component load. Any mcp_servers-aware component can then reference it via server_resource_key."
        ),
    )
    mcp_extra_env_vars: Optional[List[str]] = Field(
        default=None,
        description="mcp_server_resource_key only. Extra env var NAMES to copy (read fresh every time a consumer "
                    "resolves the resource) from this process into the spawned MCP server's env.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow", "mcp_extra_env_vars"), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Report Tools Server', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(ALL_TOOLS)}")
        files_dir = self.files_dir
        max_bytes, max_files = int(self.max_file_bytes), int(self.max_files_per_run)
        retention = float(self.file_retention_hours)

        def _init(init_context):
            # Always run_id=None in-process -- there is no live Dagster run context this server-facing resource
            # can read a run id from any more reliably than knowledge_search_server's own equivalent field can
            # (see its own run_id Field docstring); real run-scoping only happens through the MCP server path
            # below, via REPORT_TOOLS_RUN_ID resolved by the calling agent.
            return ReportTools(files_dir, run_id=None, allow=allow, max_file_bytes=max_bytes, max_files_per_run=max_files, file_retention_hours=retention)

        res = dg.ResourceDefinition(resource_fn=_init, description="Deterministic chart/PDF/Word/Excel file builders for agents.")
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {
                "REPORT_TOOLS_FILES_DIR": files_dir,
                "REPORT_TOOLS_MAX_FILE_BYTES": str(max_bytes),
                "REPORT_TOOLS_MAX_FILES_PER_RUN": str(max_files),
                "REPORT_TOOLS_RETENTION_HOURS": str(retention),
            }
            if allow:
                mcp_env["REPORT_TOOLS_ALLOW"] = ",".join(allow)
            if self.run_id_template:
                mcp_env["REPORT_TOOLS_RUN_ID"] = self.run_id_template
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key,
                type="stdio",
                command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env,
                live_env_passthrough=["DAGSTER_HOME", "DAGSTER_CLOUD_DEPLOYMENT_NAME", *(self.mcp_extra_env_vars or [])],
            )

        return dg.Definitions(resources=resources)

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""


# ── stdio MCP server entrypoint:  python -m <this module>  ──────────────────────────────────────────────────────
def build_mcp_server(tools: ReportTools):
    """MCP server (official `mcp` package) exposing exactly the allowed tools. Works on mcp 1.x and 2.x."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    try:  # the SDK's own ToolError, not this module's -- see _mcp_call below for why that distinction matters.
        from mcp.server.mcpserver.exceptions import ToolError as _SdkToolError  # mcp 2.x (confirmed live, 2.3.0)
    except ImportError:
        from mcp.server.fastmcp.exceptions import ToolError as _SdkToolError  # mcp 1.x (stable FastMCP API, not live-verified here)

    mcp = _Server("report-tools")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__ in allowed:
            mcp.tool()(fn)
        return fn

    def _mcp_call(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """tools.call() never raises (see module docstring) -- a bad-arguments or render failure comes back as
        a plain {"error": ...} dict, which the MCP transport would otherwise hand back as a NORMAL
        (isError=False) tool result indistinguishable from success. Re-raising here lets the `mcp` package's
        own exception handling (both fastmcp 1.x and mcp.server.mcpserver 2.x catch any exception from a
        registered tool function and turn it into a real isError=True CallToolResult) make the failure visible
        to whatever's counting failed calls in the agent loop. MUST be the SDK's own ToolError (_SdkToolError),
        not this module's: the SDK's tool dispatch (mcp/server/mcpserver/tools/base.py `Tool.run`) only keeps a
        raised exception's own message when it recognizes the exception as ITS OWN ToolError/ResourceError --
        anything else (including a same-named ToolError class defined elsewhere, confirmed live) is treated as
        an unexpected crash and replaced with a generic 'Error executing tool <name>' before it reaches the
        client, which would throw away the whole reason the agent needs to read and act on."""
        result = tools.call(name, args)
        if isinstance(result, dict) and "error" in result:
            raise _SdkToolError(result["error"])
        return result

    @_tool
    def make_chart(
        kind: str = "auto", title: str = "", subtitle: Optional[str] = None, x_label: Optional[str] = None,
        y_label: Optional[str] = None, unit: Optional[str] = None, series: Optional[list] = None,
        labels: Optional[list] = None, values: Optional[list] = None, timestamps: Optional[list] = None,
        bucket: Optional[str] = None, group_by: Optional[list] = None,
        records: Optional[list] = None, time_field: Optional[str] = None, group_field: Optional[str] = None,
    ) -> dict:
        """Use this when the person asks for a chart, graph, or to visualize data. Pass exactly ONE data shape:
        `series` (a list of {name, x, y} -- for line/bar/area/scatter), `labels` + `values` (one ranked/
        categorical series -- also the shape for a pie), `timestamps` (+ `bucket`: 'hour'|'day'|'week',
        optional `group_by`) to have this tool bucket and zero-fill raw event timestamps itself, or -- the
        easiest shape when the data came from another tool call, e.g. list_recent_runs -- `records` (that
        tool's own list of objects, unmodified) + `time_field` (the key in each record holding its timestamp,
        e.g. 'created_at') and optional `group_field` (e.g. 'status') instead of copying those two columns out
        into separate `timestamps`/`group_by` lists by hand. kind='auto' (default) picks a line for a time
        series, a sorted horizontal bar for ranked categories, and a stacked bar for more than one series/
        group. Returns {file_id, name, media_type, bytes, kind_used} (and 'warnings' if a request had to be
        adjusted, e.g. a pie with too many slices)."""
        return _mcp_call("make_chart", dict(
            kind=kind, title=title, subtitle=subtitle, x_label=x_label, y_label=y_label, unit=unit, series=series,
            labels=labels, values=values, timestamps=timestamps, bucket=bucket, group_by=group_by,
            records=records, time_field=time_field, group_field=group_field,
        ))

    @_tool
    def make_pdf(title: str = "Report", subtitle: Optional[str] = None, footer: Optional[str] = None, sections: Optional[list] = None) -> dict:
        """Use this when the person asks for a PDF or a document. `sections` is a list of {heading, text,
        bullets, table: {columns, rows}, chart_file_id} -- pass a chart_file_id returned by an earlier
        make_chart call IN THIS SAME RUN to embed that chart. Returns {file_id, name, media_type, bytes, pages}."""
        return _mcp_call("make_pdf", dict(title=title, subtitle=subtitle, footer=footer, sections=sections))

    @_tool
    def make_docx(title: str = "Report", subtitle: Optional[str] = None, footer: Optional[str] = None, sections: Optional[list] = None) -> dict:
        """Use this when the person asks for a Word document. Same `sections` shape as make_pdf. Returns
        {file_id, name, media_type, bytes}."""
        return _mcp_call("make_docx", dict(title=title, subtitle=subtitle, footer=footer, sections=sections))

    @_tool
    def make_xlsx(sheets: Optional[list] = None) -> dict:
        """Use this when the person asks for a spreadsheet or Excel file. `sheets` is a list of {name, columns,
        rows}. Returns {file_id, name, media_type, bytes}."""
        return _mcp_call("make_xlsx", dict(sheets=sheets))

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: REPORT_TOOLS_FILES_DIR (required), REPORT_TOOLS_ALLOW (csv), _MAX_FILE_BYTES, _MAX_FILES_PER_RUN,
    _RETENTION_HOURS, _RUN_ID (stamped per run by the calling agent's {run_id} substitution)."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    files_dir = os.environ.get("REPORT_TOOLS_FILES_DIR")
    if not files_dir:
        raise SystemExit("REPORT_TOOLS_FILES_DIR must be set")
    allow_env = os.environ.get("REPORT_TOOLS_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None

    tools = ReportTools(
        files_dir,
        run_id=os.environ.get("REPORT_TOOLS_RUN_ID"),
        allow=allow,
        max_file_bytes=int(os.environ.get("REPORT_TOOLS_MAX_FILE_BYTES", "5000000")),
        max_files_per_run=int(os.environ.get("REPORT_TOOLS_MAX_FILES_PER_RUN", "20")),
        file_retention_hours=float(os.environ.get("REPORT_TOOLS_RETENTION_HOURS", "24")),
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
