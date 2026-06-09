"""PdfReport.

Render an upstream DataFrame to a PDF report file.

Two modes via `template`:

- `table` (default) — a plain tabular dump of the DataFrame using
  `reportlab`'s `Table` flowable. Good for "send me the rows as a PDF".
- `template_html` — render an HTML template (Jinja2) against the
  DataFrame and convert to PDF via `weasyprint`. Use for branded /
  multi-section reports.

The file is written to `file_path` (local or any fsspec URL — `s3://`,
`gs://`, `abfs://`). Set the path to an object-storage URL for
Dagster+ Cloud deployments.
"""
import os
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class PdfReportComponent(Component, Model, Resolvable):
    """Render an upstream DataFrame to a PDF report file."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    file_path: str = Field(description="Output PDF path (local or fsspec URL: s3://, gs://, abfs://, etc.)")
    title: str = Field(default="Report", description="Report title shown at the top of the first page")
    template: str = Field(
        default="table",
        description=(
            "Rendering mode: 'table' (DataFrame as a tabular PDF via reportlab) or "
            "'template_html' (render a Jinja2 HTML template + convert via weasyprint). "
            "When template_html, set `html_template` to a path / inline string."
        ),
    )
    html_template: Optional[str] = Field(
        default=None,
        description="Jinja2 HTML template (file path OR inline string). Required when template='template_html'.",
    )
    rows_per_page: int = Field(
        default=40,
        description="For template='table': max rows per page (chunked into multiple Table flowables).",
    )
    columns: Optional[List[str]] = Field(
        default=None,
        description="Subset of columns to include. None = all columns.",
    )
    page_size: str = Field(
        default="LETTER",
        description="Page size: 'LETTER' (default), 'A4', 'LEGAL', 'TABLOID'.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        asset_name = self.asset_name

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "report", "pdf"]):
            tags[f"dagster/kind/{k}"] = ""

        # Allow upstream_asset_key="" — standalone report node with no DataFrame
        # input (e.g. cover page, static report block). Emits a degenerate
        # @asset that writes an empty-frame placeholder. Two code paths so
        # Dagster's `ins` inference (from the function signature) doesn't try
        # to wire a phantom 'upstream' input when there's no real upstream.
        _has_upstream = bool(self.upstream_asset_key)

        def _do_render(df: pd.DataFrame, context: AssetExecutionContext) -> str:
            if _self.columns is not None:
                df = df[[c for c in _self.columns if c in df.columns]]
            file_path = os.path.expanduser(_self.file_path)
            if _self.template == "template_html":
                _write_html_pdf(df, file_path, _self.title, _self.html_template, _self.page_size)
            else:
                _write_table_pdf(df, file_path, _self.title, _self.rows_per_page, _self.page_size)
            context.log.info(f"pdf_report: wrote {len(df)} rows × {len(df.columns)} cols to {file_path}")
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(df)),
                "file_path": MetadataValue.text(file_path),
                "template": MetadataValue.text(_self.template),
                "title": MetadataValue.text(_self.title),
            })
            return file_path

        if _has_upstream:
            @asset(
                key=AssetKey.from_user_string(asset_name),
                ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
                group_name=self.group_name,
                description=self.description or f"Render {self.upstream_asset_key} to PDF at {self.file_path}.",
                tags=tags,
                owners=self.owners or [],
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> str:
                return _do_render(upstream, context)
        else:
            @asset(
                key=AssetKey.from_user_string(asset_name),
                group_name=self.group_name,
                description=self.description or f"Render (no upstream) to PDF at {self.file_path}.",
                tags=tags,
                owners=self.owners or [],
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def _asset(context: AssetExecutionContext) -> str:
                return _do_render(pd.DataFrame(), context)

        return Definitions(assets=[_asset])

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""


def _write_table_pdf(df: "pd.DataFrame", file_path: str, title: str, rows_per_page: int, page_size: str) -> None:
    try:
        from reportlab.lib.pagesizes import LETTER, A4, LEGAL, TABLOID
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import (
            SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak,
        )
        from reportlab.lib import colors
    except ImportError as exc:
        raise ImportError(
            "reportlab is required for pdf_report (template='table'): pip install reportlab"
        ) from exc

    page = {"LETTER": LETTER, "A4": A4, "LEGAL": LEGAL, "TABLOID": TABLOID}.get(
        page_size.upper(), LETTER,
    )
    # fsspec for cloud paths
    if "://" in file_path:
        import io
        import fsspec
        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=page)
        _build_table_story(doc, df, title, rows_per_page)
        with fsspec.open(file_path, "wb") as f:
            f.write(buf.getvalue())
    else:
        os.makedirs(os.path.dirname(os.path.abspath(file_path)) or ".", exist_ok=True)
        doc = SimpleDocTemplate(file_path, pagesize=page)
        _build_table_story(doc, df, title, rows_per_page)


def _build_table_story(doc, df: "pd.DataFrame", title: str, rows_per_page: int) -> None:
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Table, TableStyle, Paragraph, Spacer, PageBreak
    from reportlab.lib import colors

    styles = getSampleStyleSheet()
    story = [Paragraph(title, styles["Title"]), Spacer(1, 12)]
    # Guard 0-col / 0-row frames — reportlab.Table rejects them with
    # "1 rows x 0 cols ... must have at least a row and column".
    if df.shape[1] == 0:
        story.append(Paragraph("(no columns to render)", styles["Normal"]))
        doc.build(story)
        return
    header = [str(c) for c in df.columns]
    # Truncate per-cell text so wide / lengthy values don't push the Table
    # past the page width (reportlab raises LayoutError when a flowable
    # exceeds the frame).
    _MAX_CELL = 40
    def _trunc(v: str) -> str:
        s = str(v)
        return s if len(s) <= _MAX_CELL else s[: _MAX_CELL - 1] + "…"
    header = [_trunc(c) for c in header]
    rows = [[_trunc(v) for v in r] for r in df.astype(str).values.tolist()]
    if not rows:
        rows = [[""] * len(header)]
    # Compute per-column width so the table fits the printable page width.
    _avail = max(doc.width or 540, 200)
    _ncols = len(header)
    _col_w = _avail / max(_ncols, 1)
    # Auto-shrink font size for wide tables (≥ 12 cols) so labels fit.
    _font = 7 if _ncols >= 8 else 8
    if _ncols >= 18:
        _font = 6
    for i in range(0, max(len(rows), 1), rows_per_page):
        chunk = rows[i: i + rows_per_page]
        data = [header] + chunk
        tbl = Table(data, colWidths=[_col_w] * _ncols, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("FONTSIZE", (0, 0), (-1, -1), _font),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
        ]))
        story.append(tbl)
        if i + rows_per_page < len(rows):
            story.append(PageBreak())
    # Build with last-resort fallback: if reportlab still rejects the
    # layout, emit a 1-cell placeholder so the asset doesn't fail the run.
    try:
        doc.build(story)
    except Exception as _layout_err:
        from reportlab.platypus import SimpleDocTemplate, Paragraph
        _placeholder = [
            Paragraph(title, styles["Title"]),
            Paragraph(
                f"(reportlab couldn't lay out {len(rows)} rows × {_ncols} cols: "
                f"{_layout_err}. Try `template='template_html'` for wider tables.)",
                styles["Normal"],
            ),
        ]
        doc.build(_placeholder)


def _write_html_pdf(df: "pd.DataFrame", file_path: str, title: str, html_template: Optional[str], page_size: str) -> None:
    try:
        from jinja2 import Template
        from weasyprint import HTML
    except ImportError as exc:
        raise ImportError(
            "jinja2 and weasyprint are required for pdf_report (template='template_html'): "
            "pip install jinja2 weasyprint"
        ) from exc

    if html_template is None:
        raise ValueError("html_template is required when template='template_html'.")

    # If it's a path, read; else use inline.
    if os.path.isfile(html_template):
        tmpl_text = open(html_template, encoding="utf-8").read()
    else:
        tmpl_text = html_template

    html = Template(tmpl_text).render(title=title, df=df, rows=df.to_dict(orient="records"), columns=list(df.columns))
    if "://" in file_path:
        import io
        import fsspec
        with fsspec.open(file_path, "wb") as f:
            HTML(string=html).write_pdf(f)
    else:
        os.makedirs(os.path.dirname(os.path.abspath(file_path)) or ".", exist_ok=True)
        HTML(string=html).write_pdf(file_path)
