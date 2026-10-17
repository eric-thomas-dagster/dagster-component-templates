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
from typing import Any, Dict, List, Optional, Union

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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
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
    return out
# END field-shape normalization


class PdfReportComponent(Component, Model, Resolvable):
    """Render an upstream DataFrame to a PDF report file."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame")
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
    columns: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Subset of columns to include. None = all columns.",
    )
    page_size: str = Field(
        default="LETTER",
        description="Page size: 'LETTER' (default), 'A4', 'LEGAL', 'TABLOID'.",
    )

    group_name: Optional[str] = Field(default=None)

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
    )
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim
        if partitions_def is not None and not self.upstream_asset_key:
            raise ValueError("partition_type needs upstream_asset_key: without an upstream frame there are no rows to partition.")

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
            @asset(partitions_def=partitions_def, 
                key=AssetKey.from_user_string(asset_name),
                ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
                group_name=self.group_name,
                description=self.description or f"Render {self.upstream_asset_key} to PDF at {self.file_path}.",
                tags=tags,
                owners=self.owners or [],
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def _asset(context: AssetExecutionContext, upstream: Any) -> str:
                # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
                # Tolerates upstream authors who annotate `-> Output` or
                # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
                if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                    upstream = upstream.value
                # partition bridge dict-concat: when an unpartitioned
                # asset consumes a partitioned upstream, Dagster's IO
                # manager loads ALL partitions as a dict; concat to
                # a single DataFrame before any DataFrame ops.
                if isinstance(upstream, dict):
                    _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                    upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
                # Filter to current partition if partitioned
                if context.has_partition_key or context.has_partition_key_range:
                    # One filter pass per partition key: a normal tick gives exactly one,
                    # a single_run backfill gives the whole range. Rows are selected by
                    # position so keys that match overlapping rows never duplicate them.
                    _pk_base = upstream
                    _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                    _pk_keep = set()
                    _pk_filtered = False
                    for _pk_iter in context.partition_keys:
                        upstream = _pk_pos
                        _pk = _pk_iter
                        _is_multi = hasattr(_pk, "keys_by_dimension")
                        _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                        _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                        if partition_date_column and partition_date_column in upstream.columns and _date_key:
                            _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                            upstream = upstream[_col_dates == _date_key]
                        if partition_static_column and partition_static_column in upstream.columns and _static_key:
                            upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                        elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                            upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                        if upstream is not _pk_pos:
                            _pk_filtered = True
                            _pk_keep.update(upstream.index)
                    upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
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


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
# END scaffold partitions-def builder
