"""Document Word Boxes Component.

Extract WORD-LEVEL bounding boxes from a column of PDF file paths or PDF
bytes using `pdfplumber`: for every word on every page, the exact text plus
its position on the page (`x0`, `x1`, `top`, `bottom`), suitable for review
overlays, field-level QA, redaction previews, and citation interfaces --
anything that needs to point at exactly where a specific word sits on a
rendered page.

This is NOT the same thing as `document_layout_analyzer`, which returns a
REGION-level bounding box (table/figure/header) via a HuggingFace
object-detection model, with a real confidence score because that model is
genuinely probabilistic. This component works at the opposite granularity --
individual words, not regions -- and pulls them from the PDF's own embedded
text layer via `pdfplumber`'s `page.extract_words()`. That means:

- **No model, no LLM call, no guessing.** The coordinates come straight from
  the PDF's own internal text-positioning data. `x0`/`x1`/`top`/`bottom` are
  pdfplumber's own field names, used as-is here (not renamed, not
  reinterpreted).
- **No real confidence score.** A born-digital PDF's text layer is exact --
  there is no OCR uncertainty to express. The `confidence` column is a fixed
  `1.0` for parity with field-shape conventions elsewhere in this repo, NOT
  a real per-word probability. Treat it as "this word is exactly what the
  PDF says it is," not as a model's confidence estimate.
- **Text-layer PDFs only.** This only works for born-digital / text-layer
  PDFs (the overwhelming majority of generated reports, invoices, filings,
  exports, etc.). A scanned/photographed PDF with no embedded text layer
  will return zero words per page, because `pdfplumber` has nothing to read
  -- there is no text to find coordinates for. This component detects that
  case (zero words extracted across every processed page of a document) and
  SKIPS that document with a clear logged warning and a
  `documents_skipped_no_text_layer` metadata counter, rather than silently
  returning an empty result with no explanation.
- **True OCR of scanned documents is NOT built here.** Producing word-level
  boxes for a scanned/photographed page requires running actual OCR (e.g.
  `pytesseract`'s `image_to_data`, which does return real per-word
  confidence scores because OCR genuinely is probabilistic) -- that is a
  separate, larger piece of future scope. This component does not fake or
  stub that path; it only ever reports what `pdfplumber` can read from a
  real text layer.

One output row is produced PER WORD (not per document, not per page) --
explode, not aggregate. For a large document, scope extraction to specific
pages with `page_range` to avoid exploding to millions of rows when only a
few pages are relevant.

## `find_text_bbox` helper

A small, standalone, read-only helper function is included at module level:
`find_text_bbox(words, needle)`. Given a list of word-box dicts (the same
shape this component's output rows have) and a text snippet, it finds the
contiguous run of words whose concatenated text contains `needle` (a simple
case-insensitive, whitespace-normalized substring match over consecutive
words -- NOT fuzzy or embedding-based matching) and returns a single combined
bounding box spanning those words, or `None` if no match is found. It is
intentionally a plain function with no component/asset dependencies, so
other code (in this file or copied elsewhere, per this repo's self-contained-
component convention) can call it directly.
"""

from typing import Any, Dict, List, Optional, Union, Literal
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    BackfillPolicy,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")



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


def find_text_bbox(words: List[dict], needle: str) -> Optional[dict]:
    """Find the contiguous run of consecutive words whose concatenated text
    contains `needle`, and return a single bounding box spanning them.

    This is a SIMPLE HEURISTIC: an exact (case-insensitive, whitespace-
    normalized) substring match over the words' text joined with single
    spaces -- it is NOT fuzzy matching, NOT embedding-based similarity, and
    it does not tolerate typos or paraphrasing. Joining with a single space
    regardless of each word's original line/position means a needle that
    happens to span a line break (or a page's visual wrapping) in the source
    PDF is still found, since word order -- not original line layout -- is
    what's being matched against.

    `words` is a list of dicts shaped like this component's output rows (at
    minimum: `text`, `x0`, `x1`, `top`, `bottom`, and either `page_number` or
    `page`). Returns a dict `{"x0": min, "x1": max, "top": min, "bottom":
    max, "page": <page of the first matched word>}` spanning every word in
    the first matching run found, or `None` if `needle` isn't found anywhere
    in the concatenated text.

    Caveat: if `words` mixes words from more than one page, a match that
    happens to straddle a page boundary will combine coordinates across two
    different pages into one (physically meaningless) box, and the reported
    `page` is only the first matched word's page. Callers that care about
    this should pass a single page's words at a time.
    """
    if not words or not needle or not str(needle).strip():
        return None
    import re

    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", str(s).strip().lower())

    needle_norm = _norm(needle)
    if not needle_norm:
        return None

    # Keep only words with real text, preserving a pointer back to the
    # original word dict so bbox fields can be read off it later.
    usable = [(w, _norm(w.get("text", ""))) for w in words]
    usable = [(w, wn) for w, wn in usable if wn]
    if not usable:
        return None

    # Build the full concatenated (normalized) text once, tracking each
    # word's [start, end) character span within it, so a substring match's
    # offsets can be mapped back to the words that produced them.
    spans: List[tuple] = []
    concat_parts: List[str] = []
    pos = 0
    for _, wn in usable:
        start = pos
        end = pos + len(wn)
        spans.append((start, end))
        concat_parts.append(wn)
        pos = end + 1  # +1 for the single joining space
    concat_norm = " ".join(concat_parts)

    idx = concat_norm.find(needle_norm)
    if idx == -1:
        return None
    match_end = idx + len(needle_norm)

    covered = [usable[i][0] for i, (s, e) in enumerate(spans) if e > idx and s < match_end]
    if not covered:
        return None

    return {
        "x0": min(w["x0"] for w in covered),
        "x1": max(w["x1"] for w in covered),
        "top": min(w["top"] for w in covered),
        "bottom": max(w["bottom"] for w in covered),
        "page": covered[0].get("page_number", covered[0].get("page")),
    }


class DocumentWordBoxesComponent(Component, Model, Resolvable):
    """Extract word-level bounding boxes from a column of PDF file paths or PDF bytes using pdfplumber.

    NOT a region-level detector -- see `document_layout_analyzer` for that (a
    HuggingFace object-detection model returning a table/figure/header
    region + confidence score, with no per-word granularity at all). This
    component uses pdfplumber's `page.extract_words()`, which derives real
    per-word text and position (`x0`, `x1`, `top`, `bottom`) from the PDF's
    own embedded text layer -- no model, no LLM call, no guessing, and (since
    a text layer is exact, not probabilistic) no real confidence score either
    -- `confidence` is a fixed `1.0` column, included only for shape parity,
    never a genuine per-word probability.

    Only works for born-digital / text-layer PDFs. A scanned/photographed
    PDF with no embedded text layer returns zero words per page; this
    component detects that (zero words across every processed page of a
    document) and skips the document with a logged warning rather than
    silently returning an empty, unexplained result. True OCR of scanned
    documents (e.g. via `pytesseract`'s `image_to_data`) is NOT built here --
    that is separate, larger future scope.

    One output row is produced PER WORD (not per document, not per page).
    Use `page_range` to scope extraction to specific pages of a large
    document and avoid exploding to millions of rows.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (the upstream "
            "DataFrame is filtered once per partition key in the range and the "
            "matching rows are combined). 'multi_run' launches one run per "
            "partition (optionally capped via backfill_policy_max_partitions_per_run). "
            "Leave unset for Dagster's default (one run per partition, uncapped). "
            "Ignored when partition_type/partition_dimensions is not set. "
            "'single_run' requires a partition-range-aware IO manager -- a "
            "generic filesystem/default IO manager cannot persist a single_run "
            "output spanning multiple partitions and will raise a clear error at "
            "materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 5 rows "
            "as a markdown table). Used by builder UIs to render asset shape "
            "without warehouse access."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )

    column: Union[str, int] = Field(description="Column containing PDF file paths or bytes")

    passthrough_columns: Optional[List[str]] = Field(
        default=None,
        description="Upstream columns to carry through to each output word row (one copy per word). Defaults to every upstream column except `column`.",
    )

    page_range: Optional[str] = Field(
        default=None,
        description=(
            "Comma-separated page numbers or ranges like '1,3,5-10' to scope extraction to "
            "specific pages, avoiding exploding to millions of rows for a large document when "
            "only a few pages matter. Default is all pages."
        ),
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps', 'passthrough_columns'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Document Word Boxes', editable=True)

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        # Standard catalog fields — phase 2 wiring
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        column = self.column
        passthrough_columns = self.passthrough_columns
        page_range = self.page_range

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "document_word_boxes"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @asset(description=self.description,
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
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

            try:
                import pdfplumber
            except ImportError:
                raise ImportError("Install pdfplumber: pip install pdfplumber")

            import io

            if column not in upstream.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame.")

            _passthrough_cols = list(passthrough_columns) if passthrough_columns else [c for c in upstream.columns if c != column]
            _missing_passthrough = [c for c in _passthrough_cols if c not in upstream.columns]
            if _missing_passthrough:
                raise ValueError(f"passthrough_columns not found in DataFrame: {_missing_passthrough}")

            def parse_page_spec(spec: Optional[str]) -> Optional[list]:
                if spec is None:
                    return None
                page_nums = []
                for part in spec.split(","):
                    part = part.strip()
                    if "-" in part:
                        start, end = part.split("-", 1)
                        page_nums.extend(range(int(start), int(end) + 1))
                    else:
                        page_nums.append(int(part))
                # pdfplumber uses 0-based index
                return [p - 1 for p in page_nums]

            target_pages = parse_page_spec(page_range)

            records: List[Dict[str, Any]] = []
            documents_processed = 0
            documents_skipped_no_text_layer = 0
            documents_failed = 0

            for _, src_row in upstream.iterrows():
                value = src_row[column]
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    continue
                try:
                    if isinstance(value, bytes):
                        pdf_source = io.BytesIO(value)
                    else:
                        pdf_source = str(value)

                    with pdfplumber.open(pdf_source) as pdf:
                        all_pages = pdf.pages
                        if target_pages is not None:
                            selected = [
                                (i + 1, all_pages[i]) for i in target_pages if i < len(all_pages)
                            ]
                        else:
                            selected = [(idx + 1, page) for idx, page in enumerate(all_pages)]

                        doc_word_count = 0
                        for page_number, page in selected:
                            try:
                                words = page.extract_words()
                            except Exception as e:
                                context.log.warning(f"extract_words failed on page {page_number}: {e}")
                                continue
                            for word_index, w in enumerate(words):
                                record: Dict[str, Any] = {c: src_row[c] for c in _passthrough_cols}
                                record["page_number"] = page_number
                                record["word_index"] = word_index
                                record["text"] = w.get("text")
                                record["x0"] = w.get("x0")
                                record["x1"] = w.get("x1")
                                record["top"] = w.get("top")
                                record["bottom"] = w.get("bottom")
                                # No real confidence score for text-layer PDF word
                                # extraction (pdfplumber doesn't produce one -- a
                                # born-digital text layer is exact, not probabilistic).
                                # Fixed 1.0 is included only for shape parity.
                                record["confidence"] = 1.0
                                records.append(record)
                                doc_word_count += 1

                        if doc_word_count == 0:
                            documents_skipped_no_text_layer += 1
                            context.log.warning(
                                f"No extractable text layer found (0 words across "
                                f"{len(selected)} processed page(s)) -- likely a "
                                "scanned/photographed PDF with no embedded text layer. "
                                "Skipping this document. True OCR of scanned documents "
                                "is not built in this component; see the README."
                            )
                        else:
                            documents_processed += 1
                except Exception as e:
                    documents_failed += 1
                    context.log.warning(f"Failed to extract word boxes from PDF: {e}")
                    continue

            _expected_cols = list(_passthrough_cols) + [
                "page_number", "word_index", "text", "x0", "x1", "top", "bottom", "confidence",
            ]
            df = pd.DataFrame(records, columns=_expected_cols) if records else pd.DataFrame(columns=_expected_cols)

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "documents_processed": MetadataValue.int(documents_processed),
                "documents_skipped_no_text_layer": MetadataValue.int(documents_skipped_no_text_layer),
                "documents_failed": MetadataValue.int(documents_failed),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col.name: [col.name] for col in _col_schema.columns
                        if col.name in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
