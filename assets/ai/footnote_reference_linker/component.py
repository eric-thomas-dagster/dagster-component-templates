"""Footnote Reference Linker Component.

Detects footnotes on each page of a PDF -- a marker (a number, symbol such as
*, †, ‡, or a bracketed number like [12]) plus its explanatory footnote text --
and links each one back to the exact short snippet of BODY text it follows,
so a downstream reviewer (or a UI) can jump from body text to its footnote.
Useful for academic papers, financial reports, legal agreements, and
diligence materials, where a footnote can change the meaning of the body
text it attaches to.

Mechanism: page text is extracted via `pdfplumber` (same library used by
`pdf_text_extractor` and `structured_document_extractor`'s own PDF path), then
ONE `litellm` call PER PAGE asks the model to identify every footnote on that
page and the exact verbatim body-text snippet its marker appears immediately
after. One call per page (rather than per document, or a multi-page window)
keeps `page_number` exact for the per-page bounding-box lookup below, at the
cost of one LLM call per page instead of per document -- a reasonable
trade-off at the per-page granularity this component is read for.

Honesty note: footnote detection from raw extracted text alone is NOT
reliably deterministic -- there is no visual/layout signal available here
(font size, vertical position on the page, superscript rendering) the way a
human reviewer or a layout-aware OCR model would see it. The model is asked
to use textual conventions only (a short marker followed by explanatory
text, typically near the bottom of the page) and nothing guarantees a given
page's footnotes are found, or found completely; a page with no footnotes
should return an empty list rather than one being invented.

Locating the body reference: once the model names a `body_reference_snippet`,
this component calls pdfplumber's own `page.extract_words()` directly (the
same per-word text/x0/x1/top/bottom shape used elsewhere in this repo for
word-level geometry) and matches the snippet against a CONTIGUOUS run of
words -- case-insensitive, whitespace-normalized, exact/near-exact substring
match only, no fuzzy/embedding matching. When the snippet doesn't match any
contiguous run of words closely enough (the model paraphrased it, or split
it across a line/column break pdfplumber's word order doesn't follow), the
row is still emitted -- marker and footnote_text are still useful without a
bounding box -- with `body_reference_bbox` left `None`. A bbox is never
fabricated for a failed match.
"""

import json
import re
from typing import Any, Dict, List, Literal, Optional, Union

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


# -- footnote detection / body-reference linking ---------------------------------------------------------------
# Module-level (not buried in the asset) so anything that wants to preview a detection sends the SAME
# prompt production will -- see structured_document_extractor's build_extraction_messages /
# parse_extraction_response and document_section_splitter's build_section_classification_messages for
# the precedent this follows.


def build_footnote_detection_messages(
    page_text: str,
    *,
    max_content_chars: Optional[int] = None,
) -> List[dict]:
    """The exact chat messages sent to the model to detect footnotes on ONE page.

    See the module docstring's honesty note: there is no layout/visual signal in plain extracted
    text, so the model is asked to rely on textual conventions only (a short marker followed by
    explanatory text, typically near the bottom of the page, whose SAME marker also appears earlier
    in the body).
    """
    content = page_text or ""
    if max_content_chars is not None and len(content) > max_content_chars:
        content = content[:max_content_chars]
    instructions = (
        "The following is the full extracted text of ONE page of a document. Identify every "
        "footnote on this page: a short marker (a number, a symbol such as *, †, ‡, or a "
        "bracketed number like [12]) followed by its explanatory footnote text -- typically near "
        "the bottom of the page -- and the exact short verbatim snippet of BODY text (not the "
        "footnote text itself) that the SAME marker appears immediately after, so the marker's "
        "location in the body can be found later. Only report footnotes that are genuinely present "
        "on this page; never invent one. If a marker's body-text location can't be identified with "
        "confidence, omit that footnote entirely rather than guessing at a snippet. "
        'Return ONLY a JSON array of objects, each shaped exactly like {"marker": "<the marker '
        'text, e.g. \'3\' or \'*\'>", "footnote_text": "<the footnote\'s own explanatory text>", '
        '"body_reference_snippet": "<a short, exact, verbatim quote from the BODY text ending at or '
        'immediately after the marker>"}. Return an empty JSON array if the page has no footnotes. '
        "No prose, no markdown outside the JSON array itself."
    )
    return [{"role": "user", "content": f"{instructions}\n\nPage text:\n{content}"}]


def parse_footnote_detection_response(raw: str) -> List[Dict[str, str]]:
    """Parse the model's reply into a list of {"marker", "footnote_text", "body_reference_snippet"}
    dicts. Raises a clear error on unparseable/malformed JSON -- never silently returns garbage, and
    never fabricates a footnote the model's reply didn't actually contain."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    parsed = json.loads(raw)
    if not isinstance(parsed, list):
        raise ValueError(f"LLM returned {type(parsed).__name__}, expected a JSON array")
    required = {"marker", "footnote_text", "body_reference_snippet"}
    out: List[Dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict) or not required.issubset(item):
            raise ValueError(
                f"LLM returned a malformed entry (expected marker/footnote_text/body_reference_snippet): {item!r}"
            )
        out.append({
            "marker": str(item["marker"]),
            "footnote_text": str(item["footnote_text"]),
            "body_reference_snippet": str(item["body_reference_snippet"]),
        })
    return out


def detect_page_footnotes(
    page_text: str,
    *,
    model: str,
    api_key: Optional[str] = None,
    llm_max_retries: int = 2,
    max_content_chars: Optional[int] = 20000,
) -> "tuple[List[Dict[str, str]], str]":
    """Detect footnotes on ONE page's text. Returns (footnote entries, raw model reply); raises on
    any failure (including an unparseable reply -- never returns garbage silently)."""
    try:
        from litellm import completion
    except ImportError:
        raise ImportError("litellm required: pip install litellm")

    messages = build_footnote_detection_messages(page_text, max_content_chars=max_content_chars)
    resp = completion(model=model, messages=messages, api_key=api_key, num_retries=llm_max_retries)
    raw = resp.choices[0].message.content
    return parse_footnote_detection_response(raw), raw


def _normalize_snippet_text(s: str) -> str:
    """Lowercase + collapse whitespace + fold common curly-quote/dash variants to their straight
    equivalents -- the 'near-exact' half of the matching rule described in the module docstring.
    What follows this is still an exact substring match, never fuzzy/embedding matching."""
    if not s:
        return ""
    s = (
        s.replace("‘", "'").replace("’", "'")
        .replace("“", '"').replace("”", '"')
        .replace("–", "-").replace("—", "-")
    )
    return re.sub(r"\s+", " ", s).strip().lower()


def find_snippet_bbox(words: List[Dict[str, Any]], snippet: str) -> Optional[Dict[str, float]]:
    """Find the bounding box of `snippet` as a CONTIGUOUS run of `words` (pdfplumber's own
    `page.extract_words()` output: dicts with text/x0/x1/top/bottom). Case-insensitive,
    whitespace-normalized, exact/near-exact substring match only -- no fuzzy/embedding matching.

    Returns None when the snippet can't be located this way (the model paraphrased it, or it spans
    a line/column break pdfplumber's word order doesn't follow). Callers must never fabricate a
    bbox in that case -- the row is still emitted with `body_reference_bbox=None`.
    """
    norm_snippet = _normalize_snippet_text(snippet)
    if not norm_snippet or not words:
        return None

    joined_parts: List[str] = []
    offsets: List["tuple[int, int]"] = []
    pos = 0
    for w in words:
        token = _normalize_snippet_text(w.get("text", ""))
        start = pos
        joined_parts.append(token)
        pos += len(token)
        offsets.append((start, pos))
        joined_parts.append(" ")
        pos += 1
    joined = "".join(joined_parts)

    idx = joined.find(norm_snippet)
    if idx == -1:
        return None
    end_idx = idx + len(norm_snippet)

    start_word = next((i for i, (s, e) in enumerate(offsets) if e > idx), None)
    matching_words = [i for i, (s, _e) in enumerate(offsets) if s < end_idx]
    end_word = matching_words[-1] if matching_words else None
    if start_word is None or end_word is None or end_word < start_word:
        return None

    run = words[start_word:end_word + 1]
    return {
        "x0": min(float(w["x0"]) for w in run),
        "x1": max(float(w["x1"]) for w in run),
        "top": min(float(w["top"]) for w in run),
        "bottom": max(float(w["bottom"]) for w in run),
    }


class FootnoteReferenceLinkerComponent(Component, Model, Resolvable):
    """Detect footnotes on each page of a PDF and link each one back to the body-text snippet its
    marker appears after -- so a reviewer or downstream UI can jump from body text to its footnote.

    Not a deterministic parser: footnote detection runs off plain extracted text with no layout
    signal (font size, vertical position, superscript rendering), so it's model-dependent, not
    guaranteed complete or correct. The body-reference bounding box is a best-effort, exact-substring
    match against pdfplumber's word geometry -- when it fails to match, the footnote is still
    emitted with `body_reference_bbox=None` rather than being dropped.
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
    column: Union[str, int] = Field(description="Column containing PDF file paths or PDF bytes to scan for footnotes")
    passthrough_columns: Optional[List[str]] = Field(
        default=None,
        description="Upstream columns to carry onto every output footnote row. Defaults to every upstream column except `column`.",
    )
    model: str = Field(
        default="gpt-4o-mini",
        description=(
            "LiteLLM model string for the per-page footnote-detection call. A cheap model is a "
            "reasonable default for this kind of page-level detection; it is not required to be any "
            "specific model, and detection quality is not guaranteed regardless of model choice."
        ),
    )
    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Environment variable holding the LLM provider API key.",
    )
    llm_max_retries: int = Field(
        default=2,
        description="Max retries forwarded to litellm's num_retries for the per-page footnote-detection call.",
    )
    max_content_chars: int = Field(
        default=20000,
        description=(
            "Maximum characters of a single page's extracted text sent to the model. A page whose "
            "text exceeds this is truncated -- footnotes past the cutoff may then be missed; this is "
            "a known, documented limitation, not a silent correctness claim."
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
        return coerce_field_shapes(
            data,
            lists=('owners', 'kinds', 'deps', 'passthrough_columns'),
            csv=('partition_values',),
            tags=('asset_tags',),
        )

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Footnote Reference Linker', editable=True)

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        column = self.column
        passthrough_columns = self.passthrough_columns
        model = self.model
        api_key_env_var = self.api_key_env_var
        llm_max_retries = self.llm_max_retries
        max_content_chars = self.max_content_chars

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset,
            timezone=self.partition_timezone,
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "footnote_reference_linker"  # component directory name
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
        column_lineage = self.column_lineage

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @asset(
            description=self.description,
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
            # Defensive Output/MaterializeResult unwrap — see pdf_text_extractor for the rationale.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned asset consumes a partitioned
            # upstream, Dagster's IO manager loads ALL partitions as a dict; concat to a single
            # DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            # Filter to current partition if partitioned
            if context.has_partition_key or context.has_partition_key_range:
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
            try:
                import litellm  # noqa: F401 -- checked eagerly so a missing dep fails before any per-row work
            except ImportError:
                raise ImportError("litellm required: pip install litellm")

            import io

            if column not in upstream.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame.")

            _passthrough_cols = list(passthrough_columns) if passthrough_columns else [c for c in upstream.columns if c != column]
            _missing_passthrough = [c for c in _passthrough_cols if c not in upstream.columns]
            if _missing_passthrough:
                raise ValueError(f"passthrough_columns not found in DataFrame: {_missing_passthrough}")

            import os
            api_key = os.environ.get(api_key_env_var)
            _output_cols = _passthrough_cols + [
                "page_number", "marker", "footnote_text", "body_reference_snippet", "body_reference_bbox",
            ]

            records: List[Dict[str, Any]] = []
            documents_processed = 0
            document_read_failures = 0
            pages_processed = 0
            llm_call_failures = 0
            footnotes_found = 0
            bbox_matched = 0
            bbox_unmatched = 0

            for idx, row in upstream.iterrows():
                value = row[column]
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    context.log.warning(f"Skipping empty PDF value at index {idx}")
                    continue
                try:
                    pdf_source = io.BytesIO(value) if isinstance(value, bytes) else str(value)
                    with pdfplumber.open(pdf_source) as pdf:
                        pages = list(pdf.pages)
                        for page_number, page in enumerate(pages, start=1):
                            page_text = page.extract_text() or ""
                            if not page_text.strip():
                                continue
                            try:
                                footnotes, _raw = detect_page_footnotes(
                                    page_text, model=model, api_key=api_key,
                                    llm_max_retries=llm_max_retries, max_content_chars=max_content_chars,
                                )
                            except Exception as e:
                                context.log.warning(
                                    f"Footnote detection failed at index {idx}, page {page_number}: {e}"
                                )
                                llm_call_failures += 1
                                continue
                            pages_processed += 1
                            if not footnotes:
                                continue
                            words = page.extract_words()
                            for item in footnotes:
                                bbox = find_snippet_bbox(words, item["body_reference_snippet"])
                                if bbox is not None:
                                    bbox_matched += 1
                                else:
                                    bbox_unmatched += 1
                                record = {c: row[c] for c in _passthrough_cols}
                                record["page_number"] = page_number
                                record["marker"] = item["marker"]
                                record["footnote_text"] = item["footnote_text"]
                                record["body_reference_snippet"] = item["body_reference_snippet"]
                                record["body_reference_bbox"] = json.dumps(bbox) if bbox is not None else None
                                records.append(record)
                                footnotes_found += 1
                except Exception as e:
                    context.log.warning(f"Failed to read PDF at index {idx}: {e}")
                    document_read_failures += 1
                    continue
                documents_processed += 1

                if idx and idx % 100 == 0:
                    context.log.info(f"Processed {idx} documents")

            df = pd.DataFrame.from_records(records, columns=_output_cols) if records else pd.DataFrame(columns=_output_cols)

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
                "document_read_failures": MetadataValue.int(document_read_failures),
                "pages_processed": MetadataValue.int(pages_processed),
                "llm_call_failures": MetadataValue.int(llm_call_failures),
                "footnotes_found": MetadataValue.int(footnotes_found),
                "body_reference_bbox_matched": MetadataValue.int(bbox_matched),
                "body_reference_bbox_unmatched": MetadataValue.int(bbox_unmatched),
            }
            # Use explicit lineage, or auto-infer: passthrough columns map 1:1, derived columns
            # (page_number/marker/footnote_text/body_reference_snippet/body_reference_bbox) map to `column`.
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _derived_from_pdf = {
                        "page_number", "marker", "footnote_text", "body_reference_snippet", "body_reference_bbox",
                    }
                    _effective_lineage = {}
                    for col in _col_schema.columns:
                        if col.name in _upstream_cols:
                            _effective_lineage[col.name] = [col.name]
                        elif col.name in _derived_from_pdf:
                            _effective_lineage[col.name] = [str(column)]
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
