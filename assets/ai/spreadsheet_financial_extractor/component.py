"""Spreadsheet Financial Extractor Component.

Extract REAL cell-level spreadsheet structure -- sheets, rows, cells, raw
values, formulas, and per-cell formatting -- directly from `.xlsx`/`.xlsm`
workbooks using `openpyxl`. NOT the same thing as `document_ingestion`'s
`markitdown`-based xlsx handling, which only flattens an entire workbook into
one generic markdown text blob: no hidden-sheet control, no raw-vs-display
value choice, no formulas, no per-cell formatting. This component opens the
workbook directly with `openpyxl` instead of going through a generic
document-to-markdown converter, so it can expose the real cell-level knobs
openpyxl itself provides.

## `use_raw_values` / `data_only`

openpyxl's own real, documented `load_workbook(data_only=...)` argument IS
the raw-value-vs-formula-text knob -- wired straight through, not
reimplemented:

- `data_only=True` -- a formula cell's `.value` returns its last-CACHED
  value (whatever a real spreadsheet application computed and saved into
  the file). openpyxl never evaluates formulas itself, so a workbook that
  was never opened and saved by a real spreadsheet app (e.g. one built
  purely via openpyxl's own API without `openpyxl.cell.formula` caching)
  has no cached value to return, and `.value` comes back `None`.
- `data_only=False` -- a formula cell's `.value` returns the formula
  STRING itself, e.g. `"=SUM(A1:A10)"`, never a computed value.

These are mutually exclusive per `load_workbook()` call. The common case
(only one of value-or-formula is wanted) opens the workbook exactly ONCE,
with `data_only=use_raw_values`. The workbook is opened a SECOND time only
when `include_cell_formatting=True`, because that mode additionally surfaces
each formula cell's original formula string alongside its value in
`cell_formatting_json` -- a thing a single `data_only` load structurally
cannot provide at the same time as the value. This is a real, deliberate
double-parse tradeoff made only in that one case, not a default behavior.

## Hidden sheets / rows / columns

openpyxl's own real, inspectable attributes are used as-is -- no invented
heuristic:

- `worksheet.sheet_state` -- one of openpyxl's own literal strings
  `"visible"` / `"hidden"` / `"veryHidden"`.
- `worksheet.row_dimensions[row_idx].hidden` -- per-row hidden flag.
- `worksheet.column_dimensions[col_letter].hidden` -- per-column hidden
  flag.

`include_hidden_sheets` / `include_hidden_rows` / `include_hidden_cols`
(all default `False`) gate these.

## Trimming empty trailing rows/columns (`only_data_rows` / `only_data_cols`)

Default ON, matching the sensible default of similar external spreadsheet
extractors. A sheet's used range as openpyxl itself tracks it
(`worksheet.max_row` / `worksheet.max_column`) can still include trailing
rows/columns that have formatting but no real value (e.g. a user dragged a
border or a fill color past the last real data cell). After the hidden
filter, genuinely fully-`None` trailing rows/columns (by actual cell value,
not just presence in the sheet) are additionally trimmed here.

## `include_cell_formatting`

When `True`, captures per-cell `bold` (`cell.font.bold`), `italic`
(`cell.font.italic`), and `number_format` (`cell.number_format`) -- real
openpyxl `Cell` attributes, used as-is, nothing invented -- plus (since this
mode double-loads the workbook, see above) each cell's original formula
string when it has one. Off by default: this repo does not emit a large
always-on formatting structure nobody asked for.

## `cell_data_mode`: `inline` / `external`

`"inline"` (default) keeps the whole sheet's cell grid as a JSON string in
the `cell_data` column. `"external"` is for very large sheets: once a
sheet's cell count exceeds `external_threshold_cells`, the grid is instead
written to a side JSONL file (one JSON object per cell) under `output_dir`
-- the same output-directory sidecar convention already used elsewhere in
this repo for large generated side-outputs (e.g.
`litellm_text_to_speech`'s `output_dir` field) -- and `cell_data` holds
`{"is_external": true, "path": ..., "count": ...}` instead of the inline
array.
"""

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


def _sheet_dimensions(worksheet):
    """openpyxl's own tracked used range for a worksheet, as (max_row, max_col).

    `worksheet.max_row` / `worksheet.max_column` are openpyxl's own real,
    already-computed used-range bounds (backed by `calculate_dimension()`)
    -- trusted directly rather than re-deriving them. A sheet with no cells
    at all reports `max_row`/`max_column` of 1 even though it is empty;
    callers handle that by also checking the actual cell values.
    """
    return worksheet.max_row or 0, worksheet.max_column or 0


def _included_indices(total: int, *, include_hidden, hidden_lookup) -> List[int]:
    """1-based indices (rows or cols) to keep, given a hidden-state lookup.

    `hidden_lookup(i)` returns True when index `i` (1-based) is hidden per
    openpyxl's own row_dimensions/column_dimensions state.
    """
    if include_hidden:
        return list(range(1, total + 1))
    return [i for i in range(1, total + 1) if not hidden_lookup(i)]


def _row_hidden(worksheet, row_idx: int) -> bool:
    dim = worksheet.row_dimensions.get(row_idx) if hasattr(worksheet.row_dimensions, "get") else None
    return bool(dim.hidden) if dim is not None else False


def _col_hidden(worksheet, col_idx: int) -> bool:
    from openpyxl.utils import get_column_letter

    letter = get_column_letter(col_idx)
    dim = worksheet.column_dimensions.get(letter) if hasattr(worksheet.column_dimensions, "get") else None
    return bool(dim.hidden) if dim is not None else False


def _build_grid(worksheet, row_indices: List[int], col_indices: List[int]) -> List[List[Any]]:
    """Row-major grid of real cell `.value`s for exactly the given (1-based) row/col indices.

    None stays None (openpyxl's own signal for a genuinely empty cell) --
    never coerced to "".
    """
    grid: List[List[Any]] = []
    for r in row_indices:
        row_vals = [worksheet.cell(row=r, column=c).value for c in col_indices]
        grid.append(row_vals)
    return grid


def _build_formatting_grid(worksheet, row_indices: List[int], col_indices: List[int]) -> List[List[Dict[str, Any]]]:
    """Per-cell bold/italic/number_format (+ formula string, when the cell is a formula)
    -- real openpyxl Cell/Font attributes, used as-is."""
    grid: List[List[Dict[str, Any]]] = []
    for r in row_indices:
        row_fmt = []
        for c in col_indices:
            cell = worksheet.cell(row=r, column=c)
            font = cell.font
            entry: Dict[str, Any] = {
                "bold": bool(font.bold) if font is not None else False,
                "italic": bool(font.italic) if font is not None else False,
                "number_format": cell.number_format,
            }
            if cell.data_type == "f":
                entry["formula"] = cell.value
            row_fmt.append(entry)
        grid.append(row_fmt)
    return grid


def _trimmed_row_col_counts(grid: List[List[Any]], *, trim_rows: bool, trim_cols: bool):
    """Return (keep_rows, keep_cols) after trimming genuinely fully-empty (None)
    trailing rows/columns from a row-major grid, by actual cell value -- not a
    separate heuristic, just the same values already read into `grid`."""
    keep_rows = len(grid)
    if trim_rows:
        while keep_rows > 0 and all(cell is None for cell in grid[keep_rows - 1]):
            keep_rows -= 1

    keep_cols = max((len(r) for r in grid), default=0)
    if trim_cols:
        while keep_cols > 0:
            col_idx = keep_cols - 1
            column_is_empty = all(
                col_idx >= len(grid[r]) or grid[r][col_idx] is None
                for r in range(keep_rows)
            ) if keep_rows > 0 else True
            if column_is_empty:
                keep_cols -= 1
            else:
                break
    return keep_rows, keep_cols


def _slice_grid(grid: List[List[Any]], keep_rows: int, keep_cols: int) -> List[List[Any]]:
    return [row[:keep_cols] for row in grid[:keep_rows]]


class SpreadsheetFinancialExtractorComponent(Component, Model, Resolvable):
    """Extract real cell-level structure (sheets/rows/cells/values/formulas/formatting) from a
    column of .xlsx/.xlsm file paths or bytes using openpyxl.

    NOT a markitdown-based flatten-to-text converter -- see
    `document_ingestion`'s xlsx handling for that (one generic markdown text
    blob per workbook, no cell-level fidelity at all). This component opens
    the workbook directly with openpyxl so it can expose openpyxl's own real
    knobs: hidden sheet/row/column inclusion, raw-cached-value-vs-formula-text
    (`data_only`), trailing-empty-range trimming, and per-cell
    bold/italic/number_format capture.

    One output row is produced PER SHEET (not per detected sub-table within a
    sheet -- a spreadsheet sheet IS already the natural table unit, and
    openpyxl gives no real signal for detecting multiple tables within one
    sheet the way a PDF table detector can for a page).
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

    column: Union[str, int] = Field(description="Column containing .xlsx/.xlsm file paths or bytes")

    passthrough_columns: Optional[List[str]] = Field(
        default=None,
        description="Upstream columns to carry through to each output sheet row (one copy per sheet). Defaults to every upstream column except `column`.",
    )

    use_raw_values: bool = Field(
        default=True,
        description=(
            "Wired directly to openpyxl's own load_workbook(data_only=...) argument. True (default) "
            "returns each formula cell's last-CACHED value (what a real spreadsheet app computed and "
            "saved) -- openpyxl never evaluates formulas itself, so a workbook that was never opened/"
            "saved by a real app has no cached value and returns None there. False returns the formula "
            "STRING itself (e.g. '=SUM(A1:A10)'), never a computed value."
        ),
    )

    include_hidden_sheets: bool = Field(
        default=False,
        description=(
            "Include sheets whose openpyxl sheet_state is 'hidden' or 'veryHidden' (openpyxl's own "
            "literal values, not invented here). Off by default -- a hidden sheet is usually hidden on "
            "purpose (e.g. scratch calculations, a superseded draft)."
        ),
    )
    include_hidden_rows: bool = Field(
        default=False,
        description="Include rows openpyxl reports as hidden via worksheet.row_dimensions[row_idx].hidden. Off by default.",
    )
    include_hidden_cols: bool = Field(
        default=False,
        description="Include columns openpyxl reports as hidden via worksheet.column_dimensions[col_letter].hidden. Off by default.",
    )

    only_data_rows: bool = Field(
        default=True,
        description=(
            "Trim genuinely fully-empty (None) trailing rows from each sheet's used range after the "
            "hidden-row filter. Default ON. A sheet's openpyxl-tracked used range can include trailing "
            "rows that carry formatting but no real value."
        ),
    )
    only_data_cols: bool = Field(
        default=True,
        description="Same trimming as only_data_rows, for trailing columns. Default ON.",
    )

    include_cell_formatting: bool = Field(
        default=False,
        description=(
            "Off by default -- this repo does not emit a large always-on formatting structure nobody "
            "asked for. When True, captures per-cell bold/italic/number_format (real openpyxl Cell/Font "
            "attributes) plus each formula cell's original formula string, as a parallel "
            "`cell_formatting_json` column. Enabling this makes the component load the workbook a SECOND "
            "time (once data_only=use_raw_values for the value grid, once data_only=False to recover "
            "formula strings) -- a real, deliberate double-parse cost paid only in this mode, since a "
            "single openpyxl load cannot return both a formula's cached value and its formula text at once."
        ),
    )

    cell_data_mode: Literal["inline", "external"] = Field(
        default="inline",
        description=(
            "'inline' (default) keeps the sheet's cell grid as a JSON string in `cell_data`. 'external' "
            "is for very large sheets: once a sheet's cell count exceeds `external_threshold_cells`, the "
            "grid is instead written to a side JSONL file (one JSON object per cell) under `output_dir`, "
            "and `cell_data` holds {'is_external': true, 'path': ..., 'count': ...} instead of the inline array."
        ),
    )
    external_threshold_cells: int = Field(
        default=50000,
        ge=1,
        description="Cell-count threshold above which cell_data_mode='external' writes a side JSONL file instead of inlining the grid.",
    )
    output_dir: str = Field(
        default="/tmp/spreadsheet_financial_extractor_cells",
        description="Directory side JSONL cell files are written to when cell_data_mode='external' produces an external file for a sheet. Created if missing.",
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
        return ComponentFormConfig(label='Spreadsheet Financial Extractor', editable=True)

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
        use_raw_values = self.use_raw_values
        include_hidden_sheets = self.include_hidden_sheets
        include_hidden_rows = self.include_hidden_rows
        include_hidden_cols = self.include_hidden_cols
        only_data_rows = self.only_data_rows
        only_data_cols = self.only_data_cols
        include_cell_formatting = self.include_cell_formatting
        cell_data_mode = self.cell_data_mode
        external_threshold_cells = self.external_threshold_cells
        output_dir = self.output_dir

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
        _comp_name = "spreadsheet_financial_extractor"  # component directory name
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
                import openpyxl
            except ImportError:
                raise ImportError("Install openpyxl: pip install openpyxl")

            import io
            import json
            import os

            if column not in upstream.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame.")

            _passthrough_cols = list(passthrough_columns) if passthrough_columns else [c for c in upstream.columns if c != column]
            _missing_passthrough = [c for c in _passthrough_cols if c not in upstream.columns]
            if _missing_passthrough:
                raise ValueError(f"passthrough_columns not found in DataFrame: {_missing_passthrough}")

            if cell_data_mode == "external":
                os.makedirs(output_dir, exist_ok=True)

            records: List[Dict[str, Any]] = []
            sheets_detected = 0
            sheets_kept = 0
            documents_failed = 0
            external_files_written = 0

            def _open(value):
                return io.BytesIO(value) if isinstance(value, bytes) else str(value)

            for doc_index, (_, src_row) in enumerate(upstream.iterrows()):
                value = src_row[column]
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    continue
                try:
                    workbook = openpyxl.load_workbook(_open(value), data_only=use_raw_values)
                    formula_workbook = (
                        openpyxl.load_workbook(_open(value), data_only=False)
                        if include_cell_formatting
                        else None
                    )

                    for sheet_index, worksheet in enumerate(workbook.worksheets):
                        sheets_detected += 1
                        if worksheet.sheet_state != "visible" and not include_hidden_sheets:
                            continue

                        max_row, max_col = _sheet_dimensions(worksheet)
                        row_indices = _included_indices(
                            max_row, include_hidden=include_hidden_rows,
                            hidden_lookup=lambda i: _row_hidden(worksheet, i),
                        )
                        col_indices = _included_indices(
                            max_col, include_hidden=include_hidden_cols,
                            hidden_lookup=lambda i: _col_hidden(worksheet, i),
                        )

                        value_grid = _build_grid(worksheet, row_indices, col_indices)
                        keep_rows, keep_cols = _trimmed_row_col_counts(
                            value_grid, trim_rows=only_data_rows, trim_cols=only_data_cols,
                        )
                        value_grid = _slice_grid(value_grid, keep_rows, keep_cols)

                        format_grid = None
                        if include_cell_formatting:
                            format_ws = formula_workbook[worksheet.title]
                            format_grid = _build_formatting_grid(format_ws, row_indices, col_indices)
                            format_grid = _slice_grid(format_grid, keep_rows, keep_cols)

                        row_count = len(value_grid)
                        col_count = max((len(r) for r in value_grid), default=0)
                        sheets_kept += 1

                        record: Dict[str, Any] = {c: src_row[c] for c in _passthrough_cols}
                        record["sheet_name"] = worksheet.title
                        record["sheet_index"] = sheet_index
                        record["row_count"] = row_count
                        record["col_count"] = col_count

                        cell_count = row_count * col_count
                        if cell_data_mode == "external" and cell_count > external_threshold_cells:
                            _fname = f"{asset_name}_{doc_index}_{sheet_index}_cells.jsonl"
                            _fpath = os.path.join(output_dir, _fname)
                            with open(_fpath, "w") as _f:
                                for r_i, row in enumerate(value_grid):
                                    for c_i, cell_value in enumerate(row):
                                        _f.write(json.dumps({"row": r_i, "col": c_i, "value": cell_value}) + "\n")
                            external_files_written += 1
                            record["cell_data"] = json.dumps({
                                "is_external": True, "path": _fpath, "count": cell_count,
                            })
                        else:
                            # None cells are genuinely empty cells (openpyxl's own
                            # signal) -- preserved as JSON null, never coerced to "".
                            record["cell_data"] = json.dumps(value_grid)

                        if include_cell_formatting:
                            record["cell_formatting_json"] = json.dumps(format_grid)

                        try:
                            if row_count > 1:
                                _md_df = pd.DataFrame(value_grid[1:], columns=value_grid[0])
                            else:
                                _md_df = pd.DataFrame(value_grid)
                            record["sheet_markdown"] = _md_df.to_markdown(index=False)
                        except Exception:
                            record["sheet_markdown"] = pd.DataFrame(value_grid).to_markdown(index=False)

                        records.append(record)
                except Exception as e:
                    documents_failed += 1
                    context.log.warning(f"Failed to extract spreadsheet: {e}")
                    continue

            _expected_cols = list(_passthrough_cols) + [
                "sheet_name", "sheet_index", "row_count", "col_count", "cell_data",
            ]
            if include_cell_formatting:
                _expected_cols.append("cell_formatting_json")
            _expected_cols.append("sheet_markdown")

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
                "sheets_detected": MetadataValue.int(sheets_detected),
                "sheets_kept": MetadataValue.int(sheets_kept),
                "documents_failed": MetadataValue.int(documents_failed),
                "external_cell_files_written": MetadataValue.int(external_files_written),
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
