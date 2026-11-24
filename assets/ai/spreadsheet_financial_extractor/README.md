# Spreadsheet Financial Extractor

Extract real cell-level spreadsheet structure -- sheets, rows, cells, raw
values, formulas, and per-cell formatting -- directly from `.xlsx`/`.xlsm`
workbooks using `openpyxl`, with zero LLM cost.

## Why this exists, and why it's different from `document_ingestion`'s xlsx handling

`document_ingestion`'s existing `.xlsx` support goes through `markitdown`,
which flattens an entire workbook into one generic markdown text blob with
**no cell-level fidelity at all**: no control over hidden sheets/rows/
columns, no choice between a formula's cached value and its formula string,
no formulas, no per-cell formatting. That's a real, honest gap -- not
something this repo previously had covered despite once believing so.

This component opens the workbook directly with `openpyxl` instead of going
through a generic document-to-markdown converter, so it can expose
`openpyxl`'s own real cell-level knobs.

## `use_raw_values` / `data_only`

Wired directly to `openpyxl`'s own documented `load_workbook(data_only=...)`
argument -- not reimplemented:

- `use_raw_values: true` (default) -- a formula cell's value is its
  last-CACHED value (whatever a real spreadsheet application computed and
  saved). `openpyxl` never evaluates formulas itself, so a workbook that was
  never opened/saved by a real spreadsheet app has no cached value there and
  returns `None`.
- `use_raw_values: false` -- a formula cell's value is the formula STRING
  itself, e.g. `"=SUM(A1:A10)"`, never a computed value.

These are mutually exclusive per `openpyxl` load. The common case opens the
workbook exactly once. `include_cell_formatting: true` is the one case that
opens it a second time (see below), since a single `data_only` load cannot
return both a formula's value and its formula text at once.

## Hidden sheets / rows / columns

Real, inspectable `openpyxl` state -- not an invented heuristic:

- `include_hidden_sheets` (default `false`) -- `openpyxl`'s own
  `sheet_state` values are `"visible"` / `"hidden"` / `"veryHidden"`.
- `include_hidden_rows` (default `false`) -- per-row
  `row_dimensions[row_idx].hidden`.
- `include_hidden_cols` (default `false`) -- per-column
  `column_dimensions[col_letter].hidden`.

## Trimming empty trailing rows/columns

`only_data_rows` / `only_data_cols` (both default **ON**) trim genuinely
fully-empty trailing rows/columns from each sheet's used range (after the
hidden filter) -- e.g. a styled-but-valueless cell far from the real data
that extends `openpyxl`'s own tracked `max_row`/`max_column` without adding
any real content.

## `include_cell_formatting`

Off by default. When `true`, captures per-cell `bold` / `italic` /
`number_format` -- real `openpyxl` `Cell`/`Font` attributes, nothing
invented -- plus each formula cell's original formula string, as a parallel
`cell_formatting_json` column (absent entirely when disabled, not emitted as
an always-on empty structure).

## `cell_data_mode`: `inline` / `external`

```yaml
attributes:
  cell_data_mode: external
  external_threshold_cells: 50000
  output_dir: /tmp/spreadsheet_financial_extractor_cells
```

`"inline"` (default) keeps the whole sheet's cell grid as a JSON string in
`cell_data`. `"external"` is for very large sheets: once a sheet's cell
count exceeds `external_threshold_cells`, the grid is instead written to a
side JSONL file (one JSON object per cell) under `output_dir`, and
`cell_data` holds `{"is_external": true, "path": ..., "count": ...}` instead
of the inline array.

## Output shape: one row per sheet

```yaml
type: dagster_community_components.SpreadsheetFinancialExtractorComponent
attributes:
  asset_name: extracted_spreadsheet_cells
  upstream_asset_key: raw_xlsx_files
  column: file_path
  use_raw_values: true
  only_data_rows: true
  only_data_cols: true
```

A spreadsheet sheet IS already the natural table unit, so every sheet (after
the hidden-sheet filter) becomes its own output row -- no sub-table
detection heuristic is attempted within a sheet:

| Column | Meaning |
|---|---|
| *(passthrough columns)* | Every upstream column except `column`, by default -- override with `passthrough_columns` |
| `sheet_name` | The worksheet's title |
| `sheet_index` | 0-based index of this sheet within its source workbook |
| `row_count` | Rows in the (hidden-filtered, trimmed) grid |
| `col_count` | Columns in the (hidden-filtered, trimmed) grid |
| `cell_data` | JSON string: a list of lists of cell values (inline mode), or `{"is_external": true, "path": ..., "count": ...}` (external mode). A cell `openpyxl` reports as `None` stays JSON `null` -- never coerced to `""`. |
| `cell_formatting_json` | Present only when `include_cell_formatting: true`: parallel JSON grid of `{bold, italic, number_format, formula}` per cell. |
| `sheet_markdown` | Markdown rendering of the sheet (via pandas `.to_markdown()`) for the Dagster UI preview. |

## Requirements

`openpyxl` for the default path. `tabulate` for `sheet_markdown` rendering
via pandas `.to_markdown()`.

## Known, deliberately-deferred gap

Only `cell_data_mode="external"`'s JSONL side-file format is supported for
very large sheets -- there is no streaming/chunked read path for workbooks
too large to load into memory with `openpyxl` at all. That is out of scope
here.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output Dagster asset name |
| `upstream_asset_key` | `Union[str, List[str]]` | Upstream asset key providing a DataFrame |
| `column` | `Union[str, int]` | Column containing .xlsx/.xlsm file paths or bytes |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group name |
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | Backfill execution strategy for a partitioned asset. 'single_run' materializes an entire backfill range in one run (the upstream DataFrame is filtered once per partition key in the range and the matching rows are combine… _(full docs in schema.json + component README)_ |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']} |
| `description` | `str` | — | Asset description shown in the Dagster catalog. |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on Dagster's TimeWindowPartitionsDefinition(cron_schedule=...). |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule. |
| `partition_end_offset` | `int` | `0` | Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition. 0 (the Dagster default) means a time-based partition set only ever holds COMPLETED windows -- e.g. a daily set never includes today… _(full docs in schema.json + component README)_ |
| `partition_timezone` | `str` | — | IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default). |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on failure. Defines a RetryPolicy when set. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `output_dir` | `str` | `"/tmp/spreadsheet_financial_extractor_cells"` | Directory side JSONL cell files are written to when cell_data_mode='external' produces an external file for a sheet. Created if missing. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run). |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output data in metadata (first 5 rows as a markdown table). Used by builder UIs to render asset shape without warehouse access. |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when `include_preview_metadata` is True. For long DataFrames (>10x preview_rows), a random sample is used so the preview reflects the data distribution; otherwise head() is used. |
| `passthrough_columns` | `List[str]` | — | Upstream columns to carry through to each output sheet row (one copy per sheet). Defaults to every upstream column except `column`. |
| `use_raw_values` | `bool` | `true` | Wired directly to openpyxl's own load_workbook(data_only=...) argument. True (default) returns each formula cell's last-CACHED value (what a real spreadsheet app computed and saved) -- openpyxl never evaluates formulas i… _(full docs in schema.json + component README)_ |
| `include_hidden_sheets` | `bool` | `false` | Include sheets whose openpyxl sheet_state is 'hidden' or 'veryHidden' (openpyxl's own literal values, not invented here). Off by default -- a hidden sheet is usually hidden on purpose (e.g. scratch calculations, a superseded draft). |
| `include_hidden_rows` | `bool` | `false` | Include rows openpyxl reports as hidden via worksheet.row_dimensions[row_idx].hidden. Off by default. |
| `include_hidden_cols` | `bool` | `false` | Include columns openpyxl reports as hidden via worksheet.column_dimensions[col_letter].hidden. Off by default. |
| `only_data_rows` | `bool` | `true` | Trim genuinely fully-empty (None) trailing rows from each sheet's used range after the hidden-row filter. Default ON. A sheet's openpyxl-tracked used range can include trailing rows that carry formatting but no real value. |
| `only_data_cols` | `bool` | `true` | Same trimming as only_data_rows, for trailing columns. Default ON. |
| `include_cell_formatting` | `bool` | `false` | Off by default -- this repo does not emit a large always-on formatting structure nobody asked for. When True, captures per-cell bold/italic/number_format (real openpyxl Cell/Font attributes) plus each formula cell's orig… _(full docs in schema.json + component README)_ |
| `cell_data_mode` | `Literal['inline', 'external']` | `"inline"` | 'inline' (default) keeps the sheet's cell grid as a JSON string in `cell_data`. 'external' is for very large sheets: once a sheet's cell count exceeds `external_threshold_cells`, the grid is instead written to a side JSO… _(full docs in schema.json + component README)_ |
| `external_threshold_cells` | `int` | `50000` | Cell-count threshold above which cell_data_mode='external' writes a side JSONL file instead of inlining the grid. |

[//]: # (FIELDS:END)

## Validation

`validation.level: code` -- verified against real `.xlsx` workbooks built
directly with `openpyxl` itself (never mocked): real hidden sheets/rows/
columns (confirmed to round-trip through `openpyxl`'s own
`sheet_state`/`row_dimensions`/`column_dimensions`), a real formula cell
with an injected cached value (confirming `use_raw_values=True` vs `False`
genuinely differ), real trailing-empty trimming against a workbook with a
styled-but-valueless cell extending the tracked range, real bold/italic/
number_format capture, and a genuinely-empty-string cell preserved distinct
from a wholly-absent one.
