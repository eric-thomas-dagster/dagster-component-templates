# dataframe_to_json

## Purpose

Writes a Pandas DataFrame to a JSON or JSON Lines file. This is a terminal sink component — it receives a DataFrame from an upstream asset via Dagster's `ins` mechanism and persists it using `DataFrame.to_json`. Supports multiple output orientations and JSON Lines format for streaming-friendly output. It returns a `MaterializeResult` with `row_count`, `column_count`, `file_path`, `orient`, and `lines` metadata.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing the DataFrame |
| `file_path` | `str` | required | Destination file path. Supports env var substitution e.g. `${OUTPUT_DIR}/results.json` |
| `orient` | `str` | `"records"` | pandas JSON orient: `"records"`, `"index"`, `"columns"`, `"values"`, `"table"`, `"split"`. Ignored when `lines=True`. |
| `lines` | `bool` | `False` | Write as JSON Lines format (one object per line). Auto-sets orient to `"records"`. |
| `indent` | `int` | `None` | JSON indentation level. Ignored when `lines=True`. |
| `date_format` | `str` | `"iso"` | Date formatting: `"iso"` (ISO 8601) or `"epoch"` (milliseconds since epoch). |
| `group_name` | `str` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.DataframeToJsonComponent
attributes:
  asset_name: write_events_jsonl
  upstream_asset_key: processed_events
  file_path: ${OUTPUT_DIR}/events.jsonl
  orient: records
  lines: true
  date_format: iso
  group_name: output
```

## Notes

### JSON Lines Format

Set `lines: true` to write one JSON object per line (JSONL / NDJSON format). This is the preferred format for streaming ingestion, log processing, and large datasets. When `lines=true`, `orient` is automatically set to `"records"` and `indent` is ignored.

### Orient Options

- `"records"`: `[{col: val}, ...]` — most common for downstream consumption
- `"split"`: `{index: [...], columns: [...], data: [...]}` — compact, preserves column order
- `"index"`: `{index: {col: val}}` — keyed by index
- `"columns"`: `{col: {index: val}}` — keyed by column
- `"values"`: array of arrays — no column names
- `"table"`: includes pandas table schema metadata

### Environment Variable Substitution

The `file_path` supports shell-style environment variable substitution via `os.path.expandvars`.

### Directory Creation

The component automatically creates parent directories if they do not exist.

### Materialization Metadata

This component returns a `MaterializeResult` with `row_count`, `column_count`, `file_path`, `orient`, and `lines` metadata.

### IO Manager

This component uses Dagster's `ins` mechanism to receive the upstream DataFrame. No IO manager configuration is required for local development.

### Requirements

Install `pandas`. No additional dependencies are required.
