# BlobConvertComponent

Convert a column of binary blobs (bytes) or text between common encodings.
Drop-in for Alteryx's **Blob Convert** tool.

## Supported operations

| `operation:` | Direction |
|---|---|
| `to_base64` | bytes → base64-encoded string |
| `from_base64` | base64 string → bytes |
| `to_hex` | bytes → lowercase hex string |
| `from_hex` | hex string → bytes |
| `to_text` | bytes → decoded string (default UTF-8) |
| `to_bytes` | text → bytes (default UTF-8) |

## Use Cases

- Convert API response bytes to base64 for JSON / CSV storage
- Decode base64-encoded image / PDF columns received from a webhook
- Hex-encode binary checksums for legible logging
- Round-trip text/bytes for downstream tools that don't accept binary

## Example

```yaml
type: dagster_community_components.BlobConvertComponent
attributes:
  asset_name: api_payloads_base64
  upstream_asset_key: api_responses
  input_column: payload_bytes
  operation: to_base64
  output_column: payload_b64
  group_name: ingestion
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | Must contain `input_column` |
| Output | `pd.DataFrame` | Original columns; `output_column` (or `input_column` overwritten) holds converted values |

## Notes

- `output_column` defaults to overwriting `input_column`.
- `encoding` (default `utf-8`) governs the `to_text` / `to_bytes` operations.
- `error_handling` (default `coerce`) sets per-row failures to `None` and
  logs the count. Set to `raise` to fail the asset on the first bad row.
- `null` / `NaN` inputs always pass through as `None` regardless of mode.
