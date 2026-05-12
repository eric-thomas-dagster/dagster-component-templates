# Text Codec Convert

Convert text between codecs (charsets). Two modes:
- **`mode: file`** — read a column of file paths, recode each file's bytes, write new files (the canonical mainframe interop pattern: EBCDIC → UTF-8)
- **`mode: string`** — recode a column of strings in place (drop non-target characters from text fields before downstream loads)

```yaml
type: dagster_component_templates.TextCodecConvertAssetComponent
attributes:
  asset_name: utf8_files
  upstream_asset_key: ebcdic_files
  mode: file
  source_path_column: file_path
  from_codec: cp037     # IBM US EBCDIC
  to_codec:   utf-8
  errors:     replace
```

## ASCII ↔ EBCDIC

EBCDIC is IBM's mainframe character encoding. z/OS systems (banking core systems, insurance policy admin, federal agencies, claims clearinghouses) still ship daily EBCDIC files. Convert with:

| Codepage | Use for |
|---|---|
| `cp037` | US/Canada (most common) |
| `cp1140` | US with Euro symbol (`€` at `0x9F`) |
| `cp273` | Germany / Austria |
| `cp500` | Western Europe (Latin-1 EBCDIC) |
| `cp1047` | Open Systems Latin-1 EBCDIC (AIX, Linux on Z) |
| `cp875` | Greek |
| `cp1025` | Cyrillic |
| `cp930` / `cp939` | Japanese |
| `cp935` | Simplified Chinese |
| `cp937` | Traditional Chinese |

Python's `codecs` module ships them all natively — no extra deps.

## Standard codec pairs

| From | To | Use |
|---|---|---|
| `cp037` | `utf-8` | EBCDIC → modern Unicode |
| `cp1252` | `utf-8` | Windows-1252 (legacy Office exports) → UTF-8 |
| `latin-1` | `utf-8` | Legacy ISO-8859-1 → UTF-8 |
| `utf-16` | `utf-8` | Cut down 2-byte UTF-16 to UTF-8 |
| `utf-8` | `ascii` | Strip non-ASCII (with `errors: ignore`) before legacy system |

## `errors` modes

| Mode | Behavior |
|---|---|
| `strict` (default) | Raise on un-encodable / un-decodable char |
| `replace` | Substitute `?` (encode) / `�` (decode) — most forgiving ETL setting |
| `ignore` | Silently drop the char |
| `backslashreplace` | Escape as `\xNN` |

## File mode example chain

```
ebcdic_files          (DataFrame of {file_id, file_path} pointing at z/OS exports)
   └── utf8_files     ← text_codec_convert_asset (cp037 → utf-8)
        └── parsed     ← bigquery_load_from_gcs / dataframe_to_bigquery / fixed_width_parser
```

## String mode example

```yaml
attributes:
  mode: string
  source_column: customer_name
  target_column: customer_name   # in-place
  from_codec: cp1252
  to_codec:   utf-8
  errors:     replace
```

Sanitizes a DataFrame column of customer names — strip / replace any non-UTF-8 characters (e.g. Windows-1252 smart quotes) before pushing into a system that requires strict UTF-8.

## Output

**File mode** adds: `converted_path`, `codec_error`, `size_bytes_in`, `size_bytes_out`.
**String mode** writes to `target_column` (defaults to `source_column` for in-place).

## Sister components

- `bigquery_load_from_gcs_asset` — common downstream after EBCDIC → UTF-8
- `dataframe_to_bigquery` — common downstream for string-mode-normalized DataFrames
- `dataframe_flatten_nested_columns` — pairs with string-mode for full pre-warehouse normalization
## ⚠️ Deployment note (Dagster+ / Kubernetes)

This component reads or writes local filesystem paths. Behavior across deployments:

| Environment | Works? |
|---|---|
| Local dev | ✅ Yes |
| Dagster+ Serverless (multiprocess executor, default) | ✅ Within a single run — `/tmp/...` is shared across ops in the same run. Files do **not** persist after the run ends. |
| Dagster Hybrid on k8s with `k8s_job` executor (op-per-pod) | ❌ Each op runs in its own pod with its own `/tmp` — files don't travel between ops, even within one run. Set the run to use the `in_process` executor as a workaround. |
| Cross-run reads (run N writes, run N+1 reads) | ❌ Anywhere — the local filesystem is ephemeral by definition. |

**Recommended alternatives for production:**

1. **Return bytes as the asset value** instead of writing a file. The default `PickledObjectFilesystemIOManager` (and the Dagster+ Serverless S3-backed IO manager) serialize binary data fine. Downstream ops read the bytes from the IO manager regardless of pod / run.
2. **Use a cloud-storage sink** for cross-run persistence: [`dataframe_to_s3`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_s3), [`dataframe_to_gcs`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_gcs), [`dataframe_to_adls`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_adls).
3. **Mount a shared volume** (k8s PVC / Cloud Run volumes) if you genuinely need a shared filesystem path across pods.
