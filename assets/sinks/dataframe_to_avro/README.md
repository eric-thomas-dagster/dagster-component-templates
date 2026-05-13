# Dataframe → Avro sink

Write a pandas DataFrame as Avro to a local path or any fsspec-supported URI (`s3://`, `gs://`, `abfss://`, `gs://`, `file://`). Companion to `dataframe_to_parquet`.

Schema is inferred from pandas dtypes by default (every column emitted as a nullable Avro union `["null", <type>]` so NaN-bearing DataFrames serialize cleanly). For producers that need explicit schema control (named records, namespaces, non-nullable fields), pass `avro_schema:` as a YAML dict to override inference.

## Why Avro

Avro is the canonical format for several streaming sinks:
- **Azure Event Hubs Capture** writes Avro by default (Parquet is Premium tier / preview).
- **Kafka** ecosystems with Schema Registry standardize on Avro.
- **Confluent Cloud** / **AWS MSK** + Schema Registry — same story.

If you're producing data that downstream Kafka/EH consumers will read, Avro gives you stable schema evolution.

## Configuration

```yaml
type: dagster_component_templates.DataframeToAvroComponent
attributes:
  asset_name: events_avro
  upstream_asset_key: events_summary
  file_path: "s3://my-bucket/events/{partition_key}.avro"
  codec: snappy
  record_name: Event
  group_name: sinks
```

## Fields

| Field | Default | Purpose |
|---|---|---|
| `asset_name` | required | Output Dagster asset name |
| `upstream_asset_key` | required | Upstream asset that produces the DataFrame |
| `file_path` | required | Destination — local path or fsspec URI; supports `${ENV_VAR}` and `{partition_key}` |
| `codec` | `null` | Avro compression: `null`, `snappy`, `deflate`, `bzip2`, `xz`, `zstandard` |
| `record_name` | `Record` | Name embedded in the inferred Avro schema |
| `avro_schema` | `None` | Optional explicit schema dict (overrides inference). |
| `description` / `group_name` / `deps` | — | Standard Dagster metadata |
| `partition_type` / `partition_start` / `partition_values` / `dynamic_partition_name` | — | Standard partition fields |

## Schema inference details

Every pandas dtype is mapped to an Avro union with `null`:

| Pandas dtype | Avro type |
|---|---|
| `int8`/`int16`/`int32`/`uint8`/`uint16` | `["null", "int"]` |
| `int64`/`uint32`/`uint64` | `["null", "long"]` |
| `float32` | `["null", "float"]` |
| `float64` | `["null", "double"]` |
| `bool` | `["null", "boolean"]` |
| `datetime64[ns]` (and tz-aware) | `["null", "string"]` (ISO-8601) |
| `object` / `string` / `category` | `["null", "string"]` |

Anything else falls back to `string`. For stricter typing (e.g., Avro logical types `timestamp-millis`, `decimal`, fixed-size), pass `avro_schema:` explicitly.

## Cloud auth

Same fsspec rules as `dataframe_to_parquet`:
- **S3**: env vars / IRSA / instance role / `~/.aws/credentials`
- **GCS**: `GOOGLE_APPLICATION_CREDENTIALS` / Workload Identity
- **Azure**: `AZURE_STORAGE_CONNECTION_STRING` / `DefaultAzureCredential`

Install per-scheme driver: `s3fs` / `gcsfs` / `adlfs`.

## Reading the Avro file back

`file_ingestion` supports `format: avro` — see [`../../assets/ingestion/file_ingestion/`](../../assets/ingestion/file_ingestion/). For ad-hoc reads in a notebook:

```python
import fastavro
import fsspec
import pandas as pd

with fsspec.open("s3://my-bucket/events/2026-05-13.avro", "rb") as f:
    df = pd.DataFrame(list(fastavro.reader(f)))
```

## See also

- [`dataframe_to_parquet`](../dataframe_to_parquet/) — Parquet sibling.
- [`file_ingestion`](../../ingestion/file_ingestion/) — reads Avro (and 6 other formats) into pandas.
