# Archive Fetcher

Download a remote archive (ZIP, tar.gz, etc.), extract it to a directory, and emit a `{filename: absolute_path}` dict so downstream assets can reference individual files.

Pairs naturally with `rest_api_fetcher`, which handles JSON/CSV/DataFrame/Parquet responses but doesn't unpack binary archives. Many public datasets ship as bundles:

- **MovieLens** — `ml-latest-small.zip` (4 CSVs: movies, ratings, links, tags)
- **IMDb** — `title.basics.tsv.gz`, `title.ratings.tsv.gz` (gzipped TSV)
- **GTFS transit feeds** — bundled `routes.txt`, `stops.txt`, `trips.txt`, … inside a single ZIP
- **USGS earthquake archives**, **BLS time-series dumps**, **Kaggle dataset releases** — typically ZIP or tar.gz
- **OpenStreetMap extracts** — `.osm.pbf`, **WorldBank** zips, **OECD** zips

```yaml
type: dagster_component_templates.ArchiveFetcherComponent
attributes:
  asset_name: movielens_raw
  url: https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
  extract_to: /tmp/movielens
  flatten: true            # strip the top-level "ml-latest-small/" dir
  include_glob: ["*.csv"]  # only emit the CSVs in the output dict
```

## Asset output

The asset's runtime value is a `dict[str, str]`:

```python
{
    "movies.csv":  "/tmp/movielens/movies.csv",
    "ratings.csv": "/tmp/movielens/ratings.csv",
    "links.csv":   "/tmp/movielens/links.csv",
    "tags.csv":    "/tmp/movielens/tags.csv",
}
```

Downstream assets can either:

1. **Reference files by path** (recommended — predictable, no input-binding gymnastics):
   ```yaml
   type: dagster_component_templates.CSVFileIngestionComponent
   attributes:
     asset_name: movies
     file_path: /tmp/movielens/movies.csv
     deps: [movielens_raw]   # declare the dependency for lineage
   ```

2. **Consume the dict directly** in a custom Python asset that takes `movielens_raw` as input.

## Supported archive types

| `archive_type` | Suffixes auto-detected |
|---|---|
| `zip` | `.zip` |
| `tar` | `.tar` |
| `tar.gz` | `.tar.gz`, `.tgz` |
| `tar.bz2` | `.tar.bz2`, `.tbz`, `.tbz2` |
| `tar.xz` | `.tar.xz`, `.txz` |
| `gz` (single file) | `.gz` (not preceded by `.tar`) |
| `bz2` (single file) | `.bz2` (not preceded by `.tar`) |

If the URL has no recognized suffix, set `archive_type:` explicitly.

## Attributes

| Field | Default | Purpose |
|---|---|---|
| `asset_name` | required | Asset key |
| `url` | required | URL of the archive |
| `extract_to` | `/tmp/<asset_name>/` | Destination directory |
| `archive_type` | inferred from URL | Override if the URL has no recognizable suffix |
| `flatten` | `false` | If all entries share a single top-level dir, strip it |
| `include_glob` | — | List of fnmatch patterns (e.g. `["*.csv", "*.tsv"]`) restricting which files appear in the output dict (everything still extracts to disk) |
| `timeout` | `300` | HTTP timeout for download (seconds) |
| `verify_ssl` | `true` | SSL verification |
| `headers` | — | Dict or JSON string of headers — e.g. for `Authorization` or `User-Agent` |
| `cleanup_archive` | `true` | Delete the downloaded archive after extraction |
| `deps` | — | Upstream asset keys (for lineage) |
| `description` / `group_name` / `tags` / `owners` / `kinds` | — | Standard Dagster metadata |

## Single-file gz / bz2 behavior

When the archive is a single-file `.gz` (e.g. IMDb's `title.basics.tsv.gz`), the output filename is derived by stripping the suffix:

```yaml
url: https://datasets.imdbws.com/title.basics.tsv.gz
# → extracts to <extract_to>/title.basics.tsv
```

## Storage-agnostic destinations

`extract_to:` accepts either a local path or a remote URI:

```yaml
extract_to: /tmp/movielens                                               # local
extract_to: file:///tmp/movielens                                         # local (explicit scheme)
extract_to: s3://my-bucket/movielens/                                     # S3 — needs `pip install s3fs`
extract_to: gs://my-bucket/movielens/                                     # GCS — needs `pip install gcsfs`
extract_to: abfss://container@account.dfs.core.windows.net/movielens/     # Azure Gen2 (canonical) — needs `pip install adlfs`
extract_to: abfs://container@account.dfs.core.windows.net/movielens/      # Azure (HTTP) — adlfs
extract_to: az://container/movielens/                                     # Azure (adlfs alias for abfss)
```

For remote URIs the archive is always downloaded to a local temp dir, extracted there, then uploaded file-by-file via `fsspec`; the temp dir is deleted after. The emitted dict contains remote URIs (e.g. `{"movies.csv": "s3://my-bucket/movielens/movies.csv"}`), which `pd.read_csv` consumes directly via fsspec — so downstream ingest components like `csv_file_ingestion` with `from_upstream` continue to work unchanged.

**Auth:** fsspec uses ambient credentials from the underlying cloud SDK:
- **S3**: env vars (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION`), `~/.aws/credentials`, EC2 instance profile, ECS task role, EKS IRSA.
- **GCS**: `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default login`, GCE/GKE workload identity.
- **Azure**: `AZURE_STORAGE_CONNECTION_STRING`, `DefaultAzureCredential` chain. For `abfss://` URIs the host portion (`container@account.dfs.core.windows.net`) identifies the storage account directly; for `az://` you need `AZURE_STORAGE_ACCOUNT_NAME` set so adlfs knows which account to target.

No explicit resource wiring required — production deployments typically just have the right env / instance role set.

## Security

- Path-traversal protection: any archive entry resolving outside `extract_to` is rejected (defends against "zip-slip" attacks).
- HTTPS verification is on by default; set `verify_ssl: false` only for trusted internal mirrors.

## Pipes well into…

- [`csv_file_ingestion`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/ingestion/csv_file_ingestion) — CSV → DataFrame
- `tsv_file_ingestion`, `excel_file_ingestion`, `json_file_ingestion`
- Any DuckDB / SQLite component that reads files by absolute path

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset |
| `url` | `str` | URL of the archive to download |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout` | `int` | `300` | HTTP timeout in seconds for the download |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | — |
| `group_name` | `str` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `owners` | `List[str]` | — | — |
| `kinds` | `List[str]` | — | — |
| `deps` | `List[str]` | — | Upstream asset keys this asset depends on |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned. |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_values` | `str` | — | Comma-separated values for static partitioning, e.g. 'us,eu,asia'. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Useful for transient errors like network glitches or rate limits. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `extract_to` | `str` | — | Destination for extracted files. Either a local directory path (`/tmp/foo`, `file:///tmp/foo`) or a remote URI: `s3://bucket/prefix/`, `gs://bucket/prefix/`, `abfss://container@account.dfs.core.windows.net/prefix/` (canonical ADLS Gen2), `abfs://...`, or `az://container/prefix/` (adlfs alias). For remote URIs the archive is extracted to a local temp dir, uploaded via fsspec, then the temp dir is cleaned up. Auth uses fsspec's ambient credential discovery (env vars, ~/.aws/credentials, IRSA, instance role, etc.). Install s3fs / gcsfs / adlfs for the scheme you need. Default: `/tmp/<asset_name>/`. |
| `archive_type` | `str` | — | Archive type: 'zip', 'tar', 'tar.gz', 'tar.bz2', 'tar.xz', 'gz', 'bz2'. If omitted, inferred from the URL extension. |
| `flatten` | `bool` | `false` | If True and all archive entries share a single top-level directory (e.g. `ml-latest-small/movies.csv`), strip that directory so files land directly under `extract_to`. |
| `include_glob` | `List[str]` | — | Optional list of fnmatch patterns (e.g. `['*.csv', '*.tsv']`) to restrict which extracted files appear in the asset's output dict. Files that don't match are still extracted to disk but omitted from the returned mapping + metadata. |
| `verify_ssl` | `bool` | `true` | Whether to verify SSL certificates on the download |
| `headers` | `Union[str, Dict[str, str]]` | — | Optional HTTP headers for the download — YAML dict or JSON string. Useful for `Authorization`, `User-Agent`, etc. |
| `cleanup_archive` | `bool` | `true` | If True, delete the downloaded archive after extraction |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition when partition_type='dynamic'. |

<!-- FIELDS:END -->
