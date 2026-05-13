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

## Security

- Path-traversal protection: any archive entry resolving outside `extract_to` is rejected (defends against "zip-slip" attacks).
- HTTPS verification is on by default; set `verify_ssl: false` only for trusted internal mirrors.

## Pipes well into…

- [`csv_file_ingestion`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/ingestion/csv_file_ingestion) — CSV → DataFrame
- `tsv_file_ingestion`, `excel_file_ingestion`, `json_file_ingestion`
- Any DuckDB / SQLite component that reads files by absolute path
