# BigQuery Query Asset

Run a SQL query against Google BigQuery and return a pandas DataFrame. Drop-in peer of `duckdb_query_reader` (DuckDB) and `database_query` (generic SQL).

For the heavier "import every BigQuery entity as a Dagster asset" pattern (scheduled queries, materialized views, transfer jobs, etc.), use `google_bigquery` instead.

## Required packages

```
dagster>=1.8.0
pandas>=1.5.0
google-cloud-bigquery>=3.0.0
google-auth>=2.0.0
db-dtypes>=1.0.0
```

## Setup

1. **Enable BigQuery API** on the SA's project. Component surfaces the activation URL on first call if not enabled.
2. **Grant the SA at least these roles** at project level:
   - `roles/bigquery.dataViewer` — read tables
   - `roles/bigquery.jobUser` — submit queries
   - (or `roles/bigquery.user` which bundles both)
   
   Grant via [GCP IAM](https://console.cloud.google.com/iam-admin/iam): IAM → +Grant access → paste SA email → add roles → Save.

3. Public datasets like `bigquery-public-data.samples.shakespeare` work with just `roles/bigquery.jobUser` — billing goes to the SA's project, not the public-data project.

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `asset_name` | yes | — | Output asset name |
| `credentials` / `credentials_path` | no | — | SA auth, falls back to `GOOGLE_APPLICATION_CREDENTIALS` |
| `project_id` | no | SA's project | Billing project for the BQ job |
| `location` | no | auto-detect | BigQuery location (`US`, `EU`, `us-central1`, etc.) |
| `query` | yes | — | SQL string. Supports `{placeholder}` from `query_params`. |
| `query_params` | no | `{}` | Mapping substituted into placeholders before submission. `partition_key` is auto-added when the asset is partitioned. |
| `use_legacy_sql` | no | `false` | |
| `dry_run` | no | `false` | Validate + report byte estimate without scanning data |
| `max_rows` | no | — | Hard cap on result rows |
| Standard Dagster attrs | | | |

## Output metadata

| Key | Type | Notes |
|---|---|---|
| `row_count` | int | |
| `column_count` | int | |
| `bytes_processed` | int | What the query scanned |
| `bytes_billed` | int | What you'll be charged for (≥ `bytes_processed`) |
| `cache_hit` | bool | True if BQ served from query cache |
| `slot_millis` | int | Compute time spent |
| `preview` | markdown | First 10 rows |
| `rendered_query` | str | The query post-placeholder substitution |

## Example — public Shakespeare data

```yaml
type: dagster_component_templates.BigQueryQueryAssetComponent
attributes:
  asset_name: top_shakespeare_words
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  query: |
    SELECT word, SUM(word_count) AS occurrences
    FROM `bigquery-public-data.samples.shakespeare`
    GROUP BY word
    ORDER BY occurrences DESC
    LIMIT {top_n}
  query_params:
    top_n: 20
  group_name: warehouse
```

## Example — partitioned

`partition_key` is auto-injected into `query_params`:

```yaml
attributes:
  asset_name: daily_orders
  partition_type: daily
  partition_start: "2026-04-01"
  query: |
    SELECT customer_id, SUM(amount) AS total
    FROM `my-project.analytics.orders`
    WHERE DATE(created_at) = '{partition_key}'
    GROUP BY customer_id
```

When materializing partition `2026-05-01`, the query becomes `WHERE DATE(created_at) = '2026-05-01'`.

## Dry-run cost preview

Set `dry_run: true` to validate the query and report `bytes_processed` + an estimated USD cost (assuming \$5/TB on-demand pricing), without scanning data. Useful in CI / linting.

## Sister components

- `duckdb_query_reader` — same shape, against a local DuckDB.
- `database_query` — generic SQL against any SQLAlchemy-supported database.
- `external_bigquery_table` — declare-only external asset for a BQ table (no data loaded).
- `google_bigquery` — multi-asset: imports BQ entities (scheduled queries, materialized views, etc.) as Dagster assets.
