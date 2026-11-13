# Sitemap Crawler Ingestion

Plain, free, no-paid-API HTTP/sitemap-based web crawler for RAG pipelines -- rounds out the "web crawl" family alongside `firecrawl_ingestion`, `jina_reader_ingestion`, and `tavily_ingestion`, which all hit a paid third-party crawl API. This component only ever makes direct HTTP requests with the Python standard library + `requests` + `beautifulsoup4` -- no API key, no account, no cost.

Two discovery modes (exactly one required):

1. **`sitemap_url`** -- fetch a `sitemap.xml`, parsed with the standard library's `xml.etree.ElementTree`. Handles both shapes the sitemap protocol defines: a leaf sitemap (`<urlset><url><loc>...</loc><lastmod>...</lastmod></url></urlset>`) and a sitemap index (`<sitemapindex><sitemap><loc>...</loc></sitemap></sitemapindex>`) that itself lists child sitemap URLs -- followed one level deep. `max_pages`/`max_depth` do not apply in this mode.
2. **`start_urls`** -- one or more seed URLs, crawled breadth-first by following same-domain `<a href>` links out of each fetched page, up to `max_pages` total pages and `max_depth` hops from the nearest seed URL.

`include_patterns`/`exclude_patterns` (comma-separated **substrings**, not regexes) scope either mode to part of a site, e.g. `include_patterns: "/docs/"`.

`respect_robots_txt` (default `true`) checks each domain's `robots.txt` via the standard library's `urllib.robotparser.RobotFileParser` before fetching any page; a missing or unreachable `robots.txt` is treated as allow-all with a logged warning. `request_delay_seconds` sleeps between fetches for politeness, and `user_agent` defaults to a string identifying this project rather than impersonating a browser.

Every fetched page's content is extracted the same way `document_corpus`'s own `.html` branch does: `<script>`/`<style>` tags are stripped, then BeautifulSoup's `.get_text()` extracts the body text and `<title>` the page title.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `sitemap_url` | optional | Sitemap (or sitemap index) URL to crawl exhaustively. Required unless `start_urls` is set. |
| `start_urls` | optional | Comma-separated seed URL(s) to crawl breadth-first. Required unless `sitemap_url` is set. |
| `max_pages` | optional (default `100`) | Max total pages in plain-crawl mode. Ignored when `sitemap_url` is set. |
| `max_depth` | optional (default `3`) | Max link-following hops from a seed URL in plain-crawl mode. Ignored when `sitemap_url` is set. |
| `include_patterns` / `exclude_patterns` | optional | Comma-separated substring filters applied to every candidate URL, in both modes. |
| `respect_robots_txt` | optional (default `true`) | Check and honor each domain's robots.txt. |
| `request_delay_seconds` | optional (default `0.5`) | Seconds to sleep between page fetches. |
| `user_agent` | optional | User-Agent header sent with every request. |

Exactly one of `sitemap_url` / `start_urls` is required -- enforced at config-resolution time, not just at runtime.

## Output shape

One row per crawled page, following the document source contract shared with every other document-source ingestion component: `doc_id`, `content`, `title`, `url`, `source_type` (`"sitemap_crawler"`), `updated_at` (the sitemap's `<lastmod>` when available, else the fetch timestamp), `content_hash` (sha256 of `content`). Point `document_chunker`'s `upstream_asset_key` at this asset, `source_column` at `content`, and (optionally) `metadata_columns` at `url,title,updated_at`.

## Re-running without duplicating pages

`write_disposition` (default `"replace"`) controls what a rerun does to the destination table: `"replace"` (default) makes each run's crawl fully replace the table -- the right default for a crawler, since dlt's own default (`"append"`) has no notion of a previous crawl's pages going stale and keeps adding another full copy of every page on top of the last. `"merge"` upserts keyed on `doc_id` (the page URL) instead, so pages from a prior run that this run's crawl didn't revisit (e.g. `max_pages` capped this run short) are kept rather than dropped. `"append"` is the old behavior, kept as an explicit opt-in for a deliberate historical log of every crawl.

As a safety net independent of `write_disposition`, the returned DataFrame is always deduplicated by `doc_id`, keeping the row with the latest `updated_at` -- this catches stale rows regardless of mode (including `"append"`'s intentional destination-level accumulation, which the DataFrame output never shows). Run metadata reports `write_disposition`, `load_id`, `pages_crawled` (pages this run fetched/reused), `rows_returned` (the deduplicated row count), and `duplicate_rows_removed` (how many the safety net dropped -- nonzero with `"append"`, or after a sitemap lists the same URL twice).

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset that will hold the data |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Asset description |
| `group_name` | `str` | `"sitemap_crawler_ingestion"` | Asset group for organization |
| `owners` | `List[str]` | — | Asset owners -- list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'docs', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. Auto-inferred from destination and asset name if not set. |
| `deps` | `List[str]` | — | Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset']) |
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | Backfill execution strategy for a partitioned asset. 'single_run' materializes an entire backfill range in one run. 'multi_run' launches one run per partition (optionally capped via backfill_policy_max_partitions_per_run… _(full docs in schema.json + component README)_ |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'chunk_text': ['content']}. The upstream asset is `upstream_asset_key`, or the only… _(full docs in schema.json + component README)_ |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'multi', 'dynamic', or None for unpartitioned. |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on Dagster's TimeWindowPartitionsDefinition(cron_schedule=...). |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule. |
| `partition_end_offset` | `int` | `0` | Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition. 0 (the Dagster default) means a time-based partition set only ever holds COMPLETED windows -- e.g. a daily set never includes today… _(full docs in schema.json + component README)_ |
| `partition_timezone` | `str` | — | IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default). |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static or multi partitioning, e.g. 'docs,blog,changelog'. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `write_disposition` | `Literal['replace', 'append', 'merge']` | `"replace"` | dlt write disposition for the destination table. 'replace' (default): each run's crawl fully replaces the table. dlt's own default is 'append', which has no notion of a previous crawl's pages becoming stale -- every re-r… _(full docs in schema.json + component README)_ |
| `bucket_url` | `str` | — | Bucket/path URL for filesystem-shaped storage (e.g. 's3://my-bucket/path', 'gs://my-bucket/path', 'az://my-container/path', or 'file:///local/path'). Required when destination='filesystem' (the final write target). Also… _(full docs in schema.json + component README)_ |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `sitemap_url` | `str` | — | URL of a sitemap.xml (leaf <urlset>, or a <sitemapindex> listing child sitemaps -- followed one level deep) to crawl exhaustively. Every <loc> URL is fetched; max_pages/max_depth do not apply. Required unless start_urls… _(full docs in schema.json + component README)_ |
| `start_urls` | `str` | — | Comma-separated seed URL(s) to crawl breadth-first, following same-domain <a href> links up to max_pages/max_depth. Required unless sitemap_url is set. |
| `max_pages` | `int` | `100` | Max total pages to fetch in plain-crawl mode (start_urls). Ignored (with a logged warning if left non-default) when sitemap_url is set -- a sitemap declares its own complete URL list. |
| `max_depth` | `int` | `3` | Max link-following hops from the nearest seed URL in plain-crawl mode (start_urls). Ignored (with a logged warning if left non-default) when sitemap_url is set. |
| `include_patterns` | `str` | — | Comma-separated substrings (not regexes -- see component docstring for why) a candidate URL must contain at least one of, to be fetched. Applied in both modes, e.g. '/docs/' to scope a crawl or sitemap to one section of a site. |
| `exclude_patterns` | `str` | — | Comma-separated substrings; a candidate URL containing any of these is skipped. Applied after include_patterns, in both modes. |
| `respect_robots_txt` | `bool` | `true` | Check each domain's robots.txt (via urllib.robotparser) before fetching any page, skipping disallowed URLs. Set False only when you have explicit permission to crawl a site regardless of its robots.txt. |
| `request_delay_seconds` | `float` | `0.5` | Seconds to sleep between successive page fetches -- politeness, avoid hammering a site. |
| `user_agent` | `str` | `"dagster-community-components-sitemap-crawler/1.0"` | User-Agent header sent with every request. Defaults to a string identifying this project (not a browser impersonation) -- change it to identify your own deployment if a site's robots.txt or access logs need that. |
| `skip_unchanged` | `bool` | `false` | Persist a per-URL {etag, last_modified, content_hash} fingerprint to state_path between runs. On the next run, each known URL is fetched with If-None-Match/If-Modified-Since from its stored etag/last_modified; a 304 resp… _(full docs in schema.json + component README)_ |
| `state_path` | `str` | — | Where skip_unchanged persists its per-URL fingerprint state (a JSON file). Defaults to './{asset_name}_crawler_state.json'. Ignored when skip_unchanged=False. |
| `hash_columns` | `List[str]` | — | Column names to pseudonymize (salted SHA-256, hex-encoded) before the data reaches the destination -- same deterministic-hashing pattern dlt's own docs recommend for PII (https://dlthub.com/docs/general-usage/customising… _(full docs in schema.json + component README)_ |
| `hash_salt` | `str` | — | Salt for hash_columns. Required (this or hash_salt_env_var) when hash_columns is set -- a hardcoded/shared salt would let the hash be reversed by dictionary attack on common values. Use "{{ env.VAR_NAME }}" for the inlin… _(full docs in schema.json + component README)_ |
| `hash_salt_env_var` | `str` | — | Alternative to hash_salt: name of an env var holding the salt. Resolved at run-time. |
| `on_schema_change` | `Literal['allow_all', 'allow_new_columns', 'block_all']` | `"allow_all"` | What to do when incoming data's shape differs from the destination table's established schema (new columns, or a column's data type changing). 'allow_all' (default, today's behavior): accept any change. 'allow_new_column… _(full docs in schema.json + component README)_ |
| `exclude_columns` | `List[str]` | — | Column names to drop from every row before it reaches the destination. Applied before hash_columns (so a column in both ends up excluded, not hashed-then-excluded). A column missing from a given row is skipped, not an er… _(full docs in schema.json + component README)_ |
| `max_rows_per_resource` | `int` | — | Caps how many crawled-page rows load into the destination. Unset (default): no cap. This still loads real rows into the real destination -- it is not a dry run or a preview -- meant for a first run to check a connection… _(full docs in schema.json + component README)_ |
| `destination` | `str` | — | dlt destination identifier (e.g. 'snowflake', 'bigquery', 'postgres', 'redshift', 'filesystem', 'duckdb', 'databricks', 'athena', 'clickhouse', 'mssql', 'motherduck'). Leave empty for in-memory DuckDB -> DataFrame mode. |
| `dataset_name` | `str` | — | Target dataset/schema in the destination. Defaults to the asset name. |
| `persist_only` | `bool` | `false` | If True with destination set: emit a MaterializeResult and skip DataFrame return. If False: query the destination back into a DataFrame (only meaningful for SQL destinations -- non-SQL destinations always emit MaterializeResult). |
| `destination_credentials_url` | `str` | — | Inline connection string passed to dlt's destination factory. Useful when one Dagster project ingests into multiple accounts of the same destination type. If unset, dlt resolves credentials from env vars -- see ../DESTINATIONS.md. |
| `destination_credentials_env_var` | `str` | — | Alternative to destination_credentials_url: name of an env var holding the connection string. Resolved at run-time. |
| `athena_query_result_bucket` | `str` | — | Optional S3 path where Athena writes query results (e.g. 's3://my-bucket/results/'). Only used when destination='athena'. May be omitted to use Athena-managed query results instead. |
| `include_preview_metadata` | `bool` | `true` | Include sample data preview in metadata |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when `include_preview_metadata` is True. For long DataFrames (>10x preview_rows), a random sample is used so the preview reflects the data distribution; otherwise head() is used. |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run). |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'sites'. |

[//]: # (FIELDS:END)

## Example

```yaml
type: dagster_community_components.SitemapCrawlerIngestionComponent
attributes:
  asset_name: docs_pages
  sitemap_url: "https://docs.example.com/sitemap.xml"
```
