# dlt Load Collection (Advanced)

> **Advanced / escape-hatch component.** If a dedicated hand-built component for your vendor already exists in this catalog (search `manifest.json` or the Designer UI), use that one instead -- it has typed fields and committed tests pinned to that vendor's real, verified API shape. Use `dlt_load_collection` for the long tail: a generic SQL database, a generic REST API, or a generic set of files in a bucket, with no custom source vendor behind them.

Declare a collection of dlt resources (tables / REST endpoints / file globs) entirely in YAML -- no custom Python module required -- across three of dlt's own built-in generic source engines:

| `source_type` | dlt engine | Resource shape |
|---|---|---|
| `sql_database` | `dlt.sources.sql_database.sql_table` (one call per resource) | `{name, primary_key?, columns?, schema?}` |
| `rest_api` | `dlt.sources.rest_api.rest_api_source` | `{name, primary_key?, columns?, endpoint: {path, ...}}` (same shape as `dlt_rest_api_source`) |
| `filesystem` | `dlt.sources.filesystem.filesystem` + a csv/parquet/jsonl reader | `{name, primary_key?, columns?, file_glob, file_format?}` |

This is the sibling to `integrations/dlt_pipeline_assets` (which requires a hand-written `@dlt.source` function in a separate module this component imports by dotted path) for the subset of dlt sources that are themselves pure config, not custom code.

## Relationship to `dlt_rest_api_source`

`assets/ingestion/dlt_rest_api_source` is also available -- a REST-API-only, single-asset-only bring-your-own `client`/`resources` config component. This component's `source_type: rest_api` mode accepts the same `client`/`resources` shape, but additionally supports `asset_shape: multi_asset` and `table_name_map`, and the same declarative shape also covers `sql_database` and `filesystem` sources. The two components currently coexist in this catalog.

## asset_shape

- `single` (default, matches every other multi-table ingestion component in this catalog): one asset that unions every resource's rows into one DataFrame, tagged with a `_resource_type` column.
- `multi_asset`: one independently-materializable asset per resource, named `{asset_name}_{resource_name}`. Materializing one resource never builds, fetches, or (for `rest_api`) even requests the others -- each dlt resource object is only constructed for resources selected in the current run.

## table_name_map

Overrides the destination table name per resource, keyed by the resource's declared `name`:

```yaml
table_name_map:
  customers: raw_customers   # other resources keep their declared name as the destination table
```

## write_disposition (idempotent re-runs)

dlt's own default is `append` -- with no `primary_key` and no explicit `write_disposition`, every materialization appends, so re-materializing the same partition **duplicates rows** (2 materializations of 5 source rows produce 10 rows). **This component infers `merge` automatically whenever a resource declares `primary_key`** (a natural key implies you want dedup-on-rerun); set `write_disposition` explicitly to override that inference:

```yaml
resources:
  - name: orders
    primary_key: order_id   # -> write_disposition is inferred as 'merge'
  - name: events
    primary_key: event_id
    write_disposition: append   # explicit override: keep appending despite the key
  - name: snapshot_table
    write_disposition: replace   # no natural key -- full-refresh every run instead
```

`write_disposition: merge` needs a dedup key -- either `primary_key` or `merge_key` (str or list of str); setting `merge` with neither raises a clear error at `build_defs` time instead of silently falling back to appending (verified: 2 loads of the same 2 rows produced 4 with no key set). Works the same way across all three `source_type` values -- passed to `sql_table(write_disposition=..., merge_key=...)`, `DltResource.apply_hints(write_disposition=..., merge_key=...)` (filesystem), or straight through into the `rest_api_source` resource config (`EndpointResource.write_disposition`/`merge_key`).

## Custom per-resource metadata (`x_` prefix)

A `resources:` entry can carry arbitrary extra keys for a subclass's own use (e.g. a blocking rows-landed check or a column-contract check reading `min_rows`/`contract_columns` off the YAML) by prefixing them with `x_`:

```yaml
resources:
  - name: orders
    endpoint: { path: orders }
    x_min_rows: 10
    x_contract_columns: [order_id, customer_id, amount]
```

`x_`-prefixed keys are guaranteed to never reach the underlying dlt call (required for `rest_api`, whose `EndpointResource` validation rejects any key it doesn't recognize; `sql_database`/`filesystem` already ignore unrecognized keys harmlessly, but the convention is recommended everywhere for consistency). A subclass reads them directly off `self.resources` -- this component does nothing with them beyond guaranteeing they're stripped before any dlt call.

## Per-resource asset metadata and asset_key (multi_asset mode only)

In `asset_shape: multi_asset`, each `resources:` entry can set its own `description`, `owners`, `tags` (merged over the component-level `asset_tags`), `metadata` (static `AssetOut` metadata), and `asset_key` (str, or a list of str for a multi-segment key) -- overriding the default single-segment `{asset_name}_{resource_name}` key:

```yaml
resources:
  - name: customers
    endpoint: { path: customers }
    description: "Customer records"
    owners: ["team:crm"]
    tags: { pii: "true" }
    asset_key: ["crm", "customers"]   # instead of the default my_api_tables_customers
```

These keys have no effect in `asset_shape: single` -- there's only one asset, so there's no per-resource asset identity to set them on.

## Overridable seams (subclassing)

`_build_resources(self, selected_names=None, run_context=None)` and `_build_pipeline(self, pipeline_name, dataset_name)` are real methods, not closures trapped inside `build_defs` -- a subclass can override either (e.g. to force a demo-mode pipeline regardless of the configured `destination`, or to add a resource kind this component doesn't natively support) without reimplementing the rest of the component. `_resolve_destination`/`_resolve_destination_with_bucket`/`_resolve_staging`/`_dest_table_names` are the other reachable seams.

## Secrets

- `sql_database`: `connection_string` (or `connection_string_env_var` for an env-var-name indirection) takes any SQLAlchemy connection string. Use `"{{ env('VAR_NAME') }}"` for the inline form.
- `rest_api`: same `client`/`resources` secret convention as `dlt_rest_api_source` -- `{{ env.VAR_NAME }}` anywhere inside `client:`/`resources:`, resolved by dg's component-YAML loader before this component ever sees it.
- `filesystem`: `bucket_url` credentials resolve from the cloud provider's standard environment variables (e.g. `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` for S3), same as the destination-side filesystem convention in `../DESTINATIONS.md`.

## Validation

Fails fast with a clear message at `Definitions`-build time, before this ever reaches dlt's own (less friendly) validation:

- `resources` must be a non-empty list; every entry needs `name`; resource names must be unique.
- `source_type='rest_api'`: every resource needs `endpoint.path`.
- `source_type='filesystem'`: every resource needs `file_glob`.
- `source_type='sql_database'` requires `connection_string`/`connection_string_env_var`; `rest_api` requires `client`; `filesystem` requires `bucket_url`.
- `table_name_map` keys not present in `resources` raise a clear error naming the offending key(s).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination (snowflake, bigquery, postgres, filesystem, etc.). See `../DESTINATIONS.md` -- note the destination-side filesystem bucket field is named `destination_bucket_url` here (not the shared `bucket_url`, which is taken by the filesystem SOURCE field for `source_type='filesystem'`).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset (or asset prefix, in multi_asset mode) that will hold the data |
| `source_type` | required | `sql_database` \| `rest_api` \| `filesystem` |
| `resources` | required | List of `{name, primary_key?, columns?, ...}` dicts -- see the table above for per-source_type required keys |

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset (or asset prefix, in multi_asset mode) that will hold the data |
| `source_type` | `Literal['sql_database', 'rest_api', 'filesystem']` | Which dlt built-in generic source engine builds these resources: 'sql_database' (dlt.sources.sql_database.sql_table, one call per declared resource), 'rest_api' (dlt.sources.rest_api.rest_api_source, same config shape as… _(full docs in schema.json + component README)_ |
| `resources` | `List[Dict[str, Any]]` | One dict per resource (table / endpoint / file-glob). Always requires 'name' (also the destination table name, unless overridden via table_name_map). Optional 'primary_key' (str or list of str) and 'columns' on every sou… _(full docs in schema.json + component README)_ |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `connection_string` | `str` | — | SQLAlchemy connection string for the source database, e.g. 'postgresql://user:pass@host:5432/db'. Required when source_type='sql_database' (or connection_string_env_var). Use "{{ env('VAR_NAME') }}" for secrets. |
| `connection_string_env_var` | `str` | — | Alternative to connection_string: name of an env var holding the SQLAlchemy connection string. Resolved at run-time. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Asset description |
| `group_name` | `str` | `"dlt_load_collection"` | Asset group for organization |
| `owners` | `List[str]` | — | Asset owners -- list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog. Auto-inferred from source_type/destination if not set. |
| `deps` | `List[str]` | — | Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset']) |
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | Backfill strategy for a partitioned asset. Unset keeps Dagster's default. |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'multi', 'dynamic', or None for unpartitioned. |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. |
| `partition_end_offset` | `int` | `0` | Passed through to the underlying time-based partitions definition. 0 (default) excludes the current, still-in-progress window. |
| `partition_timezone` | `str` | — | IANA timezone for the underlying partitions definition (defaults to UTC). |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static or multi partitioning. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec. Overrides flat fields when set. |
| `partition_date_column` | `Union[str, int]` | — | Column used to filter the returned DataFrame to the current date partition (time partitions). |
| `partition_static_column` | `Union[str, int]` | — | Column used to filter the returned DataFrame to the current static/dynamic partition key. |
| `partition_window_params` | `Dict[str, str]` | — | source_type='rest_api' only: push the partition's time window into the API requests so each partition fetches only its own slice: {start_param: ..., end_param: ..., format: iso \| date \| epoch \| epoch_ms}. Ignored (wit… _(full docs in schema.json + component README)_ |
| `partition_window_resources` | `List[str]` | — | Resources that take partition_window_params (default: all of them, rest_api only). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `table_name_map` | `Dict[str, str]` | — | Override the destination table name for specific resources, keyed by the resource's declared 'name': {resource_name: destination_table_name}. Resources not listed keep their declared name as the destination table name. |
| `bucket_url` | `str` | — | Bucket/path URL to read SOURCE files from (e.g. 's3://my-bucket/landing', 'gs://my-bucket/landing', 'az://my-container/landing', or 'file:///local/path'). Required when source_type='filesystem'. Credentials resolve from… _(full docs in schema.json + component README)_ |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_shape` | `Literal['single', 'multi_asset']` | `"single"` | 'single' (default, matches every other multi-table ingestion component in this repo): one asset that unions every resource's rows into one DataFrame, tagged with a `_resource_type` column. 'multi_asset': one independentl… _(full docs in schema.json + component README)_ |
| `schema_name` | `str` | — | Source database schema name (sql_database only). Per-resource 'schema' overrides this. |
| `sql_backend` | `Literal['sqlalchemy', 'pyarrow', 'pandas', 'connectorx']` | `"sqlalchemy"` | dlt sql_table extraction backend (sql_database only). |
| `client` | `Dict[str, Any]` | — | dlt rest_api_source 'client' config (base_url required; optional auth, headers, paginator -- see dlt_rest_api_source's README for the full shape reference). Required when source_type='rest_api'. |
| `file_format` | `Literal['csv', 'parquet', 'jsonl']` | `"csv"` | Reader applied to each matched file (filesystem only). Per-resource 'file_format' overrides this. |
| `destination` | `str` | — | dlt destination identifier (e.g. 'snowflake', 'bigquery', 'postgres', 'redshift', 'filesystem', 'duckdb', 'databricks', 'athena', 'clickhouse', 'mssql', 'motherduck'). Leave empty for in-memory DuckDB -> DataFrame mode. |
| `dataset_name` | `str` | — | Target dataset/schema in the destination. Defaults to the asset name. |
| `persist_only` | `bool` | `false` | If True with destination set: emit a MaterializeResult and skip DataFrame return. If False: query the destination back into a DataFrame (only meaningful for SQL destinations -- non-SQL destinations always emit MaterializeResult). |
| `destination_credentials_url` | `str` | — | Inline connection string passed to dlt's destination factory. Useful when one Dagster project ingests into multiple accounts of the same destination type. If unset, dlt resolves credentials from env vars -- see ../DESTINATIONS.md. |
| `destination_credentials_env_var` | `str` | — | Alternative to destination_credentials_url: name of an env var holding the connection string. Resolved at run-time. |
| `destination_bucket_url` | `str` | — | Bucket/path URL for filesystem-shaped DESTINATION storage (this is the field named 'bucket_url' in every other ingestion component in this repo -- renamed here because 'bucket_url' is the filesystem SOURCE field above)… _(full docs in schema.json + component README)_ |
| `athena_query_result_bucket` | `str` | — | Optional S3 path where Athena writes query results. Only used when destination='athena'. |
| `include_preview_metadata` | `bool` | `true` | Include sample data preview in metadata |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when include_preview_metadata is True. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (partition_type='dynamic'). |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only: maximum partitions materialized per run (default 1). |

[//]: # (FIELDS:END)

## Examples

### 1. sql_database

```yaml
type: dagster_community_components.DltLoadCollectionComponent
attributes:
  asset_name: warehouse_core_tables
  source_type: sql_database
  connection_string: "{{ env('WAREHOUSE_CONNECTION_STRING') }}"
  schema_name: public
  resources:
    - name: customers
      primary_key: id
    - name: orders
      primary_key: order_id
      columns: [order_id, customer_id, amount, created_at]
  asset_shape: multi_asset
```

### 2. rest_api

```yaml
type: dagster_community_components.DltLoadCollectionComponent
attributes:
  asset_name: my_api_tables
  source_type: rest_api
  client:
    base_url: "https://api.example.com"
    auth:
      type: bearer
      token: "{{ env('MY_API_TOKEN') }}"
  resources:
    - name: customers
      primary_key: id
      endpoint:
        path: customers
  table_name_map:
    customers: raw_customers
```

### 3. filesystem

```yaml
type: dagster_community_components.DltLoadCollectionComponent
attributes:
  asset_name: landing_zone_files
  source_type: filesystem
  bucket_url: "s3://my-bucket/landing"
  resources:
    - name: orders
      file_glob: "orders/*.csv"
      primary_key: order_id
    - name: events
      file_glob: "events/*.parquet"
      file_format: parquet
```

See `example.yaml` for the rest_api shape as a runnable file (sql_database/filesystem variants commented out, ready to uncomment).

## Partitioning

When `partition_type` is set, set `partition_date_column` (time partitions) or `partition_static_column` (static/dynamic/multi partitions) and each run returns only the rows in its partition; a backfill run covers its whole range. For `source_type='rest_api'` only, `partition_window_params` pushes the partition's time window into the API requests (same mechanism as `dlt_rest_api_source`) so each partition fetches only its own slice -- ignored (with a warning) for `sql_database`/`filesystem`. `backfill_policy` (`single_run` / `multi_run`) is opt-in; `single_run` needs a partition-range-aware IO manager.
