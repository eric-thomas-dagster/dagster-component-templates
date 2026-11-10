# ServiceNowIngestionComponent

Read records from one or more ServiceNow tables (incident, change_request, sc_request, sys_user, cmdb_ci, custom tables — `tables` is a comma-separated list) using dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`). ServiceNow has no official dlt-maintained verified source, so this uses the same config-driven REST connector pattern as `workday_ingestion` and the ad-platform ingestion components in this repo. ServiceNow's Table API pagination (`sysparm_offset` + `sysparm_limit`, stop on an empty page) maps onto dlt's `OffsetPaginator`.

`asset_shape` picks how multiple tables show up in Dagster:

- **`single`** (default, matches every other multi-table ingestion component in this repo, e.g. `salesforce_ingestion`/`zendesk_ingestion`): one asset, keyed `asset_name`, covering every table as a single unit — rows from every table are unioned into one returned DataFrame tagged by a `_table` column. Pick this when you just want to monitor "did the ServiceNow pipeline run," not select or retry individual tables.
- **`multi_asset`**: each table is instead its own **independently-materializable asset**, keyed `{asset_name}_{table}` (`@multi_asset(can_subset=True)`) — materializing one table does not fetch or persist the others. Materializing several together still batches them through one shared dlt pipeline call (one Dagster step), not parallel per-table execution.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination (Snowflake, BigQuery, Postgres, filesystem, etc.) instead — see `../DESTINATIONS.md`. Each table lands at `{dataset_name}.{table}` by default (the same convention every ingestion component in this repo relies on, e.g. `salesforce_ingestion`); set `table_name_map` to rename specific destination tables (e.g. `{incident: raw_incidents}`).

Auth is either direct env vars (`instance_env_var` + `username_env_var`/`password_env_var`, or `instance_env_var` + `bearer_token_env_var`), or a shared `resource_key` pointing at a `ServiceNowResourceComponent` — the same resource `servicenow_record_upsert` and `servicenow_sensor` use, so one set of credentials can serve all three.

## Use cases

- Pull last 90 days of incidents → warehouse for service-quality / MTTR analytics
- Snapshot the CMDB into a data product downstream of asset discovery
- Sync change-management activity for compliance / audit reporting
- Materialize a custom ServiceNow table for downstream Dagster transforms
- Persist straight into Snowflake/BigQuery/Postgres/etc. via `destination`, skipping a separate warehouse-load step

## Dependencies

- `dlt>=1.0.0`
- `pandas>=1.5.0`
- `requests>=2.28.0`
- `tabulate>=0.9.0`

## Configuration

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | `str` | Yes | — | Output asset name |
| `instance_env_var` | `str` | Yes | — | Env var with the instance subdomain (just `mycompany`, not `https://...`) |
| `username_env_var` | `str` | Conditional | — | Required unless `bearer_token_env_var` is set |
| `password_env_var` | `str` | Conditional | — | Required unless `bearer_token_env_var` is set |
| `bearer_token_env_var` | `str` | Conditional | — | OAuth bearer token. When set, basic-auth fields are ignored |
| `tables` | `str` | Yes | — | Comma-separated ServiceNow table names (`incident`, `change_request`, `sc_request`, `sys_user`, `cmdb_ci`, custom) |
| `asset_shape` | `str` | No | `"single"` | `"single"` = one asset covering every table, unioned with a `_table` column; `"multi_asset"` = one independently-materializable asset per table |
| `table_name_map` | `dict` | No | — | Override the destination table name for specific tables, e.g. `{incident: raw_incidents}`. Unlisted tables keep their ServiceNow name |
| `sysparm_query` | `str` | No | — | ServiceNow encoded query (e.g. `state=approved^priority=1`) |
| `sysparm_fields` | `str` | No | — | Comma-separated field list. Defaults to all fields |
| `sysparm_display_value` | `str` | No | `"false"` | `"false"` = sys_id only, `"true"` = display only, `"all"` = both |
| `page_size` | `int` | No | `1000` | Records per API call (`sysparm_limit`) |
| `max_records` | `int` | No | — | Cap on total records returned |
| `verify_ssl` | `bool` | No | `true` | Disable only for self-signed dev instances |

## Common ServiceNow tables

| Table | Returns |
|---|---|
| `incident` | All incidents |
| `change_request` | Change requests |
| `change_task` | Tasks within change requests |
| `problem` | Problem records |
| `sc_request` | Service Catalog requests (REQ) |
| `sc_req_item` | Service Catalog request items (RITM) |
| `sys_user` | Users |
| `sys_user_group` | Groups |
| `cmdb_ci` | Configuration items (CMDB) |
| `cmdb_ci_server` | CI Servers |
| `kb_knowledge` | Knowledge base articles |
| `task` | Top-level Task table (parent of incidents, changes, etc.) |
| `<x_company_table>` | Custom (scoped) tables |

## ServiceNow encoded queries

`sysparm_query` is ServiceNow's query syntax — operators joined by `^`:

- `state=1` — exact match
- `priority<=2` — comparisons
- `state=1^priority=1` — AND
- `state=1^ORstate=2` — OR  
- `sys_created_on>=javascript:gs.daysAgoStart(30)` — last 30 days
- `assigned_to.user_name=alice` — dot-walk to referenced fields
- `assigned_toISEMPTY` — null check
- `short_descriptionLIKEdatabase` — substring match

See <https://www.servicenow.com/docs/bundle/yokohama-platform-user-interface/page/use/common-ui-elements/reference/r_OpAvailableFiltersQueries.html> for the full operator list.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset to create |
| `tables` | `str` | Comma-separated list of ServiceNow tables to query (e.g. 'incident,change_request,sys_user,cmdb_ci'). With asset_shape='single' (default), all tables are fetched together under one asset keyed `asset_name`. With asset_sh… _(full docs in schema.json + component README)_ |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `username_env_var` | `str` | — | Env var with ServiceNow username (basic auth). Required unless resource_key or bearer_token_env_var is set. |
| `password_env_var` | `str` | — | Env var with ServiceNow password (basic auth). Required unless resource_key or bearer_token_env_var is set. |
| `resource_key` | `str` | — | Optional resource key registered by a ServiceNowResourceComponent. When set, credentials (instance/username/password/bearer_token) are read from that resource at run time instead of instance_env_var/username_env_var/pass… _(full docs in schema.json + component README)_ |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `page_size` | `int` | `1000` | Records fetched per API call (sysparm_limit). Default 1000; ServiceNow's max recommended is 10000. |
| `max_records` | `int` | — | Cap on total records returned across all pages. None (default) = no cap. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `deps` | `List[str]` | — | Upstream asset keys for lineage |
| `group_name` | `str` | — | Asset group for organization |
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | Backfill strategy for a partitioned asset. 'single_run' materializes a whole backfill range in one run (rows are filtered to the whole range); 'multi_run' launches one run per partition, optionally capped by backfill_pol… _(full docs in schema.json + component README)_ |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from destination/component name if not set. |
| `description` | `str` | — | Asset description shown in the Dagster catalog. |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on Dagster's TimeWindowPartitionsDefinition(cron_schedule=...). |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule. |
| `partition_end_offset` | `int` | `0` | Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition. 0 (the Dagster default) means a time-based partition set only ever holds COMPLETED windows -- e.g. a daily set never includes today… _(full docs in schema.json + component README)_ |
| `partition_timezone` | `str` | — | IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default). |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `Union[str, int]` | — | Column used to filter the returned DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `Union[str, int]` | — | Column used to filter the returned DataFrame to the current static/dynamic partition key (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `table_name_map` | `Dict[str, str]` | — | Override the destination table name for specific ServiceNow tables, e.g. {'incident': 'raw_incidents'}. dlt otherwise names each destination table after its ServiceNow table (the default every ingestion component in this… _(full docs in schema.json + component README)_ |
| `bucket_url` | `str` | — | Bucket/path URL for filesystem-shaped storage (e.g. 's3://my-bucket/path', 'gs://my-bucket/path', 'az://my-container/path', or 'file:///local/path'). Required when destination='filesystem' (the final write target). Also… _(full docs in schema.json + component README)_ |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `instance_env_var` | `str` | — | Env var with the ServiceNow instance subdomain (e.g. 'mycompany' or 'dev123456'). Required unless resource_key is set. |
| `bearer_token_env_var` | `str` | — | Env var with ServiceNow OAuth bearer token. When set, basic-auth env vars are ignored. |
| `asset_shape` | `Literal['single', 'multi_asset']` | `"single"` | 'single' (default, matches every other multi-table ingestion component in this repo, e.g. salesforce_ingestion/zendesk_ingestion): one asset, keyed `asset_name`, covering every table in `tables` as a single unit -- rows… _(full docs in schema.json + component README)_ |
| `sysparm_query` | `str` | — | ServiceNow encoded query string filter (e.g. 'state=approved^priority=1'), applied to every table in `tables`. See ServiceNow docs on encoded queries. |
| `sysparm_fields` | `str` | — | Comma-separated list of fields to return, applied to every table in `tables`. Defaults to all fields if unset. |
| `sysparm_display_value` | `str` | `"false"` | Reference field display: 'false' = sys_id only, 'true' = display value only, 'all' = both. Default 'false' (raw). |
| `verify_ssl` | `bool` | `true` | Enable TLS verification. Set false only for self-signed dev instances. Ignored when resource_key is set (the resource's own verify_ssl applies). |
| `hash_columns` | `List[str]` | — | Column names to pseudonymize (salted SHA-256, hex-encoded) before the data reaches the destination -- same deterministic-hashing pattern dlt's own docs recommend for PII (https://dlthub.com/docs/general-usage/customising… _(full docs in schema.json + component README)_ |
| `hash_salt` | `str` | — | Salt for hash_columns. Required (this or hash_salt_env_var) when hash_columns is set -- a hardcoded/shared salt would let the hash be reversed by dictionary attack on common values. Use "{{ env.VAR_NAME }}" for the inlin… _(full docs in schema.json + component README)_ |
| `hash_salt_env_var` | `str` | — | Alternative to hash_salt: name of an env var holding the salt. Resolved at run-time. |
| `on_schema_change` | `Literal['allow_all', 'allow_new_columns', 'block_all']` | `"allow_all"` | What to do when incoming data's shape differs from the destination table's established schema (new columns, or a column's data type changing). 'allow_all' (default, today's behavior): accept any change. 'allow_new_column… _(full docs in schema.json + component README)_ |
| `exclude_columns` | `List[str]` | — | Column names to drop from every row before it reaches the destination. Applied before hash_columns (so a column in both ends up excluded, not hashed-then-excluded). A column missing from a given resource's rows is skippe… _(full docs in schema.json + component README)_ |
| `max_rows_per_resource` | `int` | — | Caps how many rows each resource loads. Unset (default): no cap. This still loads real rows into the real destination -- it is not a dry run or a preview -- meant for a first run to check a connection and the shape of th… _(full docs in schema.json + component README)_ |
| `destination` | `str` | — | dlt destination identifier (e.g. 'snowflake', 'bigquery', 'postgres', 'redshift', 'filesystem', 'duckdb', 'databricks', 'athena', 'clickhouse', 'mssql', 'motherduck'). Leave empty for in-memory DuckDB -> DataFrame mode. |
| `dataset_name` | `str` | — | Target dataset/schema in the destination. Defaults to the asset name. |
| `persist_only` | `bool` | `false` | If True with destination set: emit a MaterializeResult and skip DataFrame return. If False: query the destination back into a DataFrame (only meaningful for SQL destinations -- non-SQL destinations always emit MaterializeResult). |
| `destination_credentials_url` | `str` | — | Inline connection string passed to dlt's destination factory. Useful when one Dagster project ingests into multiple accounts of the same destination type. If unset, dlt resolves credentials from env vars -- see ../DESTINATIONS.md. |
| `destination_credentials_env_var` | `str` | — | Alternative to destination_credentials_url: name of an env var holding the connection string. Resolved at run-time. |
| `athena_query_result_bucket` | `str` | — | Optional S3 path where Athena writes query results (e.g. 's3://my-bucket/results/'). Only used when destination='athena'. May be omitted to use Athena-managed query results instead. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only: maximum partitions materialized per run (default 1). |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output data in metadata (first 5 rows as a markdown table). Used by builder UIs to render asset shape without warehouse access. |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when `include_preview_metadata` is True. For long DataFrames (>10x preview_rows), a random sample is used so the preview reflects the data distribution; otherwise head() is used. |

[//]: # (FIELDS:END)

## Examples

Last 30 days of P1 / P2 incidents:

```yaml
type: dagster_community_components.ServiceNowIngestionComponent
attributes:
  asset_name: high_priority_incidents
  instance_env_var: SNOW_INSTANCE
  username_env_var: SNOW_USERNAME
  password_env_var: SNOW_PASSWORD
  table: incident
  sysparm_query: "priority<=2^sys_created_on>=javascript:gs.daysAgoStart(30)"
  sysparm_fields: number,short_description,priority,state,assigned_to,sys_created_on,resolved_at
  page_size: 1000
  max_records: 50000
```

Open change requests with a specific assignment group, OAuth auth:

```yaml
type: dagster_community_components.ServiceNowIngestionComponent
attributes:
  asset_name: open_data_eng_changes
  instance_env_var: SNOW_INSTANCE
  bearer_token_env_var: SNOW_ACCESS_TOKEN
  table: change_request
  sysparm_query: "state!=closed^assignment_group.name=data_engineering"
```

Full CMDB snapshot (server CIs only):

```yaml
type: dagster_community_components.ServiceNowIngestionComponent
attributes:
  asset_name: cmdb_servers
  instance_env_var: SNOW_INSTANCE
  username_env_var: SNOW_USERNAME
  password_env_var: SNOW_PASSWORD
  table: cmdb_ci_server
  sysparm_display_value: "all"
  page_size: 5000
```

## Pagination

ServiceNow paginates via `sysparm_offset`. This component walks pages until either:

1. The current page returns fewer than `page_size` records (last page), OR
2. `max_records` has been reached.

For very large tables, set `page_size: 5000`–`10000` to reduce request count. ServiceNow recommends 10000 as the practical max.

## Auth

| Mode | When | How |
|---|---|---|
| Basic | Dev instances, PDIs | Set `username_env_var` + `password_env_var` |
| OAuth bearer | Production | Set `bearer_token_env_var`. Pair with `oauth_token_resource` to acquire + rotate the token via OAuth `client_credentials` grant |

The community `oauth_token_resource` component handles the OAuth dance (token acquisition, refresh-token rotation, writeback to AWS Secrets Manager / Azure Key Vault / Vault). Wire it once and point `servicenow_ingestion` (and `servicenow_sensor`) at the resulting env var.

## See also

- `servicenow_resource` — shared connection (instance + auth) for multiple ServiceNow assets in one project
- `servicenow_sensor` — trigger a Dagster job when records match a `sysparm_query`
- `oauth_token_resource` — acquire + rotate bearer tokens for production OAuth
- [Schema](schema.json) · [Example](example.yaml)
