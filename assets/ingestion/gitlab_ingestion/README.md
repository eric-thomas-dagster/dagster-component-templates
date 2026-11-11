# GitlabIngestionComponent

Read records from one or more GitLab REST API v4 resources (`projects`, `issues`, `merge_requests`, `commits` — `tables` is a comma-separated list) using dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`). GitLab has no official dlt-maintained verified source (confirmed against dlthub's own verified-sources catalog), so this uses the same config-driven REST connector pattern as `servicenow_ingestion` / `workday_ingestion` in this repo. GitLab's own pagination maps onto dlt's `HeaderLinkPaginator`, which follows the standard RFC 5988 `Link` response header (`rel="next"`) — the same mechanism GitHub's API uses.

`asset_shape` picks how multiple tables show up in Dagster:

- **`single`** (default, matches every other multi-table ingestion component in this repo, e.g. `salesforce_ingestion`/`servicenow_ingestion`): one asset, keyed `asset_name`, covering every table as a single unit — rows from every table are unioned into one returned DataFrame tagged by a `_table` column. Pick this when you just want to monitor "did the GitLab pipeline run," not select or retry individual tables.
- **`multi_asset`**: each table is instead its own **independently-materializable asset**, keyed `{asset_name}_{table}` (`@multi_asset(can_subset=True)`) — materializing one table does not fetch or persist the others. Materializing several together still batches them through one shared dlt pipeline call (one Dagster step), not parallel per-table execution.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination (Snowflake, BigQuery, Postgres, filesystem, etc.) instead — see `../DESTINATIONS.md`. Each table lands at `{dataset_name}.{table}` by default (the same convention every ingestion component in this repo relies on); set `table_name_map` to rename specific destination tables (e.g. `{merge_requests: raw_mrs}`).

`issues`, `merge_requests`, and `commits` are all project-scoped endpoints (`GET /projects/:id/...`) and require `project_id` (a numeric ID or a URL-encoded `namespace/project` path). `projects` can instead be scoped to `group_id` (`GET /groups/:id/projects`) or left unscoped (`GET /projects`, every project visible to the token).

## Use cases

- Pull open issues / merge requests across a project → warehouse for engineering-velocity analytics
- Snapshot commit history for a repository for audit / compliance reporting
- List every project in a GitLab group for a catalog of active repositories
- Materialize GitLab activity alongside `github_ingestion` for cross-platform engineering metrics
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
| `access_token_env_var` | `str` | No | — | Env var with a GitLab personal/project/group access token, sent in the `PRIVATE-TOKEN` header. Required for private projects/groups |
| `base_url` | `str` | No | `"https://gitlab.com/api/v4"` | GitLab REST API v4 base URL. Override for a self-managed instance |
| `project_id` | `str` | Conditional | — | Numeric ID or URL-encoded `namespace/project` path. Required when `tables` includes `issues`, `merge_requests`, or `commits` |
| `group_id` | `str` | No | — | Numeric ID or URL-encoded group path. Only affects the `projects` table (`GET /groups/:id/projects`) |
| `tables` | `str` | Yes | — | Comma-separated GitLab resources: `projects`, `issues`, `merge_requests`, `commits` |
| `asset_shape` | `str` | No | `"single"` | `"single"` = one asset covering every table, unioned with a `_table` column; `"multi_asset"` = one independently-materializable asset per table |
| `table_name_map` | `dict` | No | — | Override the destination table name for specific tables, e.g. `{merge_requests: raw_mrs}`. Unlisted tables keep their GitLab name |
| `state` | `str` | No | — | Filter `issues`/`merge_requests` by state (`opened`, `closed`, `merged`, `locked`) |
| `updated_after` | `str` | No | — | Filter `issues`/`merge_requests` to records updated on/after this ISO 8601 timestamp |
| `since` / `until` | `str` | No | — | Filter `commits` to a date range (ISO 8601) |
| `ref_name` | `str` | No | — | Branch/tag/revision range for `commits`. Defaults to the project's default branch |
| `per_page` | `int` | No | `100` | Records per API call — GitLab's documented maximum |
| `max_records` | `int` | No | — | Cap on total records returned per table |
| `verify_ssl` | `bool` | No | `true` | Disable only for self-managed instances with self-signed certs |

## GitLab resources (`tables`)

| Table | Endpoint | Scope | Returns |
|---|---|---|---|
| `projects` | `GET /groups/:id/projects` or `GET /projects` | `group_id` or unscoped | Projects in a group, or every project visible to the token |
| `issues` | `GET /projects/:id/issues` | `project_id` | Issues for a project |
| `merge_requests` | `GET /projects/:id/merge_requests` | `project_id` | Merge requests for a project |
| `commits` | `GET /projects/:id/repository/commits` | `project_id` | Commits on a branch/tag/ref (default branch if `ref_name` unset) |

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset to create |
| `tables` | `str` | Comma-separated list of GitLab resources to query: 'projects', 'issues', 'merge_requests', 'commits'. With asset_shape='single' (default), all tables are fetched together under one asset keyed `asset_name`. With asset_sh… _(full docs in schema.json + component README)_ |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url` | `str` | `"https://gitlab.com/api/v4"` | GitLab REST API v4 base URL. Defaults to GitLab.com SaaS. For a self-managed GitLab instance, set this to '<https://your-gitlab-host>/api/v4' -- the path must still end in '/api/v4'. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `max_records` | `int` | — | Cap on total records returned across all pages, per table. None (default) = no cap. |

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
| `table_name_map` | `Dict[str, str]` | — | Override the destination table name for specific GitLab resources, e.g. {'merge_requests': 'raw_mrs'}. dlt otherwise names each destination table after its GitLab resource name (the default every ingestion component in t… _(full docs in schema.json + component README)_ |
| `bucket_url` | `str` | — | Bucket/path URL for filesystem-shaped storage (e.g. 's3://my-bucket/path', 'gs://my-bucket/path', 'az://my-container/path', or 'file:///local/path'). Required when destination='filesystem' (the final write target). Also… _(full docs in schema.json + component README)_ |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `access_token_env_var` | `str` | — | Env var with a GitLab personal, project, or group access token. Sent in the 'PRIVATE-TOKEN' header on every request (GitLab's recommended auth header -- see docs.gitlab.com/ee/api/rest/authentication.html). Required for… _(full docs in schema.json + component README)_ |
| `project_id` | `str` | — | GitLab project ID (numeric, e.g. '42') or URL-encoded path (e.g. 'my-group/my-project'). Required when `tables` includes 'issues', 'merge_requests', or 'commits' -- those are all project-scoped endpoints (GET /projects/… _(full docs in schema.json + component README)_ |
| `group_id` | `str` | — | GitLab group ID (numeric) or URL-encoded path (e.g. 'my-group'). Only affects the 'projects' table: when set, lists every project in that group (GET /groups/:id/projects) instead of every project visible to the token (GE… _(full docs in schema.json + component README)_ |
| `asset_shape` | `Literal['single', 'multi_asset']` | `"single"` | 'single' (default, matches every other multi-table ingestion component in this repo, e.g. servicenow_ingestion/salesforce_ingestion/zendesk_ingestion): one asset, keyed `asset_name`, covering every table in `tables` as a… _(full docs in schema.json + component README)_ |
| `state` | `str` | — | Filter 'issues'/'merge_requests' by state: 'opened', 'closed', 'merged' (merge_requests only), or 'locked' (merge_requests only). Unset (default) returns every state. Not used by 'projects'/'commits'. |
| `updated_after` | `str` | — | Filter 'issues'/'merge_requests' to records updated on or after this ISO 8601 timestamp (e.g. '2024-01-01T00:00:00Z'). Not used by 'projects'/'commits'. |
| `since` | `str` | — | Filter 'commits' to commits on or after this ISO 8601 timestamp (e.g. '2024-01-01T00:00:00Z'). Only used by 'commits'. |
| `until` | `str` | — | Filter 'commits' to commits on or before this ISO 8601 timestamp. Only used by 'commits'. |
| `ref_name` | `str` | — | Branch, tag, or revision range to list 'commits' from. Defaults to the project's default branch when unset. Only used by 'commits'. |
| `per_page` | `int` | `100` | Records fetched per API call (GitLab's 'per_page' param). Default 100, GitLab's documented maximum. |
| `verify_ssl` | `bool` | `true` | Enable TLS verification. Set false only for self-managed instances with self-signed certs. |
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

Open issues and merge requests for a single project:

```yaml
type: dagster_community_components.GitlabIngestionComponent
attributes:
  asset_name: platform_repo
  access_token_env_var: GITLAB_TOKEN
  project_id: "my-group/platform"
  tables: issues,merge_requests
  state: opened
```

Every project in a group, persisted to Snowflake:

```yaml
type: dagster_community_components.GitlabIngestionComponent
attributes:
  asset_name: gitlab_projects
  access_token_env_var: GITLAB_TOKEN
  group_id: my-group
  tables: projects
  destination: snowflake
  dataset_name: gitlab
  persist_only: true
```

Self-managed instance, commit history on a specific branch:

```yaml
type: dagster_community_components.GitlabIngestionComponent
attributes:
  asset_name: release_commits
  base_url: https://gitlab.mycompany.com/api/v4
  access_token_env_var: GITLAB_TOKEN
  project_id: "42"
  tables: commits
  ref_name: release/2024-q1
  since: "2024-01-01T00:00:00Z"
```

## Pagination

GitLab's REST API returns a standard `Link` response header (`rel="next"`) on every paginated list endpoint — the same mechanism GitHub's API uses. This component follows it via dlt's `HeaderLinkPaginator` rather than manually incrementing `page`, so it keeps working if GitLab ever changes its internal offset scheme, and it naturally stops once a response has no `rel="next"` link (the last page).

## Auth

| Mode | When | How |
|---|---|---|
| Personal/project/group access token | Any project or group | Set `access_token_env_var`. Sent as the `PRIVATE-TOKEN` header on every request |
| Unauthenticated | Public projects only | Leave `access_token_env_var` unset — GitLab rate-limits unauthenticated requests much more aggressively |

## See also

- `github_ingestion` — the equivalent component for GitHub
- `gitlab_event_sensor` — trigger a Dagster job on new GitLab events (push, merge, issue, comment)
- [Schema](schema.json) · [Example](example.yaml)
