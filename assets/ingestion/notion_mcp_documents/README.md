# NotionMcpDocuments

Extract Notion page body text for RAG pipelines via Notion's **hosted MCP server**
(`https://mcp.notion.com/mcp`) instead of a self-created integration token.
[`notion_documents`](../notion_documents/README.md) reads the same kind of data through a real
integration token, which requires Notion workspace-admin access to create — many users don't have
that. This component signs in as **you** (OAuth, no admin token needed) and sees exactly what you
can see in Notion — not a scoped service-account view. Prefer `notion_documents` once an admin can
create you a token; use this component when that isn't available yet.

## Sign in first

```
python -m dagster_community_components.resources.notion_mcp_login --oauth-dir ~/.notion_mcp
```

Runs the OAuth Authorization Code + PKCE flow once in your browser, and writes `tokens.json` +
`client_info.json` under `--oauth-dir` (mode `0600`). Point `oauth_dir` at that same directory
below. Treat it as a secret — the refresh token inside grants everything you can see in Notion.
Re-run the sign-in command to switch workspaces, or if `oauth_dir`'s refresh token is ever
rejected (every downstream run then fails fast with a clear "sign in again" error instead of a
generic auth failure).

## How the crawl works

`notion-fetch`'s real tool result is a **JSON envelope** — `{"metadata": {"type": ...}, "title":
..., "url": ..., "text": ...}` — not plain markdown; the markdown-ish XML (`<content>`, child
`<page url="...">` tags, `<ancestor-path>`) lives JSON-escaped inside `text`. This component
decodes the envelope, uses its real `title`/`url` fields directly, and strips the "Here is the
result of 'fetch' for the Page/Database ..." wrapper line before extracting `<content>` /
`<wiki-home-content>`.

Each entry in `root_page_ids` is fetched, and any `<page url="...">` child link found in its
decoded content is recursively fetched too — same page-tree-walking idea as `notion_documents`'s
own `root_page_ids`, just over MCP's `notion-fetch` tool instead of the REST API. A discovered
page is only kept if its `<ancestor-path>` traces back to one of your configured roots (or it IS a
root) — a safety bound, since this reads everything the signed-in person can see, not just the
pages you named. `exclude_title_contains` skips a page **and all of its sub-pages** (their
children are never even visited) when its real title contains any of the given substrings,
case-insensitively; the titles actually skipped are reported as `pages_excluded_titles` in
materialization metadata (capped at 50), so you can see what got dropped.

```yaml
root_page_ids:
  - a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4
exclude_title_contains:
  - "DRAFT"
  - "(internal)"
max_pages: 500
```

## Rate limiting — Notion's hosted MCP does NOT return HTTP 429

A rate-limited call comes back as a normal, successful tool result whose **text** reads "The
Notion API rate limit was reached. Wait at least N seconds before retrying this tool." A naive
reader stores that text as if it were real page content — a real incident involved 211 pages
indexed with exactly that string as their "content" before anyone noticed. This component detects
that text pattern specifically and **never** returns it as content: it sleeps the requested time
and retries, up to `max_retries`, then fails the run with a clear terminal error if it never
clears.

```yaml
max_requests_per_second: 2   # shared by every concurrent worker through ONE limiter
concurrency: 2                # paired with max_requests_per_second, not a separate throttle
max_retries: 5
```

`max_requests_per_second` is enforced by a single limiter shared across every worker — pacing each
worker independently (e.g. `concurrency` workers each sleeping `1/rate`) still exceeds the real
aggregate rate once you have more than one worker, which is exactly what a real 700-page crawl
hit. The run reports `rate_limit_hits` / `rate_limit_total_wait_seconds` in materialization
metadata, along with `pages_skipped_excluded` / `_out_of_scope` / `_errored`.

## Secret redaction (`redact_secrets`, on by default)

A real runbook page can easily contain a pasted credential — one live crawl found a GitHub token
sitting in a page. Before content is hashed or stored, Slack (`xox...`), GitHub (`gh[pousr]_...`),
AWS (`AKIA...`), JWT-shaped, and PEM private-key strings are replaced with `[redacted]`:

```yaml
redact_secrets: true   # default
redact_secrets_extra_patterns:
  - "SECRET-\\d+"       # add your own regexes alongside the built-in ones
```

The total redaction count is reported as `secrets_redacted` in materialization metadata. Set
`redact_secrets: false` to disable (not recommended).

## Incremental sync via `state_path`

```yaml
state_path: /var/lib/dagster/notion_mcp_state/my_workspace.json
```

This repo's `notion-search` enumeration semantics for listing a whole workspace were not
independently verified against a live server this session, so incremental sync here is
**content-hash-based**, not `notion-search`-timestamp-based (the spec's own documented fallback).
Every run still re-fetches every in-scope page (fetching is also how child pages are discovered —
there's no cheaper partial crawl), but an unchanged page keeps its prior `updated_at` instead of
bumping it every run, and the run reports `pages_pruned`: ids that were in a prior run's state but
not this one. **Actually removing** pruned pages from a vector store or knowledge graph is
[`prune_missing_sources`](../../ai/vector_store_writer/README.md)'s job on `vector_store_writer` /
`knowledge_graph_builder`, downstream of this asset — not this component's.

## What this does NOT do

- No real "list every page in the workspace" call — this only crawls `root_page_ids` and whatever
  they link to, bounded by `exclude_title_contains`/`max_pages`/the ancestor-path scope check.
- `title`/`url` come from the envelope's own top-level fields whenever the tool result is valid
  JSON. For a tool result that isn't (an unexpected shape), `title` falls back to a `<title>` tag
  then the first markdown heading in `<content>`, then `""`, and `url` falls back to
  `https://www.notion.so/<32-hex-id>`.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset that will hold the data |
| `oauth_dir` | `str` | Directory holding tokens.json + client_info.json from the sign-in CLI (python -m dagster_community_components.resources.notion_mcp_login --oauth-dir <dir>). Treat as a secret -- it grants everything the signed-in person can see in Notion. |
| `root_page_ids` | `List[str]` | Notion page ids (or URLs) to crawl as page trees -- each is walked, and any child_page link found while walking its content is recursively walked too, bounded to pages whose ancestor path traces back to one of these roots. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `max_retries` | `int` | `5` | Retries on Notion's own text-based rate-limit signal (never HTTP 429) or a transient/5xx-shaped error, before failing the run with a clear terminal error. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"notion_mcp_documents"` | Asset group for organization |
| `description` | `str` | — | Asset description |
| `owners` | `List[str]` | — | Asset owners -- team names or email addresses. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags applied to the asset. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog. Auto-inferred if unset. |
| `deps` | `List[str]` | — | Upstream Dagster asset keys for lineage. |
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | Backfill execution strategy for a partitioned asset. |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'multi', 'dynamic', or None for unpartitioned. |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. |
| `partition_end_offset` | `int` | `0` | 0 (default): a time-based partition set only ever holds COMPLETED windows. 1: include the current, still-in-progress window. |
| `partition_timezone` | `str` | — | IANA timezone for the partitions definition (defaults to UTC). |
| `partition_start` | `str` | — | Partition start date in ISO format. Required for time-based partition types. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static or multi partitioning. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec. Overrides flat fields when set. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure (distinct from max_retries, which retries individual MCP tool calls). |
| `retry_policy_delay_seconds` | `int` | — | Seconds between asset-level retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `write_disposition` | `Literal['replace', 'append', 'merge']` | `"replace"` | dlt write disposition. 'replace' (default): each run fully replaces the table (matches notion_documents's own default and reasoning -- a stale page tree would otherwise just keep accumulating). 'merge': upserts keyed on… _(full docs in schema.json + component README)_ |
| `bucket_url` | `str` | — | Bucket/path URL for filesystem-shaped storage, or the staging area for destination='databricks'/'athena'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `exclude_title_contains` | `List[str]` | — | Case-insensitive substrings; a page whose title contains any of these is skipped, along with all of its sub-pages (its children are never even visited). |
| `max_pages` | `int` | — | Caps how many pages this run emits. Unset: no cap. |
| `max_requests_per_second` | `float` | `2.0` | Shared across every concurrent worker through ONE rate limiter -- a per-worker sleep still exceeds Notion's real aggregate limit (a previously-hit problem). Default 2, below Notion's own ~3 req/s average. |
| `concurrency` | `int` | `2` | Number of pages crawled in parallel. Default 2 -- paired with max_requests_per_second=2, not a separate per-worker throttle. |
| `redact_secrets` | `bool` | `true` | Replace Slack ('xox...'), GitHub ('gh[pousr]_...'), AWS ('AKIA...'), JWT-shaped, and PEM private-key strings found in page content with '[redacted]' before it's hashed or stored -- on by default, since a real runbook pag… _(full docs in schema.json + component README)_ |
| `redact_secrets_extra_patterns` | `List[str]` | — | Additional regex patterns (strings) to redact alongside the built-in ones. Only used when redact_secrets is True. |
| `state_path` | `str` | — | Local JSON file tracking {doc_id: {content_hash, updated_at}} across runs, so an unchanged page keeps its prior updated_at instead of bumping it every run. Also used to report how many previously-seen pages fell out of s… _(full docs in schema.json + component README)_ |
| `destination` | `str` | — | dlt destination identifier (e.g. 'snowflake', 'bigquery', 'postgres', 'filesystem', 'duckdb'). Leave empty for in-memory DuckDB -> DataFrame mode. |
| `dataset_name` | `str` | — | Target dataset/schema in the destination. Defaults to the asset name. |
| `persist_only` | `bool` | `false` | If True with destination set: emit a MaterializeResult and skip the DataFrame return. |
| `destination_credentials_url` | `str` | — | Inline destination connection string. If unset, dlt resolves credentials from env vars. |
| `destination_credentials_env_var` | `str` | — | Alternative to destination_credentials_url: env var holding the connection string. |
| `include_preview_metadata` | `bool` | `true` | Include a sample data preview in metadata |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (partition_type='dynamic'). |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only. |

[//]: # (FIELDS:END)

## Example

```yaml
type: dagster_community_components.NotionMcpDocumentsComponent
attributes:
  asset_name: notion_pages
  oauth_dir: /home/dagster/.notion_mcp
  root_page_ids:
    - a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4
  exclude_title_contains:
    - DRAFT
  max_requests_per_second: 2
  concurrency: 2
  state_path: /var/lib/dagster/notion_mcp_state/notion_pages.json
  partition_type: daily
  partition_start: "2024-01-01"
```

## Requirements

```
dagster
dlt
pandas
requests
mcp
anyio
```
