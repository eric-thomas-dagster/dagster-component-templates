# OAuth REST Ingestion Component

Generic OAuth2-backed JSON REST API → pandas DataFrame, with native pagination. Covers the non-OData side of the SAP family (Concur, Ariba) and any other Bearer-token REST API.

Pair with [`oauth_token_resource`](../../../resources/oauth_token_resource/) for headless token lifecycle (refresh + rotation writeback).

## When to use this vs other ingestion components

| Component | Best for |
|---|---|
| `oauth_rest_ingestion` | JSON REST APIs needing OAuth2 + pagination. Concur, Ariba, GitHub Enterprise, Atlassian Cloud, PagerDuty, etc. |
| `odata_ingestion` | OData v2/v4 endpoints. S/4HANA, SuccessFactors, Datasphere, Dynamics 365, MS Graph |
| `rest_api_fetcher` | Single-request fetches without OAuth lifecycle |
| `airtable_ingestion` / vendor-specific | dlt-supported sources (when the vendor's verified source exists) |

## Pagination patterns

The protocol differs by vendor but the patterns are well-defined. Config one of:

### `none` — single request, no pagination

### `next_url` — response embeds next-page URL
```json
{"Items": [...], "NextPage": "https://..."}
```
Used by **Concur**, GitHub paginated APIs (`Link` header is alternate; this component reads body).
Config: `pagination: next_url`, `next_url_path: NextPage`, `records_path: Items`

### `cursor` — opaque cursor token
```json
{"records": [...], "nextPageToken": "abc123"}
```
Used by **Ariba** (Operational Reporting), Slack, Salesforce, BigQuery.
Config: `pagination: cursor`, `cursor_response_path: nextPageToken`, `cursor_param: pageToken`, `records_path: records`

### `page` — increment page number
Used by JIRA Cloud (older endpoints), many CRMs.
Config: `pagination: page`, `page_param: page`, `page_start: 1`, `records_path: data`

### `offset` — offset + limit
Used by older SAP APIs, MySQL-style admin APIs.
Config: `pagination: offset`, `offset_param: offset`, `limit_param: limit`, `page_size: 100`

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_name` | `str` | yes | |
| `api_url` | `str` | yes | GET endpoint |
| `query_params` | `dict` | no | `{partition_key}` substitution supported |
| `oauth_token_resource_key` | `str` | conditional | References an `oauth_token_resource` — preferred |
| `auth_token_env_var` | `str` | conditional | Pre-minted token (no rotation) |
| `extra_headers` | `dict` | no | |
| `pagination` | `str` | no | `none` (default) / `next_url` / `cursor` / `page` / `offset` |
| `records_path` | `str` | no | Dotted path to records array in response |
| `next_url_path` | `str` | for `next_url` | Default `NextPage` |
| `cursor_response_path` / `cursor_param` | `str` | for `cursor` | |
| `page_param` / `page_start` | `str` / `int` | for `page` | |
| `offset_param` / `limit_param` / `page_size` | `str` / `int` | for `offset` | |
| `max_pages` | `int` | no | Default 1000 |
| Standard fields | | | partition / freshness / owners / tags / deps / retry |

## Headless OAuth

The component is designed to run in Dagster Daemon, CI, k8s — no interactive auth ever. Pair it with `oauth_token_resource`:

```yaml
# resources/concur_token.yaml
type: dagster_component_templates.OAuthTokenResourceComponent
attributes:
  resource_key: concur_token
  token_endpoint: https://us.api.concursolutions.com/oauth2/v0/token
  grant_type: refresh_token
  client_id_env_var: CONCUR_CLIENT_ID
  client_secret_env_var: CONCUR_CLIENT_SECRET
  refresh_token_env_var: CONCUR_REFRESH_TOKEN
  refresh_writeback_command_env_var: CONCUR_REFRESH_WRITEBACK_CMD
```

```yaml
# defs/concur_expense/defs.yaml
type: dagster_component_templates.OAuthRestIngestionComponent
attributes:
  asset_name: concur_expense_reports
  api_url: https://us.api.concursolutions.com/expensereports/v4/reports
  oauth_token_resource_key: concur_token   # <-- the resource above
  pagination: next_url
  next_url_path: NextPage
  records_path: Items
```

The token resource handles refresh + (optional) rotation writeback so the next run keeps working.

## See also

- [`oauth_token_resource`](../../../resources/oauth_token_resource/) — paired token manager
- [`odata_ingestion`](../odata_ingestion/) — OData-specific sibling
- [`rest_api_fetcher`](../rest_api_fetcher/) — simpler single-request fetcher
