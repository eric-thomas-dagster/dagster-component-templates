# `JiraServiceManagementRequestUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **Jira Service Management (JSM) customer requests** via **search-then-write** — JSM has no single-call native upsert, and no servicedeskapi-scoped search. This sink JQL-searches `/rest/api/3/search/jql` (core API) scoped to `project_key` + `key_field`, then `PUT`s the match's fields (core API) or `POST`s a new request to `/rest/servicedeskapi/request` (servicedeskapi).

## Why two API surfaces

- **Create**: `POST /rest/servicedeskapi/request` — needs `service_desk_id` + `request_type_id` to set up the request-type-specific customer-portal plumbing a bare issue create doesn't.
- **Search / Update**: JSM has no `/rest/servicedeskapi/` endpoint for either of these. Search goes through core JQL (`POST /rest/api/3/search/jql`); field updates go through core `PUT /rest/api/3/issue/{id}` — because a service desk request IS a Jira issue under the hood.

See `jira_service_management_resource`'s README for the full architectural rationale (including why this isn't just `jira_resource` reused).

## The JQL custom-field quirk (read this before configuring `key_field`)

If `key_field` is a custom field (e.g. `customfield_10050`), JQL requires **`cf[10050] = "value"`** syntax — **not** `customfield_10050 = "value"` (that form does not parse as JQL, even though it's exactly what you'd PUT/POST in the REST body). This component's `_jql_match_clause()` helper handles the translation automatically: strips the `customfield_` prefix, wraps the numeric ID in `cf[...]`. Built-in fields (`summary`, `labels`, etc.) are used by their plain name directly.

## When to use

- Sync computed ticket/alert data from a warehouse INTO Jira Service Management as customer requests, with ongoing field updates as the upstream row changes.

## Prerequisites

1. A service desk + request type already configured in JSM (`service_desk_id`, `request_type_id`).
2. A stable **key field** present on created requests (a custom field or a built-in like `labels`) so repeat runs match the same request instead of creating duplicates.

## Pairs with

- **`jira_service_management_resource`** — API connection + auth (required).
- **`jira_service_management_ingestion`** — the READ-side counterpart (dlt-based bulk pull).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `jira_service_management_resource`) | Resource key registered by JiraServiceManagementResourceComponent. |
| `project_key` | required | Jira project key that scopes the JQL search, e.g. `ITSM`. |
| `service_desk_id` | required | Target service desk ID for request creation. |
| `request_type_id` | required | Target request type ID for request creation. |
| `key_field` | required | Jira field ID used to match existing requests. Must appear in `fields_map` values. |
| `fields_map` | required | Source column -> Jira field ID (`summary`, `description`, `customfield_10050`, etc.). |
| `comment_column` | optional | Source column with comment text, posted as an internal (non-public) comment on both create and update. |
| `batch_size` | optional (default `500`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.JiraServiceManagementRequestUpsertComponent
attributes:
  asset_name: jsm_requests_from_support_tickets
  upstream_asset_key: dbt_marts_open_tickets
  resource_key: jira_service_management_resource
  project_key: ITSM
  service_desk_id: "1"
  request_type_id: "10"
  key_field: customfield_10050
  fields_map:
    ticket_id: customfield_10050
    title: summary
    body: description
  comment_column: internal_note
  group_name: reverse_etl
```
