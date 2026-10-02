# `JiraServiceManagementResourceComponent`

Shared Jira Service Management (JSM) connection: HTTP Basic auth (email + Atlassian API token) over the `/rest/servicedeskapi/...` surface, plus the slice of the CORE Jira Cloud REST v3 API (`/rest/api/3/...`) that JSM itself has no equivalent for.

## Why this is a separate resource from `jira_resource`

This repo already has `jira_resource`, which wraps the core Jira Cloud REST v3 API (`/rest/api/3/...` — issues, JQL search, comments, transitions, projects). Every one of its methods builds a URL under that one base path. It has **zero** methods touching `/rest/servicedeskapi/`, and it cannot create a customer request the way Jira Service Management needs:

- `jira_resource.create_issue()` posts to `POST /rest/api/3/issue` — a bare Jira issue.
- Jira Service Management's own "create a customer request" API is `POST /rest/servicedeskapi/request`, which needs `serviceDeskId` + `requestTypeId` + `requestFieldValues`. This sets up request-type-specific customer-portal plumbing (SLA clocks, portal visibility, approval workflow, organization sharing) that plain issue creation does not.

So creating/commenting on a *request* genuinely requires a different API surface — hence a new resource, not reuse.

The flip side: Jira Service Management's API has **no** general "update a request's fields" endpoint under `/rest/servicedeskapi/`. Field updates on an existing request go through the **core** API — `PUT /rest/api/3/issue/{issueIdOrKey}` — because under the hood a service desk request IS a Jira issue. So this resource duplicates a small piece of core-API logic (JQL search + field PUT) that `jira_resource` already implements. This is intentional: per this repo's convention, components and resources never import from one another — everything is self-contained, even at the cost of a little overlap.

| Operation | API surface | Method |
|---|---|---|
| Create a request | `POST /rest/servicedeskapi/request` | `create_request()` |
| Comment on a request | `POST /rest/servicedeskapi/request/{id}/comment` | `add_request_comment()` |
| Search for a request | `POST /rest/api/3/search/jql` (core) | `search_issues_jql()` |
| Update a request's fields | `PUT /rest/api/3/issue/{id}` (core) | `update_issue_fields()` |

## Prerequisites

1. An Atlassian API token: <https://id.atlassian.com/manage-profile/security/api-tokens>
2. A Jira Service Management project with at least one service desk + request type configured.

## Pairs with

- **`jira_service_management_request_upsert`** — the write-side reverse-ETL sink built on this resource.
- **`jira_service_management_ingestion`** — the read-side bulk ingestion (dlt-based).

## JQL custom-field quirk (real, confirmed)

To filter on a custom field by its numeric ID, JQL requires `cf[10050] = "value"` syntax — **not** `customfield_10050 = "value"` (the latter does not parse). Built-in fields (`summary`, `labels`, etc.) use their plain name directly, e.g. `summary ~ "value"`. `search_issues_jql()` does not build this for you — see `jira_service_management_request_upsert` for a helper that does.

## Configuration

| Field | Required | Default | Description |
|---|---|---|---|
| `resource_key` | optional | `jira_service_management_resource` | Resource registration key. |
| `email_env_var` | optional | `JSM_EMAIL` | Env var holding the Atlassian account email. |
| `api_token_env_var` | optional | `JSM_API_TOKEN` | Env var holding the Atlassian API token. |
| `site_domain` | required | — | Subdomain only, e.g. `mysite` for `mysite.atlassian.net`. |
| `verify_ssl` | optional | `true` | Disable only for self-signed / on-prem edge cases. |

## Example
```yaml
type: dagster_component_templates.JiraServiceManagementResourceComponent
attributes:
  resource_key: jira_service_management_resource
  email_env_var: JSM_EMAIL
  api_token_env_var: JSM_API_TOKEN
  site_domain: mysite
```

## See also

- `jira_resource` — core Jira Cloud REST API (issues, JQL, transitions) — use this for non-service-desk Jira work.
- [Schema](schema.json) · [Example](example.yaml)
