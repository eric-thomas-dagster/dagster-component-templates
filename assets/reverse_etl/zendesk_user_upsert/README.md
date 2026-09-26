# `ZendeskUserUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **Zendesk end-users** via Zendesk's **native** `create_or_update` endpoint.

- `POST /api/v2/users/create_or_update.json`

Zendesk atomically creates or updates based on matching `email` (or `external_id` once set on the user). No search-then-write needed.

## When to use

- Sync computed customer data (health scores, plan tier, churn risk) from a warehouse INTO Zendesk users so support agents see it in-context on a ticket.

## Prerequisites

1. **Custom user fields already created** in Zendesk admin (Admin Center → People → Configuration → User fields) for anything in `fields_map` — Zendesk rejects unknown field keys.

## Pairs with

- **`zendesk_resource`** — Zenpy client auth (required).
- **`zendesk_ingestion`** — the READ-side counterpart (dlt-based bulk pull).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `zendesk_resource`) | Resource key registered by ZendeskResourceComponent. |
| `email_column` | required | Upstream column holding the user's email (Zendesk match key). |
| `name_column` | required | Upstream column holding the user's display name. |
| `external_id_column` | optional | Upstream column holding an external_id to set on the Zendesk user. |
| `fields_map` | optional | Upstream column -> Zendesk custom user_field key. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.ZendeskUserUpsertComponent
attributes:
  asset_name: zendesk_users_mirror
  upstream_asset_key: dbt_marts_customers
  resource_key: zendesk_resource
  email_column: email
  name_column: full_name
  external_id_column: customer_id
  fields_map:
    health_score: health_score
    plan_tier: plan_tier
  group_name: reverse_etl
```
