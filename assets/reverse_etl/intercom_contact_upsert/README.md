# `IntercomContactUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **Intercom contacts** via **search-then-write** — Intercom has no single-call native upsert (unlike Zendesk/HubSpot). This sink searches `/contacts/search` by `external_id` (or `email`), then updates the match or creates a new contact.

## When to use

- Sync computed customer data (health scores, plan tier) from a warehouse INTO Intercom contacts so support/sales see it in-context.

## Prerequisites

1. **Custom data attributes already created** in Intercom (Settings → Data → Custom Data Attributes) for anything in `custom_attributes_map` — Intercom rejects unknown attribute keys.
2. At least one of `email_column` or `external_id_column` must be set.

## Pairs with

- **`intercom`** resource — Bearer token auth (required).
- **`intercom_ingestion`** — the READ-side counterpart (dlt-based bulk pull).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `intercom`) | Resource key registered by IntercomResourceComponent. |
| `email_column` | one of email_column/external_id_column | Upstream column holding the contact's email. |
| `external_id_column` | one of email_column/external_id_column | Upstream column holding an external_id (takes precedence over email). |
| `attributes_map` | optional | Upstream column -> Intercom CORE contact field (e.g. name, phone). |
| `custom_attributes_map` | optional | Upstream column -> Intercom custom_attributes key. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.IntercomContactUpsertComponent
attributes:
  asset_name: intercom_contacts_mirror
  upstream_asset_key: dbt_marts_customers
  resource_key: intercom
  email_column: email
  attributes_map:
    name: full_name
  custom_attributes_map:
    health_score: health_score
  group_name: reverse_etl
```
