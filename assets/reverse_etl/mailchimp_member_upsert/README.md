# `MailchimpMemberUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into a **Mailchimp audience** via the **native upsert** endpoint.

- `PUT /lists/{list_id}/members/{subscriber_hash}` (subscriber_hash is the lowercased, MD5-hashed email)

Mailchimp atomically creates or updates each member based on email. No search-then-write, no race conditions.

## When to use

- Sync computed customer data (lifecycle stage, plan tier, engagement score) from a warehouse INTO Mailchimp merge fields so marketing campaigns can segment on it.
- Apply computed tags (e.g. `at-risk`, `power-user`) for audience segmentation.

## Prerequisites

1. **Merge fields already created** in the target audience (Audience → Settings → Merge fields) for anything in `merge_fields_map` — Mailchimp rejects unknown merge tags.

## Pairs with

- **`mailchimp`** resource — API key auth + `upsert_member` convenience method (required).
- **`mailchimp_ingestion`** — the READ-side counterpart (dlt-based bulk pull; does not share this resource).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `mailchimp`) | Resource key registered by MailchimpResourceComponent. |
| `list_id` | required | Mailchimp audience (list) ID to upsert members into. |
| `email_column` | required | Upstream column holding the member email. |
| `merge_fields_map` | optional | Upstream column -> Mailchimp merge field tag. |
| `tags_column` | optional | Upstream column holding tags to apply. |
| `status_if_new` | optional (default `subscribed`) | Status to set when creating a new member. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.MailchimpMemberUpsertComponent
attributes:
  asset_name: mailchimp_members_mirror
  upstream_asset_key: dbt_marts_customers
  resource_key: mailchimp
  list_id: a1b2c3d4e5
  email_column: email
  merge_fields_map:
    full_name: FNAME
    plan_tier: PLAN
  tags_column: segment_tags
  group_name: reverse_etl
```
