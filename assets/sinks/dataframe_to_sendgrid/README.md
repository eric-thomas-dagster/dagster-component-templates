# DataFrame to SendGrid

Reverse-ETL sink — batch-PUT DataFrame rows to SendGrid Marketing as
contact upserts via the async bulk-import endpoint
(`PUT /v3/marketing/contacts`, up to 30,000 contacts per request).

Pairs with **`SendGridResourceComponent`** for shared auth.

## Two source shapes (pick one)

- `upstream_asset_key` — read from another Dagster asset
- `source: {kind: sql | csv | inline, ...}` — inline source config

## Field layout

SendGrid Marketing contacts have a fixed set of top-level "reserved"
fields (`email`, `first_name`, `last_name`, `address_line_1`, `city`,
`state_province_region`, `country`, `postal_code`, `phone_number_id`,
`whatsapp`, `line`, `facebook`, `unique_name`, ...). Custom attributes
must be sent under `custom_fields` — a dict keyed by SendGrid's
numeric field ID (not the field name).

The sink handles this automatically:

- `email_column` provides the primary identifier (required).
- Values from `fields_map` land at the top level if the target is a
  reserved field, else under `custom_fields` (keyed by the ID in
  `custom_field_ids`).

Look up your custom field IDs via `GET /v3/marketing/field_definitions`.

## Example

```yaml
type: dagster_community_components.DataframeToSendGridComponent
attributes:
  asset_name: sendgrid_daily_contact_sync
  upstream_asset_key: dim_customers_current
  resource_key: sendgrid

  email_column: email
  fields_map:
    first: first_name                  # reserved → top level
    last: last_name
    city: city
    country: country
    ltv: lifetime_value_usd            # custom → nested
    segment: segment
  custom_field_ids:
    lifetime_value_usd: e1_N            # SendGrid custom field IDs
    segment: e2_T

  list_ids:
    - "abc-123-list-id"

  freshness_cron: "0 8 * * *"
  partition_type: daily
  partition_start: "2026-01-01"
```

## Materialization metadata

- `rows_total`, `contacts_sent`, `contacts_failed`
- `batches` — number of bulk-import requests PUT
- `job_ids` — list of SendGrid job ids; poll via `GET /v3/marketing/contacts/imports/{id}`
- `list_ids`, `dry_run`
