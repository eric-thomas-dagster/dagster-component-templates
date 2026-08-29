# `HubSpotObjectUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into a **HubSpot CRM object** (contacts / companies / deals / tickets / custom) via HubSpot's **native batch upsert** endpoint.

- `POST /crm/v3/objects/{objectType}/batch/upsert`

HubSpot atomically creates or updates each record based on matching a unique property (`key_property`). Up to 100 records per HTTP call — this sink chunks larger DataFrames automatically.

Native batch upsert is HubSpot's fastest sync write path — no search-then-write, no race conditions, no idempotency gymnastics.

## When to use

- Sync computed customer data (health scores, LTV predictions, segmentation flags) from a warehouse INTO HubSpot Contacts or Companies so sales/CS teams see them in-context.
- Mirror a golden-record deals mart into HubSpot Deals for pipeline reporting.
- Push a computed "at-risk" flag from a churn model into a custom property on Contacts.

## Prerequisites

1. **Private App with the right scopes** — Settings → Integrations → Private Apps → Create a Private App:
   - `crm.objects.contacts.read` / `crm.objects.contacts.write` (as needed per object type)
   - Same pattern for `companies`, `deals`, `tickets`, or custom object schemas
2. **`key_property` marked as unique** — HubSpot's batch upsert requires the key property to be marked "unique" in the object schema. `email` is unique on `contacts` by default; `domain` is unique on `companies`. For custom properties, go to Settings → Data Management → Properties → {Object} → Edit → check "Require unique values".

## Pairs with

- **`hubspot_resource`** — Private App bearer auth + workhorse HTTP client (required).
- **`hubspot_ingestion`** — the READ-side counterpart (dlt-based bulk pull).

## Example

```yaml
type: dagster_component_templates.HubSpotObjectUpsertComponent
attributes:
  asset_name: hubspot_contacts_mirror
  upstream_asset_key: dbt_marts_customers
  resource_key: hubspot

  object_type: contacts
  key_property: email
  fields_map:
    email: email                        # upstream col → HubSpot property
    first_name: firstname               # (both must be strings on the wire)
    last_name: lastname
    lifecycle_stage: lifecyclestage
    health_score: customer_health_score # custom property on Contacts

  batch_size: 100                       # HubSpot's per-call limit
  max_rows: 10000                       # overall safety cap per run
```

## Behavior + gotchas

- **Property names are case-sensitive** — HubSpot uses `firstname` (not `firstName`), `lifecyclestage`, `lastmodifieddate`, etc. Match the exact API property name.
- **All properties are strings on the wire** — the sink coerces DataFrame values to strings via `str(v)`. Numbers, dates, and booleans convert cleanly; complex objects would need to be pre-serialized upstream.
- **key_property must be in fields_map values** — validated at build time.
- **Missing key values are skipped** — rows where the mapped `key_property` is `None`, `NaN`, or `""` are dropped and counted in `rows_skipped_no_key` metadata.
- **Rate limits** — HubSpot enforces 100 requests / 10 sec on Private Apps (default). The resource retries on 429 honoring `Retry-After` header, up to `max_retries` (default 3).
- **Custom objects** — pass the fully-qualified type name (like `p123456_custom_thing`). Standard objects use short names (`contacts`, `deals`, etc.).
- **Association / relationships** — this sink upserts records only. To create associations (e.g. link a Contact to a Company), use `hubspot_resource` methods directly in a downstream component.

## Related

- `hubspot_resource` — connection + Private App bearer + workhorse HTTP.
- `hubspot_ingestion` — read side (dlt-backed bulk pull).
