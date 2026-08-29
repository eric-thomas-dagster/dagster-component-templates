# `SalesforceRecordUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into a **Salesforce SObject** using Salesforce's **native External-ID upsert** endpoint. Atomic create-or-update in a single request — no search-then-write, no eventual consistency window, no idempotency-key gymnastics.

For each upstream row:
- `PATCH /services/data/vXX.0/sobjects/{Object}/{ExternalIdField}/{Value}`

Salesforce returns:
- **201 Created** if no record matched the external Id → new record inserted.
- **204 No Content** if a match existed → record updated in place.

Composite mode (`use_composite: true`, default) batches 200 records per HTTP call at:
- `PATCH /services/data/vXX.0/composite/sobjects/{Object}/{ExternalIdField}`

The composite path is Salesforce's fastest sync write API short of Bulk 2.0.

## When to use

- Sync computed data (customer scores, segmentation flags, LTV predictions) from a warehouse INTO Salesforce so sales/CS teams see them in-context.
- Mirror a golden-record customer table (dbt marts, Reverse-ETL best-practice) into Salesforce Accounts with atomic External-ID match.
- Push status changes (subscription state, health score) into Salesforce as a nightly job.

## Prerequisite: External-ID field on the SObject

The `external_id_field` you pass MUST be marked as **`External ID`** in the Salesforce SObject schema:
1. *Setup → Object Manager → {SObject} → Fields & Relationships*
2. Open the field, click *Edit*, check *External ID*, save.
3. Custom External Id fields end in `__c` (e.g. `External_Account_Id__c`).

Without the External ID flag, the upsert endpoint returns 400.

## Pairs with

- **`salesforce_resource`** — OAuth password-flow auth + workhorse HTTP client (required).
- **`salesforce_ingestion`** — the READ-side counterpart (dlt-based bulk pull).

Together they cover both directions of Salesforce data movement.

## Example

```yaml
type: dagster_component_templates.SalesforceRecordUpsertComponent
attributes:
  asset_name: salesforce_accounts_mirror
  upstream_asset_key: dbt_marts_accounts
  resource_key: salesforce

  sobject: Account
  external_id_field: External_Account_Id__c   # must be marked External ID
  fields_map:
    account_id: External_Account_Id__c        # upstream col → SF field
    name: Name
    industry: Industry
    annual_revenue: AnnualRevenue

  batch_size: 5000
  use_composite: true          # 200-record batches (default)
  composite_all_or_none: false # partial-success (per-row), not all-or-nothing
```

## Behavior + gotchas

- **External Id required** — every upstream row must have a value in the column mapped to `external_id_field`; rows with `None` / `NaN` there are skipped and counted in the `rows_skipped_no_key` metadata.
- **Blank vs missing** — `None` / `NaN` values are omitted from the request body so the existing Salesforce value is preserved. Empty strings `""` are sent verbatim.
- **Composite atomicity** — with `composite_all_or_none: false` (default) each row succeeds or fails independently within a 200-record batch. With `true`, any error rolls back the entire batch.
- **Rate limits** — Salesforce enforces API request limits (varies by org edition). The resource retries on 429 / 5xx with exponential backoff up to `max_retries` (default 3), and refreshes the OAuth token on 401.
- **Custom SObjects** — pass the API name (with `__c` suffix). Works identically to standard SObjects.
- **Fields not writable** — some SObject fields (Id, CreatedDate, LastModifiedDate, formula fields, roll-up summary fields) are not writable. If you include them in `fields_map`, Salesforce returns an error for those rows.

## Related

- `salesforce_resource` — connection + OAuth password flow + workhorse HTTP.
- `salesforce_ingestion` — read side (dlt-backed bulk pull).
