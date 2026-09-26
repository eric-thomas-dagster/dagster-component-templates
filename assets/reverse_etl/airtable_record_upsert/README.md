# `AirtableRecordUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into an **Airtable table** via Airtable's **native server-side upsert**.

- `PATCH /v0/{baseId}/{tableName}?performUpsert[fieldsToMergeOn][]=<field>`

Airtable atomically creates or updates each record based on matching `key_fields`. The sink is idempotent by design — safe to re-run on the same data.

## When to use

- Sync computed data (scores, flags, statuses) from a warehouse INTO an Airtable base so non-technical teams can view/filter/sort it in Airtable's UI.

## Prerequisites

1. **Personal Access Token** with `data.records:write` scope for the target base — create one at https://airtable.com/create/tokens.
2. **`key_fields` must uniquely identify a record** — Airtable's upsert matches rows where ALL listed fields equal the incoming row's values (typically one field like `email` or `Name`).

## Pairs with

- **`airtable_resource`** — Personal Access Token auth + read/write convenience methods (required).
- **`airtable_ingestion`** — the READ-side counterpart (dlt-based bulk pull).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `airtable`) | Resource key registered by AirtableResourceComponent. |
| `base_id` | required | Airtable base ID (starts with `app`). |
| `table` | required | Target table name or ID (starts with `tbl`). |
| `key_fields` | required | Airtable field name(s) to match on for upsert. |
| `fields_map` | required | Upstream column -> Airtable field name. |
| `typecast` | optional (default `true`) | Let Airtable auto-coerce string values into typed fields. |
| `batch_size` | optional (default `1000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.AirtableRecordUpsertComponent
attributes:
  asset_name: airtable_tasks_mirror
  upstream_asset_key: tasks_seed
  resource_key: airtable
  base_id: appXXXXXXXXX
  table: "Table 1"
  key_fields: [Name]
  fields_map:
    name: Name
    description: Notes
    state: Status
  typecast: true
```
