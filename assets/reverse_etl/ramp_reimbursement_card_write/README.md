# `RampReimbursementCardWriteComponent`

Reverse-ETL sink: write rows of an upstream DataFrame **into Ramp** (ramp.com) — the write side, pairing with the existing `ramp_ingestion` read-side component. One component, four `mode`s, since they all share the same OAuth2 `client_credentials` connection (`ramp_resource`):

| `mode` | Ramp endpoint | Effect |
|---|---|---|
| `mileage_reimbursement` | `POST /developer/v1/reimbursements/mileage` | Create one mileage reimbursement per row. |
| `receipt_reimbursement` | `POST /developer/v1/reimbursements/submit-receipt` (multipart) | Upload a receipt image per row, optionally attached to an existing reimbursement (else Ramp OCRs a draft). |
| `virtual_card_create` | `POST .../cards/vault` (Vault API) | Issue one virtual card per row and retrieve its PAN/CVV. |
| `card_update` | `PATCH /developer/v1/cards/physical/{card_id}` | Update `display_name`/`fund_id`/`automatic_routing_enabled` on an existing **physical** card. |

Every mode is **create/update-only, per-row** (no delete), with per-row success/error/skip counts in the output metadata — same convention as `asana_task_create` and `okta_user_upsert`.

## Read this before using `virtual_card_create` in production

Ramp has no endpoint that creates a virtual card by itself — the only documented way is the **Vault API** (`POST /developer/v1/cards/vault`), which returns a full PAN/CVV and which Ramp gates explicitly:

> Ramp reviews your use case, security controls, and PCI handling before the Vault API can return full PANs and CVVs in production. **All customers can use the Vault API in Sandbox.** Submit a Developer API support ticket to begin the review.

Confirmed directly against `https://docs.ramp.com` — not assumed. In practice:

- **Sandbox** (`https://demo-api.ramp.com`): works immediately for every developer once your app has `cards:read_vault` + `limits:write` + `funds:write` enabled.
- **Production**: will fail with a permissions error until Ramp has manually approved your app's Vault API access via a support ticket — independent of whether the scopes are toggled on. Budget time for that review before relying on this mode in production.

**Security:** the Vault API response includes a full PAN and CVV. This component never writes them into Dagster metadata or logs — only `card_id`, `spend_limit_id`, and a masked last-4 survive into the `created_cards` metadata (Ramp's own docs: "Do not store or log PANs or CVVs.").

## `card_update` has no spend-limit field — by design, not by omission

This was initially assumed to exist (a PATCH to change `spend_limit`/`restrictions` on an existing card) — it doesn't. Ramp's Developer API documents exactly one update endpoint for an existing card, and it accepts only `display_name`, `fund_id`, and `automatic_routing_enabled`. There is **no documented way, anywhere in Ramp's public API, to change a card's spend limit after it has been created** — not for physical cards, not for virtual ones. `card_update` mode reflects that real, narrower surface; it raises at build time if none of `display_name_column`/`fund_id_column`/`automatic_routing_enabled_column` is set, since there would otherwise be nothing it could ever do.

If you need a different spend limit, issue a new card via `virtual_card_create` instead (or change it by hand in the Ramp dashboard — Ramp's UI can do this even though the public API cannot).

## Prerequisites

1. A `ramp_resource` registered via `RampResourceComponent`, with scopes matching the mode(s) you use (`reimbursements:write` for the two reimbursement modes, `cards:write` for `card_update`, `cards:read_vault`+`limits:write`+`funds:write` for `virtual_card_create`).
2. For `receipt_reimbursement`: the receipt files must be reachable as local filesystem paths from wherever the asset executes (e.g. a shared volume, or a prior step that downloads/stages them).

## Pairs with

- **`ramp_resource`** — OAuth2 `client_credentials` connection (required).
- **`ramp_ingestion`** — the READ-side counterpart (dlt-based bulk pull of transactions/cards/users/reimbursements via a separately pre-minted static bearer token).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `ramp_resource`) | Resource key registered by RampResourceComponent. |
| `mode` | required | One of `mileage_reimbursement`, `receipt_reimbursement`, `virtual_card_create`, `card_update`. |
| `reimbursee_id_column` | required for mileage/receipt modes | Upstream column holding the Ramp user ID of the recipient. |
| `trip_date_column` | required for mileage_reimbursement | Upstream column holding the trip date. |
| `distance_column` | required for mileage_reimbursement | Upstream column holding the mileage distance. |
| `distance_units` | optional (default `MILES`) | `MILES` or `KILOMETERS`, applies to every row. |
| `receipt_file_path_column` | required for receipt_reimbursement | Upstream column holding a local path to the receipt image/PDF. |
| `user_id_column` | required for virtual_card_create | Upstream column holding the Ramp cardholder user_id. |
| `limit_amount_column` | required for virtual_card_create | Upstream column holding the spend limit amount. |
| `interval` | required for virtual_card_create | Spend-limit interval (ANNUAL/DAILY/MONTHLY/...), applies to every row. |
| `card_id_column` | required for card_update | Upstream column holding the Ramp card_id to update. |

See the component's `schema.json` / `component.py` for the full per-mode optional-column list (start/end location, memo, spend_allocation_id, waypoints, reimbursement_id, idempotency_key, display_name, spend_program_id, transaction_amount_limit, lock_date, fund_id, automatic_routing_enabled).

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output Dagster asset name. |
| `mode` | `str` | One of 'mileage_reimbursement', 'receipt_reimbursement', 'virtual_card_create', 'card_update'. Each mode calls a different Ramp endpoint with a different required-column set -- see README. |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"ramp_resource"` | Resource key registered by RampResourceComponent. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `max_rows` | `int` | `10000` | Overall safety cap on rows per run. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"ramp"` | Dagster asset group name. |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners. |
| `tags` | `Dict[str, str]` | — | Catalog tags. |
| `kinds` | `List[str]` | — | Asset kinds (auto-includes 'ramp'). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | Upstream Dagster asset providing the DataFrame. Mutually exclusive with `source:`. |
| `source` | `Dict[str, Any]` | — | Inline source config. Mutually exclusive with `upstream_asset_key`. Shapes: {kind: sql, resource_key/database_url_env_var, query}, {kind: csv, path, read_csv_kwargs}, {kind: inline, rows}. |
| `reimbursee_id_column` | `str` | — | Upstream column holding the Ramp user ID of the reimbursement recipient. Required for mileage_reimbursement and receipt_reimbursement. |
| `trip_date_column` | `str` | — | Upstream column holding the trip date (ISO date string). Required for mileage_reimbursement. |
| `distance_column` | `str` | — | Upstream column holding the mileage distance. Required for mileage_reimbursement. |
| `distance_units` | `str` | `"MILES"` | 'MILES' or 'KILOMETERS' -- applies to every row in mileage_reimbursement mode. |
| `start_location_column` | `str` | — | Upstream column holding the trip start location (mileage_reimbursement). |
| `end_location_column` | `str` | — | Upstream column holding the trip end location (mileage_reimbursement). |
| `memo_column` | `str` | — | Upstream column holding a free-text memo (mileage_reimbursement). |
| `spend_allocation_id_column` | `str` | — | Upstream column holding a Ramp spend_allocation_id (mileage_reimbursement). |
| `waypoints_column` | `str` | — | Upstream column holding a comma-separated list of waypoint addresses (mileage_reimbursement). |
| `receipt_file_path_column` | `str` | — | Upstream column holding a local filesystem path to the receipt image/PDF. Required for receipt_reimbursement. |
| `reimbursement_id_column` | `str` | — | Upstream column holding an existing Ramp reimbursement_id to attach the receipt to. If omitted/blank for a row, Ramp attempts to auto-create a draft reimbursement via OCR on the receipt image (receipt_reimbursement). |
| `idempotency_key_column` | `str` | — | Upstream column holding an idempotency key for the receipt upload. If unset, a deterministic key is derived per row from (reimbursee_id, receipt_file_path, reimbursement_id) so re-running the same row is idempotent (receipt_reimbursement). |
| `user_id_column` | `str` | — | Upstream column holding the Ramp user_id (cardholder). Required for virtual_card_create. |
| `limit_amount_column` | `str` | — | Upstream column holding the spend limit amount. Required for virtual_card_create. |
| `interval` | `str` | — | Spend limit interval, one of ANNUAL/DAILY/MONTHLY/QUARTERLY/TERTIARY/TOTAL/WEEKLY/YEARLY. Applies to every row. Required for virtual_card_create. |
| `currency_code` | `str` | `"USD"` | Currency code for the spend limit (virtual_card_create). |
| `display_name_column` | `str` | — | Upstream column holding a display name. Used by both virtual_card_create (new card) and card_update (rename existing card). |
| `spend_program_id_column` | `str` | — | Upstream column holding a Ramp spend_program_id (virtual_card_create). |
| `transaction_amount_limit_column` | `str` | — | Upstream column holding a per-transaction amount limit (virtual_card_create). |
| `lock_date_column` | `str` | — | Upstream column holding an ISO lock_date for the spend limit (virtual_card_create). |
| `spending_restrictions_extra` | `Dict[str, Any]` | — | Static (same for every row) extra spending_restrictions fields to merge in, e.g. {'allowed_categories': [...], 'blocked_vendors': [...]} (virtual_card_create). |
| `card_id_column` | `str` | — | Upstream column holding the Ramp card_id to update. Required for card_update. |
| `fund_id_column` | `str` | — | Upstream column holding a new fund_id to attach to the card (card_update). |
| `automatic_routing_enabled_column` | `str` | — | Upstream column holding a boolean for automatic_routing_enabled (card_update). |

[//]: # (FIELDS:END)

## Example
```yaml
type: dagster_component_templates.RampReimbursementCardWriteComponent
attributes:
  asset_name: ramp_mileage_reimbursements
  upstream_asset_key: approved_mileage_claims
  resource_key: ramp_resource
  mode: mileage_reimbursement
  reimbursee_id_column: ramp_user_id
  trip_date_column: trip_date
  distance_column: miles_driven
  distance_units: MILES
  memo_column: trip_memo
  group_name: reverse_etl
```
