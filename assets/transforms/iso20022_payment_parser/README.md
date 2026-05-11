# ISO 20022 Payment Parser

Parse a column of ISO 20022 XML payment messages into a flat per-transaction DataFrame. Handles the four most-common message families out of the box; falls back to a generic header-only extractor for others.

```yaml
type: dagster_component_templates.Iso20022PaymentParserComponent
attributes:
  asset_name: payments_flat
  upstream_asset_key: iso20022_messages
  xml_column: xml
```

## Supported message families

| ID | Name | Output |
|---|---|---|
| `pacs.008` | FI-to-FI Customer Credit Transfer | One row per `CdtTrfTxInf` (transactions inside the message) |
| `pain.001` | Customer Credit Transfer Initiation | One row per transaction (across all `PmtInf` blocks) |
| `pacs.002` | Payment Status Report | One row per status (`TxInfAndSts`) |
| `camt.054` | Bank-to-Customer Debit/Credit Notification | One row per entry (`Ntry`) |

Unknown families get a single fallback row with the `GrpHdr` (msg_id, control sum, etc.) so you can still see what came through.

## Why this exists

ISO 20022 is replacing legacy SWIFT MT globally. SEPA, Fedwire, CHIPS, and a long list of clearing networks all use it. Every fintech / treasury / payments-ops pipeline needs to parse these XML envelopes into rows for reconciliation, anomaly detection, and reporting.

## Auto-detected delimiters and namespaces

The component strips XML namespaces (`{urn:...:pacs.008.001.08}Document` → `Document`) so the same XPath works across schema versions (`.001.08`, `.001.09`, etc.). Message type is detected from the XML namespace if present, else from element-tag shape.

## Output columns per message family

**pacs.008 / pain.001:**
- `message_type`, `msg_id`, `txn_id`, `end_to_end_id`
- `amount`, `currency`
- `debtor_name`, `debtor_account`, `debtor_bic`
- `creditor_name`, `creditor_account`, `creditor_bic`
- `remittance_info`

**pacs.002:**
- `message_type`, `msg_id`, `txn_id`, `end_to_end_id`
- `status`, `reason_code`, `reason_text`

**camt.054:**
- `message_type`, `credit_debit_ind`, `amount`, `currency`
- `booking_dt`, `value_dt`, `txn_ref`, `remittance_info`

## Carry-over columns

Any non-XML column on the upstream row (e.g. `ingested_at`, `source_bank`, `file_name`) is carried into every output row.

## Validation

Messages that don't parse as XML are recorded as parse errors (one row with `_error` set). The `errors` metadata field counts them.

## Sister components

- `synthetic_data_generator` `iso20022_payments` schema — common upstream for demos
- `fhir_resource_normalizer` / `hl7_v2_parser` — same pattern for healthcare
- `dataframe_to_bigquery` — common downstream sink for treasury reconciliation
