# X12 EDI Parser

Parse ASC X12 EDI envelopes (the dominant US-domestic B2B / healthcare exchange format) into a flat DataFrame with one row per ST/SE transaction set. ISA + GS envelope context is inherited as columns.

```yaml
type: dagster_component_templates.X12EdiParserComponent
attributes:
  asset_name: edi_transactions
  upstream_asset_key: x12_messages
  message_column: message
```

## Supported transaction sets (with hand-tuned extractors)

| ID | Name | Extra columns |
|---|---|---|
| **270** | Eligibility inquiry | `trace_num`, `payer_name`, `subscriber_first_name`, `subscriber_last_name` |
| **271** | Eligibility response | Same as 270 |
| **835** | Remittance advice / claim payment | `payment_amount`, `credit_debit`, `payment_method`, `payment_date` |
| **837** | Healthcare claim | `claim_account_num`, `claim_total_charge`, `billing_provider_name`, `subscriber_*`, `patient_*`, `payer_name` |
| **850** | Purchase order | `po_type`, `po_number`, `po_date` |
| **855** | PO acknowledgment | Same as 850 |

Other transaction sets fall through to the envelope-only output (ISA + GS + ST headers, no detail extraction). Easy to extend: add a function and wire it into `_TXN_EXTRACTORS`.

## Auto-detected delimiters

X12 declares its own delimiters in the ISA header at fixed offsets:
- Element separator: `ISA[3]` (almost always `*`)
- Component separator: `ISA[104]` (`:` or `>`)
- Segment terminator: `ISA[105]` (typically `~`)

The parser auto-detects all three — no configuration needed.

## Output columns (always present)

| Column | Source |
|---|---|
| `isa_sender_id`, `isa_receiver_id`, `isa_date`, `isa_time`, `isa_control_num`, `isa_usage_indicator` | ISA Interchange Control Header |
| `gs_functional_id`, `gs_sender`, `gs_receiver`, `gs_date`, `gs_control_num`, `gs_version` | GS Functional Group Header |
| `transaction_set`, `st_control_num`, `segment_count` | ST + SE Transaction Set wrappers |
| (+ extracted fields per transaction set type) | |

## Carry-over columns

Any non-message column on the upstream row (e.g. `ingested_at`, `source_system`, `file_name`) is carried into every transaction row.

## Validation

Messages that don't start with `ISA` are recorded as parse errors with an `_error` column.

## Sister components

- [`hl7_v2_parser`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/hl7_v2_parser) — different US healthcare standard (clinical / messaging)
- [`fhir_resource_normalizer`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/tree/main/assets/transforms/fhir_resource_normalizer) — modern healthcare standard (JSON)
- [`iso20022_payment_parser`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/iso20022_payment_parser) — global payments standard
- [`fix_message_parser`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/fix_message_parser) — trading messages
- [`synthetic_data_generator`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/ai/synthetic_data_generator) `x12_messages` schema — common upstream for demos
