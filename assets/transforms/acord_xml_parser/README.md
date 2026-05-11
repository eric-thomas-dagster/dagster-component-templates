# ACORD XML Parser

Parse ACORD (Association for Cooperative Operations Research and Development) insurance XML messages into a flat DataFrame. One row per `<Policy>`, `<Claim>`, `<Application>`, or `<Quote>` inside the envelope.

ACORD is the dominant data-exchange standard across the global insurance industry — carriers, agencies, brokers, rating engines, and reinsurers all speak ACORD XML for policies / claims / quotes / certificates / appraisals / MVR pulls.

```yaml
type: dagster_component_templates.AcordXmlParserComponent
attributes:
  asset_name: acord_flat
  upstream_asset_key: acord_messages
  xml_column: xml
```

## Supported message types (envelope detection)

The component reads the first child of the `<ACORD>` root to detect the transaction type. Common envelopes:

| Envelope | Domain |
|---|---|
| `InsurancePolicyAddRq` / `Rs` | New-business submission |
| `InsurancePolicyChangeRq` / `Rs` | Endorsements |
| `InsurancePolicyCancelRq` / `Rs` | Cancellations |
| `InsurancePolicyQuoteInqRq` / `Rs` | Quote requests |
| `ClaimsNotificationRq` / `Rs` | First notice of loss (FNOL) |
| `ClaimsResponseRq` / `Rs` | Claim status / disposition |
| `CertificateOfInsuranceRq` / `Rs` | COI generation |
| `AppraisalRq` / `Rs` | Auto appraisal |
| `MotorVehicleReportRq` / `Rs` | MVR pull |

Most transactions can wrap multiple entities (e.g. a `ClaimsNotificationRq` might carry many `<Claim>` elements). The parser emits one row per inner entity, with the envelope metadata (`message_type`, `transaction_id`, `sender_id`) copied to every row.

## Entity types extracted

| Entity | Columns |
|---|---|
| `Policy` / `PolicySummary` / `Application` | policy_number, policy_status, line_of_business, effective_date, expiration_date, premium_amount, premium_currency, insured_name, insured_type |
| `Claim` | claim_number, loss_date, report_date, loss_cause, loss_desc, claim_status, policy_number, loss_amount, loss_currency |
| `Quote` | quote_number, quote_date, quote_expiration, rating_engine |

Other entity types fall through and just produce the envelope-only row.

## Namespace handling

The component strips XML namespaces internally (so `{urn:iso:...}Policy` is treated the same as `Policy`). Works with all common ACORD root namespaces:

- `http://www.ACORD.org/standards/PC_Surety/ACORD1/xml/` (P&C / Surety)
- `http://www.ACORD.org/standards/Life/2/xml/` (Life)
- `urn:acord:standards:lifeAnnuity:ACORD-XML-Std` (alternate Life)

Carrier-specific subsets follow the same shape — just additional vendor elements that the generic walk-through tolerates.

## Why ACORD matters

ACORD XML is the lingua franca between US/UK/AU/CA carriers and their distribution channels. If you're ingesting policy data from MGAs, brokers, or rating engines, you're parsing ACORD. The protocol is XML-heavy and verbose; this component covers the high-traffic fields used by ~80% of real-world ingest needs without forcing a full ACORD-spec compiler.

## Carrier-specific extensions

Carriers commonly extend ACORD with their own elements under custom namespaces (e.g. `<TravelersExt>...</TravelersExt>`). The generic walker preserves these by not failing on them, but doesn't extract them into named columns. For carrier-specific extraction, write a thin transform downstream that picks fields out of the raw XML by XPath.
