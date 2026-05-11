# Cloud DLP Inspect Asset

Scan DataFrame text columns for PII / sensitive data via [Cloud DLP](https://cloud.google.com/dlp). One inspect call per row; adds three columns:

| Added column | Type | Meaning |
|---|---|---|
| `dlp_finding_count` | int | Total findings across all infoTypes |
| `dlp_infotypes` | list[str] | Distinct infoType names detected (sorted) |
| `dlp_findings` | list[dict] | Per-finding `{info_type, likelihood, (quote)}` |

```yaml
type: dagster_component_templates.CloudDlpInspectAssetComponent
attributes:
  asset_name: tickets_dlp_scanned
  upstream_asset_key: support_tickets
  text_columns: [subject, body]
  info_types: [EMAIL_ADDRESS, PHONE_NUMBER, US_SOCIAL_SECURITY_NUMBER, CREDIT_CARD_NUMBER]
  min_likelihood: POSSIBLE
```

## Common patterns

- **Compliance pre-load gate**: scan → asset check → block warehouse load if `rows_with_pii > 0` for unexpected columns.
- **Redaction pipeline**: scan → conditional redact step on rows where `dlp_finding_count > 0`.
- **Audit reporting**: feed `infotypes_seen` metadata into a daily compliance dashboard.

## InfoTypes

Pick from the [infoType reference](https://cloud.google.com/dlp/docs/infotypes-reference). Common picks:

| Category | Examples |
|---|---|
| Identity | `PERSON_NAME`, `EMAIL_ADDRESS`, `PHONE_NUMBER`, `STREET_ADDRESS` |
| US PII | `US_SOCIAL_SECURITY_NUMBER`, `US_DRIVERS_LICENSE_NUMBER` |
| Payment | `CREDIT_CARD_NUMBER`, `IBAN_CODE` |
| Health | `MEDICAL_TERM`, `US_HEALTHCARE_NPI` |
| Credentials | `AUTH_TOKEN`, `AWS_CREDENTIALS`, `GCP_CREDENTIALS` |

## `include_quote: false` (default)

Quotes contain the actual matched substring — which is itself often PII. Default behavior excludes them. Enable only if you'll secure the resulting DataFrame (e.g. encrypted at-rest column).

## Auth

Service account needs `roles/dlp.user`.

## Cost

DLP charges per 1000 unit inspections; first 1 GB/month free, then ~$1/GB of content scanned. See <https://cloud.google.com/dlp/pricing>.

## Sister components

- Custom asset on top — for de-identification (`deidentify_content`), built-in transformations (MASK, REDACT, FPE, etc.).
