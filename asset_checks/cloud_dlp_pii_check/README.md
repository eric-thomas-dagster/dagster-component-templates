# Cloud DLP PII Check

Pass/fail Dagster asset check that runs [Cloud DLP](https://cloud.google.com/dlp) against an upstream DataFrame and fails (or warns) if forbidden PII is present. Sister to the `cloud_dlp_inspect_asset` — same scan, different Dagster object type.

```yaml
type: dagster_component_templates.CloudDlpPiiCheckComponent
attributes:
  asset_key: support_tickets
  check_name: no_payment_pii
  text_columns: [subject, body]
  forbidden_info_types: [US_SOCIAL_SECURITY_NUMBER, CREDIT_CARD_NUMBER]
  min_likelihood: LIKELY
  max_allowed_findings: 0
  blocking: true
```

## When to pick this vs `cloud_dlp_inspect_asset`

| You want | Pick |
|---|---|
| **Block downstream** materializations if PII is present | `cloud_dlp_pii_check` (check, with `blocking: true`) |
| Continue with PII info available downstream (for redaction routing, reporting) | `cloud_dlp_inspect_asset` (augments DataFrame) |
| Both: scan once, gate, also propagate findings | Use both — `inspect_asset` first, then check downstream of it |

## Behavior

- Scans only the first `sample_size` rows by default (200) to keep cost predictable. Set to `null` to scan all.
- Joins `text_columns` per row with newlines, single DLP `inspect_content` call per row.
- Counts findings across `forbidden_info_types` only — non-forbidden infoTypes are not surfaced.
- Passes when `total_forbidden_findings <= max_allowed_findings`.

## Tuning `min_likelihood`

Default is `LIKELY` (vs. `POSSIBLE` in the asset version) — DLP's likelihood enum is per-finding, and `LIKELY+` reduces false-positive churn at the gate. Tighten to `VERY_LIKELY` if your data has many `LIKELY` false alarms, or loosen to `POSSIBLE` for stricter compliance.

## Auth

Service account needs `roles/dlp.user`.

## Cost

DLP charges per inspection unit. At `sample_size: 200`, you're under the 1 GB/mo free tier even with thousands of check runs. See <https://cloud.google.com/dlp/pricing>.

## Sister components

- `cloud_dlp_inspect_asset` — augments DataFrame with finding columns (different shape)
- `pandas_dataframe_check` — column existence + dtype checks (no PII)
- `great_expectations_check` — full GE expectation-suite integration
