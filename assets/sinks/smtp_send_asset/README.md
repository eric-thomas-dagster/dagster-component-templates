# SMTP Send Asset

Send emails via SMTP — one per upstream row (data-driven outbound mail) or one summary email per run. Pairs with [`imap_inbox_source`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/ingestion/imap_inbox_source) for round-trip pipelines.

```yaml
type: dagster_component_templates.SmtpSendAssetComponent
attributes:
  asset_name: email_send_log
  upstream_asset_key: notifications
  host: smtp.gmail.com
  port: 587
  use_starttls: true
  username_env_var: SMTP_USER
  password_env_var: SMTP_APP_PASSWORD
  sender: "Pipeline Bot <bot@me.com>"
  mode: per_row
  to_template: "{user_email}"
  subject_template: "Action required for {customer_id}"
  body_template: |
    Hi {first_name},
    Your account has {issue_count} issue(s).
```

## Modes

| Mode | Behavior |
|---|---|
| `per_row` | One email per upstream row. All templates render `{column}` placeholders from the row. |
| `summary` | One email per run. `summary_template` can use `{row_count}` and `{table_md}` (markdown table of up to 50 rows). |

## Templates

`{column_name}` placeholders fill from the row dict. Missing keys yield empty string. Examples:

```yaml
to_template: "{user_email}"
cc_template: "manager-{region}@me.com,ops@me.com"
subject_template: "[{severity}] {customer_name} — issue {issue_id}"
body_template: |
  Hi {first_name},

  Customer {customer_id} ({plan}) has {issue_count} open issues.
  Total spend: ${total_spend}

  - Dagster
html_body_template: |
  <p>Hi <b>{first_name}</b>,</p>
  <p>Customer <code>{customer_id}</code> has <b>{issue_count}</b> open issues.</p>
```

If both `body_template` and `html_body_template` are set, the recipient gets a `multipart/alternative` email (plain text + HTML; the client picks).

## Attachments

```yaml
attachment_path_columns:
  - report_pdf_path
  - invoice_pdf_path
```

Values in those columns are read as local filesystem paths and attached. Missing files are skipped with a warning.

In `summary` mode, attachments are deduplicated across all rows.

## Auth

- **Gmail SMTP** (smtp.gmail.com:587): App password required. Same flow as IMAP — enable 2FA, generate app password at https://myaccount.google.com/apppasswords.
- **Outlook/O365 SMTP** (smtp.office365.com:587): Basic auth needs admin opt-in; otherwise use Microsoft Graph.
- **SES / SendGrid / Mailgun**: Set their SMTP relay creds. Each has provider-specific sender-verification requirements.
- **Self-hosted Postfix/exim**: usually port 25 or 587, plain or STARTTLS.

## Safety

- `dry_run: true` renders + logs every message without sending. Use to verify templates against real data.
- `max_send` caps emails per run — guardrail against runaway upstreams.
- The component never auto-retries — if delivery fails for a given recipient, the row is logged and counted in `messages_failed`. Add a separate retry sensor if you need that.

## Not a replacement for Dagster+ notifications

For *run-level* alerts (job failed, asset materialization failed, freshness violated, etc.) use Dagster+'s built-in alerting. This component is for **data-row-driven** outbound mail (one email per record in your upstream).
