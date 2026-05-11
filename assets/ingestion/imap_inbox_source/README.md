# IMAP Inbox Source

Pull emails from any IMAP-compliant mailbox (Gmail, Outlook/O365, Yahoo, FastMail, Dovecot, …) and emit one DataFrame row per message.

```yaml
type: dagster_component_templates.ImapInboxSourceComponent
attributes:
  asset_name: inbound_emails
  host: imap.gmail.com
  port: 993
  username_env_var: IMAP_USER
  password_env_var: IMAP_APP_PASSWORD
  mailbox: INBOX
  search_criteria: UNSEEN
  max_messages: 100
```

## Columns produced

| Column | Type | Notes |
|---|---|---|
| `uid` | int | IMAP UID |
| `message_id` | str | RFC-5322 Message-ID |
| `in_reply_to` | str | Thread parent |
| `subject` | str | RFC-2047 decoded |
| `from` / `to` / `cc` / `reply_to` | str | Headers (decoded) |
| `sent_at` | datetime (UTC) | Parsed Date header |
| `fetched_at` | datetime (UTC) | When this asset ran |
| `body_text` | str | text/plain part (or HTML-stripped fallback) |
| `body_html` | str | Only if `include_body_html: true` |
| `attachments_count` | int | # of attachments (not their content) |

## Search criteria

IMAP `SEARCH` expressions — passed verbatim. Common examples:

```yaml
search_criteria: UNSEEN                      # everything not yet flagged \Seen
search_criteria: ALL                         # everything in the folder
search_criteria: 'SINCE 01-Jan-2025'         # date filter
search_criteria: '(FROM "alerts@example.com")'
search_criteria: 'SUBJECT "invoice"'
search_criteria: '(UNSEEN FROM "vendor@")'   # combine
```

## Auth

Provider-specific:

- **Gmail**: Use an **app password** (not your account password). Enable 2FA, then create an app password at https://myaccount.google.com/apppasswords. Set `IMAP_USER` to your address, `IMAP_APP_PASSWORD` to the 16-character app password.
- **Outlook/O365**: Basic auth for IMAP is disabled by default since 2022 — your tenant admin must enable it, OR use Microsoft Graph (different component).
- **Yahoo / FastMail / iCloud**: All support app passwords; flow similar to Gmail.
- **Self-hosted (Dovecot/Cyrus)**: Just username + password.

## Idempotency

To advance through the inbox without re-fetching the same messages:

```yaml
search_criteria: UNSEEN
mark_read: true     # flips \Seen on each fetched message
```

The next run only sees newly-arrived messages.

## Body decoding

- Prefers `text/plain` over `text/html` for multipart messages.
- Handles 7-bit, quoted-printable, base64.
- Falls back to HTML→text via cheap tag-strip if only HTML body is present (controlled by `fallback_html_to_text`).
- Set `include_body_html: true` to emit the raw HTML as a separate column.

## Caveats

- Attachments are counted, not extracted. For attachment extraction add a downstream transform that re-parses `body_text` against the message — or use Microsoft Graph / Gmail API instead.
- The IMAP protocol doesn't provide great pagination; for very large mailboxes prefer `search_criteria` with a `SINCE` window plus periodic runs.
- BODY-PEEK semantics aren't used by default — Python's `imaplib.fetch(RFC822)` may set `\Seen` on some servers regardless of `mark_read`. If you need strict "don't touch flags," use a server that supports `BODY.PEEK[]`.
