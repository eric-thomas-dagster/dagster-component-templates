# `HelpScoutConversationUpsertComponent`

Reverse-ETL sink: create-or-update an upstream DataFrame into **Help Scout conversations**. A row with a `conversation_id` present **updates** the existing conversation (tags/note/status); otherwise a new conversation is **created**, which natively auto-upserts the underlying Customer record by email.

## When to use

- Push structured warehouse data (e.g. a billing flag, an escalation tag, a computed health score) into Help Scout conversations so support agents see it in-context, or to kick off a new conversation from an upstream event (an alert, a form submission, a workflow trigger).

## Why "create-or-update" and not search-then-write

Unlike ServiceNow/Freshdesk/Freshservice tickets, Help Scout conversations have no clean external-key search endpoint to upsert against. This sink instead uses an explicit `conversation_id_column` as the match key (same shape as `greenhouse_candidate_update`'s update-only pattern elsewhere in this repo) — but unlike Greenhouse, Help Scout DOES support creating brand-new conversations, so this is a genuine create-or-update:

- `conversation_id_column` value present on a row -> **UPDATE** that conversation (tags/note/status).
- value absent (or the field unset entirely) -> **CREATE** a new conversation.

## Native customer upsert-by-email

Creating a conversation with `customer_email_column` set to an email that doesn't match an existing Help Scout Customer **auto-creates** that Customer record — this is Help Scout's own documented behavior, not something this sink implements separately. There is no standalone "customer upsert" step here.

## Tags are a full replace on UPDATE, not additive

`tags_column` on an UPDATE row triggers `update_tags`, which **replaces the entire tag list** — any existing tag you don't include is removed. (On CREATE, tags are just included directly in the create body.)

## Prerequisites

1. A target `mailbox_id` (the destination mailbox for new conversations).
2. If you want update capability, a `conversation_id_column` sourced from a prior Help Scout ingestion or a previous run's output.

## Pairs with

- **`help_scout_resource`** — OAuth2 `client_credentials` auth (required).
- **`help_scout_ingestion`** — the READ-side counterpart (dlt-based bulk pull, uses a simpler static-token auth).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `help_scout_resource`) | Resource key registered by HelpScoutResourceComponent. |
| `mailbox_id` | required | Target Help Scout mailbox ID for new conversations. |
| `conversation_id_column` | optional | Present -> update; absent -> create. |
| `customer_email_column` | required | CREATE identifier; auto-upserts the Customer. |
| `customer_first_name_column` / `customer_last_name_column` | optional | CREATE only. |
| `subject_column` / `body_column` | required together | Needed for CREATE (both set or both unset). |
| `tags_column` | optional | CREATE: included. UPDATE: full replace. |
| `note_column` | optional | UPDATE only: posts an internal note. |
| `status_column` | optional | UPDATE only: patches status. |
| `conversation_type` | optional (default `email`) | Help Scout conversation type for new conversations. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.HelpScoutConversationUpsertComponent
attributes:
  asset_name: help_scout_conversations_mirror
  upstream_asset_key: warehouse_support_events
  resource_key: help_scout_resource
  mailbox_id: 85
  conversation_id_column: conversation_id
  customer_email_column: email
  subject_column: subject
  body_column: body
  tags_column: tags
  status_column: status
  group_name: reverse_etl
```
