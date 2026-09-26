# `MailchimpResourceComponent`

Registers a `MailchimpResource` (API key auth) for other components to use via `resource_key`.

Beyond the raw `.get_client()` escape hatch (an authenticated `requests.Session`), this resource provides `.upsert_member(list_id, email, merge_fields=..., tags=..., status_if_new=...)` -- a native Mailchimp upsert via `PUT /lists/{list_id}/members/{subscriber_hash}`, where `subscriber_hash` is the lowercased, MD5-hashed email. Mailchimp creates the member if they don't exist, or updates their merge fields if they do.

## Pairs with

- **`mailchimp_member_upsert`** — reverse-ETL sink built on top of this resource.
- **`mailchimp_ingestion`** — the READ-side counterpart (dlt-based bulk pull; does not use this resource).

## Configuration

| Field | Required | Description |
|---|---|---|
| `resource_key` | optional (default `mailchimp`) | Key used to register this resource. |
| `api_key_env_var` | optional (default `MAILCHIMP_API_KEY`) | Env var holding a Mailchimp API key. |

## Example
```yaml
type: dagster_component_templates.MailchimpResourceComponent
attributes:
  resource_key: mailchimp
  api_key_env_var: MAILCHIMP_API_KEY
```
