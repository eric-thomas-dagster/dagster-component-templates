# `ZohoCrmResourceComponent`

Self-contained **Zoho CRM REST API workhorse**. Real OAuth2 **refresh-token** grant (not a static access token) -- safe for long-running reverse-ETL processes, since Zoho access tokens expire in ~1 hour. Retries on 401 (token refresh) / 429 / 5xx. `.get()` / `.post()` / `.patch()` convenience methods plus `.upsert()` wrapping Zoho's native **Upsert Records** API.

## Why this exists

`zoho_crm_ingestion` (this repo's dlt-based bulk read) takes a plain static `access_token` string -- fine for a short-lived pull, but Zoho access tokens expire in ~1 hour, which is too short for a longer-running or scheduled reverse-ETL write process. This resource instead does a real OAuth2 refresh-token exchange and caches the resulting access token in memory, refreshing automatically.

Consumed by:
- **`zoho_crm_record_upsert`** -- reverse-ETL sink using Zoho's native Upsert Records API.
- Custom Dagster asset code that needs arbitrary Zoho CRM REST calls (`.get()` / `.post()` / `.patch()`).

## Auth: OAuth2 refresh-token grant

```
POST https://accounts.zoho.{region}/oauth/v2/token
    grant_type=refresh_token
    client_id=<client_id>
    client_secret=<client_secret>
    refresh_token=<refresh_token>
```

Response: `{"access_token": ..., "expires_in": 3600, "api_domain": "https://www.zohoapis.com", "token_type": "Bearer"}`.

**One-time setup** (outside this resource): register a Self Client or Server-based Application at Zoho's [API Console](https://api-console.zoho.com), then perform a one-time `authorization_code` exchange (Zoho's console supports generating a Self Client grant token directly, no browser redirect needed) to mint the initial `refresh_token`. Seed that into the env var named by `refresh_token_env_var`.

### Regional data centers

Zoho hosts live in multiple regions. `accounts_domain` selects the OAuth accounts host:

| `accounts_domain` | Accounts host |
|---|---|
| `com` (default) | `accounts.zoho.com` (US) |
| `eu` | `accounts.zoho.eu` |
| `in` | `accounts.zoho.in` |
| `com.au` | `accounts.zoho.com.au` |
| `jp` | `accounts.zoho.jp` |
| `com.cn` | `accounts.zoho.com.cn` |
| `ca` | `accounts.zohocloud.ca` **(note: NOT `accounts.zoho.ca`)** |

### API host: derived, not guessed

Rather than deriving the *API* host from the accounts region, this resource uses the `api_domain` that Zoho's own token response returns -- that's Zoho's documented, canonical way to resolve which API host your data center uses. `api_domain` can still be explicitly overridden in config if needed, but normally you only need to set `accounts_domain` (for the OAuth exchange) and the resource figures out the rest.

### Refresh tokens: long-lived and reusable

Unlike Outreach (whose refresh tokens rotate on every exchange), **Zoho refresh tokens do not rotate** -- the same refresh token is reused indefinitely across every access-token exchange, and refresh tokens do not expire on their own (they're only invalidated by explicit revocation, a password change with API-token termination enabled, or exceeding Zoho's 20-active-refresh-tokens-per-client-id cap). This means a stable, long-lived process only ever needs the one refresh_token seeded once.

Zoho does rate-limit token *generation*, though: at most 10 access-token exchanges per refresh token per 10-minute window, and at most 15 "active" access tokens retained per refresh token (the 16th invalidates the oldest). This resource caches the access token in-memory per instance and only refreshes ~60s before the ~3600s expiry, which keeps normal operation well under that limit.

## Native Upsert Records API

```
POST {api_domain}/crm/{version}/{module_api_name}/upsert
    {"data": [...up to 100 records...], "duplicate_check_fields": [...]}
```

- **Max 100 records per request** (Zoho-documented hard limit; `.upsert()` raises `ValueError` if you exceed it -- chunk upstream of the call).
- `duplicate_check_fields` -- Zoho field API names used to detect duplicates, e.g. `["Email"]` for Leads/Contacts (Zoho's own system-defined duplicate-check field for those modules). If omitted, Zoho falls back to system-defined duplicate-check fields, then user-defined unique fields, in that order. **Zoho's docs do not publish a hard maximum field count** for this array (its own examples show 1-2 fields, e.g. `["Email", "Mobile"]`) -- this resource does not enforce an artificial cap, but fields here should be ones marked unique/mandatory on the module for duplicate detection to actually work.
- Response: `{"data": [{"code": "SUCCESS", "duplicate_field": "Email", "action": "insert"|"update", "status": "success"|"error", "message": "...", "details": {"id": ..., ...}}, ...]}` -- one entry per input record, in the same order. `.upsert()` returns this `data` list directly.

## API version

Defaults to `v8` -- Zoho's current REST API version per its own developer docs, and the version whose docs describe the `duplicate_check_fields` upsert semantics this resource relies on. `zoho_crm_ingestion`'s dlt-based read in this repo uses `v3` for a simple bulk pull; `v3` still works for upsert too, but `v8` is what Zoho's current documentation describes, so it's the better default here. Fully overridable via `api_version`.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"zoho_crm"` | Resource key. Other components reference it via this name. |
| `client_id_env_var` | `str` | `"ZOHO_CLIENT_ID"` | Env var holding the Zoho OAuth Client ID. |
| `client_secret_env_var` | `str` | `"ZOHO_CLIENT_SECRET"` | Env var holding the Zoho OAuth Client Secret. |
| `refresh_token_env_var` | `str` | `"ZOHO_REFRESH_TOKEN"` | Env var holding the long-lived OAuth refresh token. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `api_version` | `str` | `"v8"` | Zoho CRM REST API version. |
| `request_timeout_seconds` | `int` | `60` | Per-request timeout in seconds. |
| `max_retries` | `int` | `3` | Retry attempts on 429 / 5xx (exponential backoff, capped at 10s) + one on 401 (token refresh). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `accounts_domain` | `str` | `"com"` | Zoho accounts data-center region: 'com' (US), 'eu', 'in', 'com.au', 'jp', 'com.cn', or 'ca' (zohocloud.ca). |
| `api_domain` | `str` | — | Override the Zoho API host. Leave unset to use the api_domain Zoho's OAuth token response returns for your data center. |

[//]: # (FIELDS:END)

## Example

```yaml
type: dagster_component_templates.ZohoCrmResourceComponent
attributes:
  resource_key: zoho_crm
  client_id_env_var:     ZOHO_CLIENT_ID
  client_secret_env_var: ZOHO_CLIENT_SECRET
  refresh_token_env_var: ZOHO_REFRESH_TOKEN
  accounts_domain: com
  api_version: v8
```

## Convenience methods

- `get(path, params)` -- GET, returns parsed JSON.
- `post(path, json_body)` -- POST, returns parsed JSON.
- `patch(path, json_body)` -- PATCH, returns parsed JSON.
- `upsert(module_api_name, records, duplicate_check_fields)` -- native Upsert Records call (max 100 records/call).

All four share the same retry-on-401/429/5xx `_request()` core and the same cached access token.

## Pairs with

- **`zoho_crm_record_upsert`** -- reverse-ETL sink using this resource's `.upsert()`.
- **`zoho_crm_ingestion`** -- dlt-based bulk pull (static access token, doesn't use this resource).

## What doesn't work here

Interactive OAuth flows (Authorization Code with a live browser redirect) don't run in a Dagster resource -- the code-location process has no browser. Mint the `refresh_token` once, out of band (Zoho's API Console supports a no-redirect "Self Client" grant specifically for this), then this resource handles every subsequent refresh headlessly.
