# `RampResourceComponent`

Registers a `RampResource` (OAuth2 `client_credentials` grant against a Ramp Developer App) wrapping Ramp's Developer API for other components to use via `resource_key`.

## Auth: real client_credentials token exchange, not a pasted static token

`ramp_ingestion` (the read-side component) takes a raw `access_token` field — it expects you to obtain and rotate that token yourself outside Dagster. This resource instead performs the real OAuth2 **client_credentials** exchange Ramp's docs describe, with in-memory caching and automatic refresh before expiry — confirmed directly against `https://docs.ramp.com` (not assumed):

> Ramp uses OAuth 2.0 for secure API access... Ramp authenticates requests to `/developer/v1/token` using your client ID and client secret, typically with HTTP Basic Auth.

Token endpoint: `POST {base_url}/developer/v1/token`

```
Authorization: Basic base64(client_id:client_secret)
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&scope=reimbursements:write cards:write
```

Client Credentials access tokens normally last **10 days** (864,000s) per Ramp's docs — this resource reads the real `expires_in` from the token response rather than hard-coding that.

## Base URLs

| Environment | Standard Developer API | Vault API (see below) |
|---|---|---|
| Production | `https://api.ramp.com` | `https://vault-api.ramp.com` |
| Sandbox | `https://demo-api.ramp.com` | `https://demo-vault-api.ramp.com` |

Ramp requires a **separate app registration** (separate client_id/client_secret) per environment — a sandbox app and a production app are not the same credentials with a different base_url.

## What this resource exposes

| Method | Ramp endpoint | Scope needed |
|---|---|---|
| `create_mileage_reimbursement(...)` | `POST /developer/v1/reimbursements/mileage` | `reimbursements:write` |
| `upload_reimbursement_receipt(...)` | `POST /developer/v1/reimbursements/submit-receipt` (multipart) | `reimbursements:write` |
| `create_virtual_card(...)` | `POST .../cards/vault` ("Create a spend limit and retrieve sensitive card details") | `cards:read_vault` + `limits:write` + `funds:write` (+ optionally `users:read`) |
| `update_physical_card(...)` | `PATCH /developer/v1/cards/physical/{card_id}` | `cards:write` |
| `get_client()` | — | Escape hatch: an authenticated `requests.Session`. |

## The Vault API constraint — read before using `create_virtual_card`

Ramp has **no endpoint to create a virtual card by itself.** The only documented way to mint a virtual card with a retrievable PAN/CVV is the Vault API (`POST /developer/v1/cards/vault`), and Ramp gates it explicitly:

> Ramp reviews your use case, security controls, and PCI handling before the Vault API can return full PANs and CVVs in production. **All customers can use the Vault API in Sandbox.** Submit a Developer API support ticket to begin the review.

In practice:
- **Sandbox** (`https://demo-api.ramp.com`, scopes `cards:read_vault`/`limits:write`/`funds:write` enabled on your app): works immediately for every Ramp developer.
- **Production**: requires a manual Ramp approval review (open a Developer API support ticket) before this call will succeed — expect it to fail with a permissions error until that review completes, independent of whether your app has the scopes toggled on.

This is a genuine Ramp-side access-tier gate confirmed directly from Ramp's docs, not a limitation of this resource. Build/test against Sandbox; budget time for Ramp's review before relying on this in production.

Ramp's guides additionally describe a dedicated Vault API host (`vault-api.ramp.com` prod / `demo-vault-api.ramp.com` sandbox) reachable at the shorter path `/cards/vault` (no `/developer/v1` prefix) as an alternative to the standard host's `/developer/v1/cards/vault`. Set `vault_base_url` only if Ramp's onboarding tells you your app's Vault grant is issued against that dedicated host.

**Security:** the Vault API response includes a full PAN and CVV. Never log or persist them — `create_virtual_card` returns Ramp's raw response so the caller decides what to keep, but `ramp_reimbursement_card_write` (the paired reverse-ETL component) redacts both before anything reaches Dagster metadata or logs.

## No spend-limit update, by design (matches reality, not a convenience gap)

There is no `update_spend_limit`-style method here, and the task that commissioned this resource initially assumed one existed — it doesn't. Ramp's Developer API documents exactly one way to modify an existing card: `PATCH /developer/v1/cards/physical/{card_id}`, which accepts only `display_name`, `fund_id`, and `automatic_routing_enabled`. There is no field, anywhere in Ramp's public API, to change a card's spend limit or spending restrictions after it has been created — not for physical cards, not for virtual cards. `update_physical_card` reflects exactly that real, narrower surface.

## Pairs with

- **`ramp_reimbursement_card_write`** — reverse-ETL sink built on top of this resource.
- **`ramp_ingestion`** — the READ-side counterpart (dlt-based bulk pull via a pre-minted static bearer token; does not use this resource).

## Configuration

| Field | Required | Description |
|---|---|---|
| `resource_key` | optional (default `ramp_resource`) | Key used to register this resource. |
| `client_id_env_var` | optional (default `RAMP_CLIENT_ID`) | Env var holding your Ramp Developer App's Client ID. |
| `client_secret_env_var` | optional (default `RAMP_CLIENT_SECRET`) | Env var holding your Ramp Developer App's Client Secret. |
| `base_url` | optional (default `https://api.ramp.com`) | Ramp Developer API base URL (`https://demo-api.ramp.com` for Sandbox). |
| `vault_base_url` | optional | Dedicated Vault API host override — see above. Leave unset unless Ramp tells you otherwise. |
| `scope` | optional (default `reimbursements:write cards:write`) | Space-separated OAuth scope(s) requested at token time. |
| `token_path` | optional (default `/developer/v1/token`) | Token endpoint path, relative to `base_url`. |

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"ramp_resource"` | Key used to register this resource. Other components reference it via resource_key. |
| `client_id_env_var` | `str` | `"RAMP_CLIENT_ID"` | Env var holding your Ramp Developer App's Client ID. |
| `client_secret_env_var` | `str` | `"RAMP_CLIENT_SECRET"` | Env var holding your Ramp Developer App's Client Secret. |
| `base_url` | `str` | `"https://api.ramp.com"` | Ramp Developer API base URL ('https://demo-api.ramp.com' for Sandbox). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `vault_base_url` | `str` | — | Optional dedicated Vault API host (see RampResource docstring). Leave unset unless Ramp tells you otherwise. |
| `scope` | `str` | `"reimbursements:write cards:write"` | Space-separated OAuth scope(s) requested at token time. |
| `token_path` | `str` | `"/developer/v1/token"` | Token endpoint path, relative to base_url. |

[//]: # (FIELDS:END)

## Example
```yaml
type: dagster_component_templates.RampResourceComponent
attributes:
  resource_key: ramp_resource
  client_id_env_var: RAMP_CLIENT_ID
  client_secret_env_var: RAMP_CLIENT_SECRET
  base_url: "https://api.ramp.com"
  scope: "reimbursements:write cards:write"
```
