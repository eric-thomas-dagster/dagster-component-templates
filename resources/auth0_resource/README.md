# `Auth0ResourceComponent`

Registers an `Auth0Resource` (OAuth2 `client_credentials` grant against an Auth0 Machine-to-Machine application) wrapping the Auth0 Management API's user endpoints for other components to use via `resource_key`.

## Auth: client_credentials, not a user-facing flow

Token endpoint: `POST https://{tenant_domain}/oauth/token`

```json
{
  "client_id": "...",
  "client_secret": "...",
  "audience": "https://{tenant_domain}/api/v2/",
  "grant_type": "client_credentials"
}
```

This is the flow Auth0 documents for server-side/M2M access to the Management API (no end user, no browser redirect). The M2M application behind `client_id`/`client_secret` must be authorized for the Management API with scopes `read:users`, `create:users`, `update:users`. It does **not** need `delete:users` — this resource never requests or uses it.

Access tokens are cached in-memory (class-level, keyed by tenant + client_id env var) and refreshed automatically a little before expiry.

## What this resource exposes

| Method | Auth0 endpoint | Purpose |
|---|---|---|
| `find_user_by_email(email)` | `GET /api/v2/users-by-email` | Look up existing user(s) by email. Returns a list — Auth0 allows more than one user to share an email across different connections. |
| `create_user(payload)` | `POST /api/v2/users` | Create a new user. `payload` must include `connection` and `email`. |
| `update_user(user_id, payload)` | `PATCH /api/v2/users/{id}` | Update profile attributes. Never put `blocked` in this payload. |
| `set_blocked(user_id, blocked)` | `PATCH /api/v2/users/{id}` with **only** `{"blocked": ...}` | Auth0's documented block/unblock mechanism. |
| `get_client()` | — | Escape hatch: an authenticated `requests.Session`. |

**There is no `delete_user` method.** Auth0's Management API does expose `DELETE /api/v2/users/{id}` (a true, permanent hard delete), but this resource never calls it, by design — see `auth0_user_upsert`'s README "Safety" section.

## Pairs with

- **`auth0_user_upsert`** — reverse-ETL sink built on top of this resource (the only intended consumer).
- **`auth0_management_ingestion`** — the READ-side counterpart (dlt-based bulk pull of users/roles/organizations; does not use this resource).

## Configuration

| Field | Required | Description |
|---|---|---|
| `resource_key` | optional (default `auth0_resource`) | Key used to register this resource. |
| `tenant_domain` | required | Your Auth0 tenant domain, e.g. `mycompany.us.auth0.com`. |
| `client_id_env_var` | optional (default `AUTH0_CLIENT_ID`) | Env var holding the M2M application's Client ID. |
| `client_secret_env_var` | optional (default `AUTH0_CLIENT_SECRET`) | Env var holding the M2M application's Client Secret. |

## Example
```yaml
type: dagster_component_templates.Auth0ResourceComponent
attributes:
  resource_key: auth0_resource
  tenant_domain: "mycompany.us.auth0.com"
  client_id_env_var: AUTH0_CLIENT_ID
  client_secret_env_var: AUTH0_CLIENT_SECRET
```
