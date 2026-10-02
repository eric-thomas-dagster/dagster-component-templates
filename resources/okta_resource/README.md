# `OktaResourceComponent`

Registers an `OktaResource` (OAuth2 `client_credentials` grant against an Okta API Service Integration) wrapping the Okta Users API for other components to use via `resource_key`.

## Auth: client_credentials via an API Service Integration, not the SSWS token used elsewhere in this repo

`okta_management_ingestion` (the read-side component) authenticates with a static `SSWS` API token. This resource deliberately uses a different, OAuth2-native flow instead: Okta's **API Service Integration** (a Service/machine-to-machine app type), which mints short-lived, scoped access tokens via `client_credentials` — the flow Okta documents as the one that can carry first-party Okta API scopes like `okta.users.manage`.

Token endpoint (`client_secret_basic`): `POST https://{org_url}/oauth2/v1/token`

```
Authorization: Basic base64(client_id:client_secret)
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&scope=okta.users.manage
```

Grant the Service app **only** the `okta.users.manage` scope — it covers every operation this resource performs (find/create/update/deactivate) and nothing more. Access tokens are cached in-memory (class-level, keyed by org + client_id env var) and refreshed automatically a little before expiry.

## What this resource exposes

| Method | Okta endpoint | Purpose |
|---|---|---|
| `find_user(identifier)` | `GET /api/v1/users/{id\|login\|email}` | Look up a user. Returns `None` on 404 instead of raising. |
| `create_user(profile, credentials=None, activate=True)` | `POST /api/v1/users` | Create a new user. |
| `update_user(user_id, profile)` | `POST /api/v1/users/{id}` | **Merge**-update profile attributes (unspecified fields untouched). |
| `deactivate_user(user_id, send_email=False)` | `POST /api/v1/users/{id}/lifecycle/deactivate` | Okta's distinct lifecycle endpoint — structurally separate from `update_user`. |
| `get_client()` | — | Escape hatch: an authenticated `requests.Session`. |

**There is no `delete_user` method.** Okta's Users API does expose `DELETE /api/v1/users/{id}` — but only on a user already in the `DEPROVISIONED` (deactivated) status, and it is a genuine, unrecoverable hard delete that Okta's own documentation recommends against for audit/compliance reasons. This resource never calls it, by design — see `okta_user_upsert`'s README "Safety" section.

## Pairs with

- **`okta_user_upsert`** — reverse-ETL sink built on top of this resource (the only intended consumer).
- **`okta_management_ingestion`** — the READ-side counterpart (dlt-based bulk pull of users/groups/apps via SSWS token; does not use this resource).

## Configuration

| Field | Required | Description |
|---|---|---|
| `resource_key` | optional (default `okta_resource`) | Key used to register this resource. |
| `org_url` | required | Your Okta org URL, e.g. `https://mycompany.okta.com`. |
| `client_id_env_var` | optional (default `OKTA_CLIENT_ID`) | Env var holding the Service Integration's Client ID. |
| `client_secret_env_var` | optional (default `OKTA_CLIENT_SECRET`) | Env var holding the Service Integration's Client Secret. |
| `scope` | optional (default `okta.users.manage`) | Space-separated OAuth scope(s) requested. |
| `token_path` | optional (default `/oauth2/v1/token`) | Token endpoint path, relative to `org_url`. |

## Example
```yaml
type: dagster_component_templates.OktaResourceComponent
attributes:
  resource_key: okta_resource
  org_url: "https://mycompany.okta.com"
  client_id_env_var: OKTA_CLIENT_ID
  client_secret_env_var: OKTA_CLIENT_SECRET
```
