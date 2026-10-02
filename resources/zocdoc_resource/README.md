# `ZocdocResourceComponent`

Registers a `ZocdocResource` wrapping Zocdoc's Developer Platform API using the **OAuth2 client_credentials** grant — the documented machine-to-machine flow for a backend service with no end-user login involved.

## Partner-gated — read this first

Zocdoc's API is **not self-serve**. A Client ID/Secret pair does nothing until Zocdoc has reviewed and approved your integration. There is no sandbox you can spin up unilaterally — apply through Zocdoc's partner program first. See https://api-docs.zocdoc.com/guides/faqs.

Zocdoc's API surface is split in two:
- **Patient-booking surface** — find provider availability, book/cancel/reschedule appointments on a patient's behalf.
- **Provider-scheduling surface** — create/replace availability (timeslots) on behalf of a provider, manage appointments, receive webhooks when real bookings happen.

This resource authenticates against both; which endpoints you're approved for is controlled by Zocdoc, not by this code.

## Verified API facts (api-docs.zocdoc.com, 2026)

- **Auth**: OAuth2 `client_credentials` (machine-to-machine) or `authorization_code` with PKCE (user login). This resource implements `client_credentials` only.
- **Token endpoint**: `POST https://auth.zocdoc.com/oauth/token` (production) / `POST https://auth-api-developer-sandbox.zocdoc.com/oauth/token` (sandbox).
- **Token request body**: `{client_id, client_secret, grant_type: "client_credentials", audience}`, where `audience` is the base API URL for the chosen environment.
- **Base API URLs**: `https://api-developer.zocdoc.com/` (production) / `https://api-developer-sandbox.zocdoc.com/` (sandbox).
- **Token usage**: `Authorization: Bearer <token>` header on every resource request.
- **Token expiry**: 60 minutes. This resource caches the token and refetches ~60s early rather than racing expiry (same pattern as `auth0_resource`).

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"zocdoc_resource"` | Key used to register this resource. |
| `environment` | `str` | — (required) | `"sandbox"` or `"production"`. No default — an environment must be chosen deliberately given production carries real PHI. |
| `client_id_env_var` | `str` | `"ZOCDOC_CLIENT_ID"` | Env var holding the OAuth2 Client ID. |
| `client_secret_env_var` | `str` | `"ZOCDOC_CLIENT_SECRET"` | Env var holding the OAuth2 Client Secret. |
| `scope` | `Optional[str]` | `None` | Optional OAuth2 scope (e.g. `offline_access` for a refresh token). Most `client_credentials` integrations leave this unset. |

## Example

```yaml
type: dagster_component_templates.ZocdocResourceComponent
attributes:
  resource_key: zocdoc_resource
  environment: sandbox
  client_id_env_var: ZOCDOC_CLIENT_ID
  client_secret_env_var: ZOCDOC_CLIENT_SECRET
```

## PHI note

This resource itself never logs or returns anything beyond the bearer token and an authenticated `requests.Session` — it has no knowledge of what any downstream component does with the response bodies it fetches. The PHI-safety guarantees (no data preview in metadata/logs/errors) live in the two components built against this resource:

- `assets/ingestion/zocdoc_appointments_ingestion` — read side
- `assets/reverse_etl/zocdoc_availability_upsert` — write side

See each of their READMEs' "PHI Safety" sections for the full design.

## Pairs with

- `zocdoc_appointments_ingestion` — read side (booked appointments → DataFrame).
- `zocdoc_availability_upsert` — write side (DataFrame → provider timeslots).
