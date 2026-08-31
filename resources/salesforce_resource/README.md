# `SalesforceResourceComponent`

Self-contained **Salesforce REST API workhorse**. Two auth modes (password grant + JWT Bearer), retries on 401 (token refresh) / 429 / 5xx. Read + write convenience methods for downstream components — no dependency on any third-party Dagster integration package.

## Why this exists

Consumed by:
- **`salesforce_record_upsert`** — reverse-ETL sink using Salesforce's native External-ID upsert (composite + Bulk 2.0 modes).
- Custom Dagster asset code that needs SOQL queries or SObject CRUD.

`salesforce_ingestion` (dlt-backed bulk pull) has its own dlt configuration and does NOT use this resource.

## Auth modes

Pick one via `auth_mode`. Both are headless — no browser required.

### `auth_mode: password` (default) — OAuth 2.0 password grant

```
POST /services/oauth2/token
    grant_type=password
    client_id=<consumer_key>
    client_secret=<consumer_secret>
    username=<username>
    password=<password><security_token>   # concatenated
```

Simple. Works everywhere. **But Salesforce is deprecating password grant for newly created orgs** — for future-proofing, use JWT Bearer instead.

Requires: Connected App consumer key + secret, Salesforce user password, optional security token (IP-restricted orgs).

### `auth_mode: jwt_bearer` — OAuth 2.0 JWT Bearer flow

```
POST /services/oauth2/token
    grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer
    assertion=<signed JWT>
```

The component builds a JWT with `iss=consumer_key`, `sub=username`, `aud=https://{domain}.salesforce.com`, `exp=now+180s`, signs it RS256 with your private key, and exchanges it for an access token. **No password stored anywhere.**

Requires:
- A **Connected App** with a **Digital Certificate** uploaded (Setup → App Manager → the app → Digital Certificates).
- The **matching RSA private key** in PEM format, held in an env var (`private_key_pem_env_var`).
- Consumer key from the Connected App.
- The user in `username` must be pre-authorized for the Connected App (Setup → Manage → Manage Profiles / Permission Sets).

This is Salesforce's recommended headless auth for new orgs.

## Connected App setup

Common to both modes:

- Setup → App Manager → New Connected App.
- Enable OAuth Settings; add scopes `api`, `refresh_token`, `offline_access` (at minimum).

Password mode additionally requires:
- Under *"Grant Access Using"*, check *"Username-Password Flows"*.
- User's security token — reset via *Personal Info → Reset My Security Token*.

JWT Bearer mode additionally requires:
- Generate an RSA keypair (`openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout key.pem -out cert.pem`).
- Upload `cert.pem` under the Connected App's Digital Certificates section.
- Store `key.pem` contents in the env var referenced by `private_key_pem_env_var`.
- Pre-authorize the user in the Connected App's Manage Profiles/Permission Sets.

## Example (password grant, production org)

```yaml
type: dagster_community_components.SalesforceResourceComponent
attributes:
  resource_key: salesforce
  username: svc-account@mycompany.com
  auth_mode: password

  password_env_var:        SF_PASSWORD
  security_token_env_var:  SF_SECURITY_TOKEN   # optional (IP-restricted orgs)
  consumer_key_env_var:    SF_CONSUMER_KEY
  consumer_secret_env_var: SF_CONSUMER_SECRET

  domain: login          # 'login' (prod), 'test' (sandbox), 'mycompany.my' (custom)
  api_version: "58.0"
```

## Example (JWT Bearer)

```yaml
type: dagster_community_components.SalesforceResourceComponent
attributes:
  resource_key: salesforce
  username: svc-account@mycompany.com
  auth_mode: jwt_bearer

  consumer_key_env_var:    SF_CONSUMER_KEY
  private_key_pem_env_var: SF_PRIVATE_KEY_PEM   # full '-----BEGIN...-----' block

  domain: login
```

Set the env var to the whole PEM including begin/end markers:

```bash
export SF_PRIVATE_KEY_PEM="$(cat /path/to/key.pem)"
```

## Convenience methods

Read:
- `describe_sobject(sobject)` — field metadata for an SObject.
- `soql(query, batch_size, max_records)` — iterate every record from a SOQL query, transparently following `nextRecordsUrl`.
- `get_record(sobject, id)` — GET single by Id.
- `find_record(sobject, ext_id_field, ext_id_value)` — GET single by external Id.

Write:
- `create_record(sobject, body)` — POST.
- `update_record(sobject, id, body)` — PATCH by Id.
- `upsert_record(sobject, ext_id_field, ext_id_value, body)` — native External-ID upsert.
- `composite_upsert(sobject, ext_id_field, records)` — batch upsert up to 200 records per call.
- **`bulk_upsert(sobject, ext_id_field, records)` — Bulk 2.0 async ingest job**. 10-100x faster than composite for >200-row loads; Salesforce chunks + parallelizes server-side. Submits + polls to completion; returns aggregate counts.
- `delete_record(sobject, id)` — DELETE by Id.

## Pairs with

- **`salesforce_record_upsert`** — reverse-ETL sink using this resource. Toggle `use_bulk: true` for Bulk 2.0 mode on large loads.
- **`salesforce_ingestion`** — dlt-based bulk pull (uses its own dlt config, doesn't touch this resource).

## What doesn't work here

Interactive OAuth flows (Authorization Code, PKCE) DO NOT work in Dagster resources — the code-location process has no browser. Use `password` or `jwt_bearer`; both are headless.
