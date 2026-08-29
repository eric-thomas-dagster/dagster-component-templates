# `SalesforceResourceComponent`

Self-contained **Salesforce REST API workhorse**. OAuth 2.0 password grant against a Connected App, retries on 401 (token refresh) / 429 / 5xx. Provides read + write convenience methods for downstream components — no dependency on any third-party Dagster integration package.

## Why this exists

Consumed by:
- **`salesforce_record_upsert`** — reverse-ETL sink using Salesforce's native External-ID upsert.
- Custom Dagster asset code that needs SOQL queries or SObject CRUD.

`salesforce_ingestion` (dlt-backed bulk pull) has its own dlt configuration and does NOT use this resource.

## Auth

Uses **OAuth 2.0 password grant** (headless — no browser required):

```
POST /services/oauth2/token
    grant_type=password
    client_id=<consumer_key>
    client_secret=<consumer_secret>
    username=<username>
    password=<password><security_token>   # concatenated
```

Returns `access_token` + `instance_url`. All subsequent API calls hit `{instance_url}/services/data/v{api_version}/sobjects/...` with `Authorization: Bearer <access_token>`.

Requires:
- A **Connected App** in Salesforce (Setup → App Manager → New Connected App).
  - Enable OAuth Settings + check *"Enable OAuth Settings"*.
  - Add OAuth scopes: `api`, `refresh_token`, `offline_access` (at minimum).
  - Under *"Grant Access Using"*, ensure *"Username-Password Flows"* is checked.
- **Consumer Key + Secret** from that Connected App (env vars).
- **Salesforce user credentials** with API access + the object permissions you need.
- **Security token** — required for IP-restricted orgs. Reset via *Personal Info → Reset My Security Token*.

## Auth path caveats

- **Password grant is being deprecated** by Salesforce for newly created orgs. Existing orgs continue to work. For future-proofing, **JWT Bearer** (this component's follow-up) is the modern headless path — server signs a JWT with a Connected App certificate, no password required.
- **`_dagster_platform_` / interactive OAuth flows** (Authorization Code, PKCE) DO NOT work in Dagster resources — the code-location process has no browser. That's why this component uses password grant instead.

## Example

```yaml
type: dagster_component_templates.SalesforceResourceComponent
attributes:
  resource_key: salesforce
  username: svc-account@mycompany.com

  password_env_var:        SF_PASSWORD
  security_token_env_var:  SF_SECURITY_TOKEN   # optional (IP-restricted orgs)
  consumer_key_env_var:    SF_CONSUMER_KEY
  consumer_secret_env_var: SF_CONSUMER_SECRET

  # 'login' for production, 'test' for sandbox, or 'mycompany.my' for custom domain
  domain: login
  api_version: "58.0"
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
- `composite_upsert(sobject, ext_id_field, records)` — bulk upsert up to 200 records per call.
- `delete_record(sobject, id)` — DELETE by Id.

## Pairs with

- **`salesforce_record_upsert`** — reverse-ETL sink using this resource's `composite_upsert` for high-throughput External-ID upserts.
- **`salesforce_ingestion`** — dlt-based bulk pull (uses its own dlt config, doesn't touch this resource).

## Roadmap

- **JWT Bearer flow** — modern headless auth using a Connected App certificate. Password grant works today but is being phased out for new orgs.
- **Bulk 2.0 API integration** — for upserts of >200 records, the Bulk 2.0 async job API can be 10-100x faster than the composite endpoint. Follow-up on `salesforce_record_upsert`.
