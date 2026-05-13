# OData Resource Component

Register an OData service connection once and let multiple components reference it.

Same pattern as [`sap_hana_resource`](../sap_hana_resource/) — the resource holds the base URL + auth config + headers, and exposes helpers that downstream components use to make requests.

## When to use a resource vs. inline config

| Pattern | Use when |
|---|---|
| **Inline config in each component** | One-off pipeline, one entity, one place that needs OData |
| **`odata_resource` + components that reference it** | Multiple components pulling from the same S/4HANA tenant — auth + base URL + headers configured once |

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `resource_key` | `str` | no | Default `odata`. Use distinct keys for multiple tenants (e.g. `sap_s4`, `sap_sf`) |
| `service_url` | `str` | yes | Base service URL |
| `odata_version` | `str` | no | `v2` (default) or `v4` |
| `auth_type` | `str` | no | `basic` (default) / `bearer` / `oauth_resource` / `none` |
| `auth_username_env_var` / `auth_password_env_var` | `str` | conditional | For `basic` |
| `auth_token_env_var` | `str` | conditional | For `bearer` |
| `oauth_resource_key` | `str` | conditional | For `oauth_resource` — references an `oauth_token_resource` |
| `sap_client` | `str` | no | Sets the `sap-client` header (e.g. `'100'`). SAP-only. |
| `csrf_fetch_path` | `str` | no | Path to fetch CSRF token from on session start (for write APIs) |
| `verify_ssl` | `bool` | no | Default true |
| `timeout_seconds` | `int` | no | Default 120 |

## Usage from a custom asset

```python
from dagster import asset

@asset(required_resource_keys={"sap_s4_odata"})
def business_partners(context):
    odata = context.resources.sap_s4_odata
    s = odata.session()           # pre-configured requests.Session
    r = s.get(odata.url("A_BusinessPartner") + "?$top=10&$format=json",
              timeout=odata.timeout_seconds)
    r.raise_for_status()
    return r.json()["d"]["results"]
```

## Usage from OData family components

The `odata_ingestion`, `dataframe_to_odata`, and `odata_check` components currently take auth inline (env-var-backed fields) rather than reading a resource. To wire them to this resource explicitly, copy the resource's auth settings into the component config. Future versions may add a `resource_key` field that reads directly from this resource.

## CSRF handling for write APIs

SAP S/4HANA write APIs require a `x-csrf-token` on POST/PUT/DELETE. Configure `csrf_fetch_path`:

```yaml
attributes:
  csrf_fetch_path: $metadata
```

The resource fetches a token from `<service_url>/$metadata` on session start (with header `x-csrf-token: fetch`) and caches it on the session for the rest of the run. Read-only ingestion doesn't need this.

## Headless OAuth (Concur / Ariba / Datasphere)

For OAuth-token flows, use `oauth_token_resource` alongside this one:

```yaml
# 1. Configure the token resource:
type: dagster_component_templates.OAuthTokenResourceComponent
attributes:
  resource_key: sap_concur_token
  token_endpoint: https://www.concursolutions.com/oauth2/v0/token
  grant_type: refresh_token
  client_id_env_var: CONCUR_CLIENT_ID
  client_secret_env_var: CONCUR_CLIENT_SECRET
  refresh_token_env_var: CONCUR_REFRESH_TOKEN
```

```yaml
# 2. Reference it from this resource:
type: dagster_component_templates.ODataResourceComponent
attributes:
  resource_key: sap_concur_odata
  service_url: https://www.concursolutions.com/odata/...
  auth_type: oauth_resource
  oauth_resource_key: sap_concur_token
```

In your asset, read the token from the OAuth resource and set the `Authorization` header on each request.

## See also

- [`odata_ingestion`](../../assets/ingestion/odata_ingestion/) — the ingestion side
- [`dataframe_to_odata`](../../assets/sinks/dataframe_to_odata/) — the sink side
- [`oauth_token_resource`](../oauth_token_resource/) — pairs with this for OAuth flows
- [`sap_hana_resource`](../sap_hana_resource/) — same pattern, HANA SQL
