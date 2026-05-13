# OData Ingestion Component

Generic OData v2 / v4 client → pandas DataFrame. OData is the dominant machine-interface protocol across enterprise ERP today — it's not a SAP-only thing.

## What this covers

| Vendor | Products |
|---|---|
| **SAP** | S/4HANA Cloud + on-prem, SuccessFactors, Datasphere, Commerce Cloud (Hybris), Marketing/Service/Customer Data Cloud, Concur (newer endpoints), Ariba (newer endpoints), any SAP NetWeaver Gateway service |
| **Microsoft** | Dynamics 365 / Dataverse (Sales, Customer Service, Field Service, Marketing), Microsoft Graph (Outlook, Teams, OneDrive, Azure AD), SharePoint Online, Business Central, Project Server / Online, Power BI REST API |
| **Oracle** | Fusion Cloud Applications (Financials, HCM, SCM) |
| **Other ERP** | Epicor ERP (v10+), IFS Cloud, Infor M3 / ION, JD Edwards (newer Tools) |
| **Public** | `services.odata.org/V4/Northwind/Northwind.svc/` (v4 test), `services.odata.org/V2/Northwind/Northwind.svc/` (v2 test) |

## Why one component instead of N

OData is a single protocol with consistent semantics: `$filter`, `$select`, `$expand`, `$top`, pagination via `__next` (v2) / `@odata.nextLink` (v4). One client handles all of them. The differences between vendors are at the **edges**: service URL, auth flow, header conventions, entity-set naming. Those are config, not new code.

The SAP-product walkthroughs in `dagster-community-components-cli/examples/` show product-specific config for S/4HANA, SuccessFactors, and Datasphere.

## How OData isn't old

It feels old because SharePoint OData specifically is old — SharePoint is a 20-year-old product. The protocol itself: OData v1 (2007, Microsoft), v4 (OASIS 2014, ISO/IEC 2016). SAP's API Business Hub today lists thousands of OData services and zero GraphQL. Microsoft Graph is OData v4 under the hood. It's the enterprise-ERP lingua franca.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_name` | `str` | yes | Dagster asset name |
| `service_url` | `str` | yes | Base service URL (no entity set) |
| `entity_set` | `str` | yes | Entity collection appended to service_url |
| `odata_version` | `str` | no | `v2` (default) or `v4` |
| `select` / `filter` / `expand` / `orderby` / `top` | `str` / `int` | no | `$select` / `$filter` / `$expand` / `$orderby` / `$top` |
| `extra_query` | `dict` | no | Other `$<key>` query options |
| `auth_type` | `str` | no | `basic` (default) / `bearer` / `none` |
| `auth_username_env_var` / `auth_password_env_var` | `str` | conditional | For `basic` |
| `auth_token_env_var` | `str` | conditional | For `bearer` |
| `extra_headers` | `dict` | no | CSRF, sap-client, cookies |
| `verify_ssl` | `bool` | no | Default true; set false for self-signed on-prem |
| `max_pages` | `int` | no | Safety cap (default 1000) |
| `timeout_seconds` | `int` | no | Default 120 |
| Standard fields | | | partition / freshness / owners / tags / deps / retry — same convention as every other component |

## Quick smoke test (no credentials needed)

```yaml
type: dagster_component_templates.ODataIngestionComponent
attributes:
  asset_name: northwind_customers
  service_url: https://services.odata.org/V4/Northwind/Northwind.svc
  entity_set: Customers
  odata_version: v4
  auth_type: none
  top: 5
```

Run `uv run dg launch --assets northwind_customers` and you should get 5 rows back. Useful for verifying the component is wired right before pointing it at a real SAP/Dynamics tenant.

## Partition templating

`{partition_key}` is substituted into `$filter` at run time:

```yaml
filter: CreationDate ge datetime'{partition_key}T00:00:00'
partition_type: daily
partition_start: '2024-01-01'
```

## SAP-specific notes

- **CSRF**: most S/4HANA write APIs require fetching an `x-csrf-token` first. This component is read-only; if you need writes, see `rest_api_fetcher` + a sibling token-fetch step.
- **sap-client header**: pass via `extra_headers: {sap-client: '100'}` to route to a specific client.
- **basic auth on cloud**: S/4HANA Cloud supports basic auth for "Communication User"-type users; production deployments typically use OAuth 2.0 → bearer token (set `auth_type: bearer`).
- **Datasphere consumption endpoint**: requires a JWT in `Authorization: Bearer <token>` form fetched from XSUAA. Run a sidecar token-refresh step or feed via `auth_token_env_var`.

## See also

- [`rest_api_fetcher`](../rest_api_fetcher/) — generic non-OData REST fetcher
- [`openapi_asset`](../openapi_asset/) — schema-driven ingestion from any OpenAPI service
- SAP-product walkthroughs in `dagster-community-components-cli/examples/`
