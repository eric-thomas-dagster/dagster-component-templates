# Microsoft Dynamics 365 (Dataverse) Ingestion

Ingest Microsoft Dynamics 365 CRM data (accounts, contacts, opportunities, leads) via the Dataverse Web API (OData v4) using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Dynamics 365's Dataverse Web API is well-documented OData v4 (the auth flow, base_url pattern, and 'value'-wrapped response shape are high confidence), but real-world setup complexity is genuinely high: the Azure AD app registration needs an Application User created in Dynamics with the right security role, separate from the app registration itself. Pagination uses @odata.nextLink cursors -- this connector does not independently confirm dlt's default paginator auto-detects that link style; verify against a live tenant with a large dataset before relying on complete pulls. No resource is bound to the partition window (OData $filter date-range syntax was not implemented here to avoid stacking additional unverified complexity onto an already-complex setup).


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `org_url` | required | Your Dynamics 365 organization URL. |
| `tenant_id` | required | Azure AD (Entra ID) tenant ID. |
| `client_id` | required | Azure AD app registration client ID (with API permissions granted for Dataverse). |
| `client_secret` | required | Azure AD app registration client secret. |
| `resources` | optional | Comma-separated list of resources to extract: accounts, contacts, opportunities, leads. Default: `accounts,contacts` |

## Example
```yaml
type: dagster_component_templates.DynamicsCrmIngestionComponent
attributes:
  asset_name: dynamics_crm_ingestion
  org_url: "https://myorg.crm.dynamics.com"
  tenant_id: "${DYNAMICS_TENANT_ID}"
  client_id: "${DYNAMICS_CLIENT_ID}"
  client_secret: "${DYNAMICS_CLIENT_SECRET}"
  resources: "accounts,contacts"
```
