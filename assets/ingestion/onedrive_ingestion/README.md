# OneDrive / SharePoint (Microsoft Graph) Ingestion

Ingest OneDrive/SharePoint file listings via Microsoft Graph (app-only auth) using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** This connector uses app-only client_credentials auth (application permissions, admin-consented) -- deliberately NOT the delegated/interactive 3-legged flow, since a Dagster pipeline can't complete an interactive consent screen. The permission model genuinely differs between app-only and delegated access, and between work/school (Azure AD) vs personal (Microsoft/MSA) accounts -- this connector only supports work/school accounts via app-only auth. Pagination uses @odata.nextLink; only the root children of one drive are listed (no recursive folder walk).


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `tenant_id` | required | Azure AD (Entra ID) tenant ID. |
| `client_id` | required | Azure AD app registration client ID (granted Files.Read.All or Sites.Read.All application permission, admin-consented). |
| `client_secret` | required | Azure AD app registration client secret. |
| `drive_id` | required | Target drive ID (a user's OneDrive or a SharePoint document library's drive ID). |
| `resources` | optional | Comma-separated list of resources to extract: files (root children of the configured drive). Default: `files` |

## Example
```yaml
type: dagster_component_templates.OneDriveIngestionComponent
attributes:
  asset_name: onedrive_ingestion
  tenant_id: "${AZURE_TENANT_ID}"
  client_id: "${AZURE_CLIENT_ID}"
  client_secret: "${AZURE_CLIENT_SECRET}"
  drive_id: "b!abc123..."
  resources: "files"
```
