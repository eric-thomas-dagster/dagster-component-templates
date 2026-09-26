# Egnyte Ingestion

Ingest Egnyte file/folder listings, users, and groups using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Egnyte's OAuth2 flow (authorization_code for public apps, historically password grant for internal apps) means there is no simple always-static-token path for every integration type -- this connector assumes you already hold a valid access_token and does not implement token refresh. The pubapi endpoint paths/response shapes (users/groups/fs) were recalled from general knowledge, not independently confirmed against live docs this session.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `domain` | required | Egnyte domain (for mycompany.egnyte.com). |
| `access_token` | required | OAuth2 access token from Egnyte's authorization flow. Egnyte tokens issued to internal/first-party apps are often long-lived; verify your token's actual expiry. |
| `folder_path` | optional | Folder path to list files/folders from (enables the files resource), e.g. '/Shared/Reports'. |
| `resources` | optional | Comma-separated list of resources to extract: users, groups, files (requires folder_path). Default: `users,groups` |

## Example
```yaml
type: dagster_component_templates.EgnyteIngestionComponent
attributes:
  asset_name: egnyte_ingestion
  domain: "mycompany"
  access_token: "${EGNYTE_ACCESS_TOKEN}"
  resources: "users,groups"
```
