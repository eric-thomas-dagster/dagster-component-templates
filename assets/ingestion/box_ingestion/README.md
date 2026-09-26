# Box Ingestion

Ingest Box file/folder listings and users using dlt's generic REST API source, authenticated via Client Credentials Grant (service account).

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Box's Client Credentials Grant (service account) is a genuine static server-to-server auth path -- no interactive OAuth needed. box_subject_id must be an enterprise ID (for full-enterprise access) or a specific user ID (for that user's content only); mismatches here are a common setup mistake. folder_items only lists ONE folder's direct children (no recursive walk) -- pass a specific folder_id to scope it, or leave unset to list the root.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `client_id` | required | Box app client ID (configured for Client Credentials Grant). |
| `client_secret` | required | Box app client secret. |
| `box_subject_type` | required | Box CCG subject type: 'enterprise' or 'user'. |
| `box_subject_id` | required | Box enterprise ID (if subject_type=enterprise) or user ID (if subject_type=user). |
| `folder_id` | optional | Box folder ID to list children of. Defaults to '0' (root) if unset. |
| `resources` | optional | Comma-separated list of resources to extract: folder_items (children of folder_id, default root), users. Default: `folder_items,users` |

## Example
```yaml
type: dagster_component_templates.BoxIngestionComponent
attributes:
  asset_name: box_ingestion
  client_id: "${BOX_CLIENT_ID}"
  client_secret: "${BOX_CLIENT_SECRET}"
  box_subject_type: "enterprise"
  box_subject_id: "${BOX_ENTERPRISE_ID}"
  resources: "folder_items,users"
```
