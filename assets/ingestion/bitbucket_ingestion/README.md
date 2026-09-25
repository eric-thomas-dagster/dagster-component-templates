# Bitbucket Ingestion

Ingest Bitbucket Cloud repository data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `workspace` | required | Bitbucket workspace slug. |
| `email` | required | Atlassian account email for HTTP Basic auth. |
| `api_token` | required | Atlassian API token (scoped), used as the Basic auth password. |
| `repo_slug` | optional | Repository slug. Required for pullrequests/commits/issues resources; not needed for repositories. |
| `resources` | optional | Comma-separated list of resources to extract: repositories (workspace-level); pullrequests, commits, issues (require repo_slug). Default: `repositories` |

## Example
```yaml
type: dagster_component_templates.BitbucketIngestionComponent
attributes:
  asset_name: bitbucket_ingestion
  workspace: "my-team"
  email: "${BITBUCKET_EMAIL}"
  api_token: "${BITBUCKET_API_TOKEN}"
  repo_slug: "my-repo"
  resources: "repositories"
```
