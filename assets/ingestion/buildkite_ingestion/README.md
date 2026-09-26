# Buildkite Ingestion

Ingest Buildkite organization, pipeline, and build data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Buildkite paginates via the HTTP `Link` header (RFC 5988), not a JSON body field -- this session did not independently confirm dlt's default rest_api paginator auto-detects Link-header pagination; verify against an organization with more than one page of results before relying on complete pulls.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_token` | required | Buildkite API access token. |
| `org_slug` | optional | Organization slug. Required for pipelines/builds resources; not needed for organizations. |
| `resources` | optional | Comma-separated list of resources to extract: organizations (account-level); pipelines, builds (require org_slug). Default: `organizations,pipelines` |

## Example
```yaml
type: dagster_component_templates.BuildkiteIngestionComponent
attributes:
  asset_name: buildkite_ingestion
  api_token: "${BUILDKITE_API_TOKEN}"
  resources: "organizations,pipelines"
```
