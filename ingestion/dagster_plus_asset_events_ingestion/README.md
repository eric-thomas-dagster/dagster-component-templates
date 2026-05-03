# DagsterPlusAssetEventsIngestionComponent

Pull Dagster+ asset materialization / observation events via the GraphQL API.

## Authentication
Reads `Dagster-Cloud-User-Token` from env var `DAGSTER_PLUS_USER_TOKEN` (configurable).
Generate one in your Dagster+ deployment's Settings → Tokens.

## GraphQL endpoint
Pattern: `https://{org}.dagster.cloud/{deployment}/graphql`

## Validate the default query
The shipped `default_query` is a best-guess. The exact GraphQL field names may
differ in your Dagster+ schema. Inspect the GraphQL playground in your
deployment, then override via the `query` field if needed.

## See also
- [Schema](schema.json) · [Example](example.yaml)
