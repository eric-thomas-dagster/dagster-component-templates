# Help Scout Ingestion

Ingest Help Scout support data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Help Scout has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Help Scout OAuth2 access token. Use ${HELPSCOUT_ACCESS_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: conversations, customers, mailboxes, users, tags. Default: `conversations,customers,mailboxes,users` |

## Example

```yaml
type: dagster_component_templates.HelpScoutIngestionComponent
attributes:
  asset_name: help_scout_ingestion
  access_token: "${HELPSCOUT_ACCESS_TOKEN}"
  resources: "conversations,customers,mailboxes,users"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/help_scout`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
