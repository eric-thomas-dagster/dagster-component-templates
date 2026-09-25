# Trello Ingestion

Ingest Trello boards/cards data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Trello has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Trello API key. Use ${TRELLO_API_KEY} for env vars. |
| `api_token` | required | Trello API token. Use ${TRELLO_API_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: boards, cards, lists, actions. Default: `boards,cards,lists` |

## Example

```yaml
type: dagster_component_templates.TrelloIngestionComponent
attributes:
  asset_name: trello_ingestion
  api_key: "${TRELLO_API_KEY}"
  api_token: "${TRELLO_API_TOKEN}"
  resources: "boards,cards,lists"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/trello`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
