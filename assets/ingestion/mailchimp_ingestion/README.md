# Mailchimp Ingestion

Ingest Mailchimp marketing data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Mailchimp has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Mailchimp API key. The datacenter suffix (e.g. 'us19') is parsed from it automatically. Use ${MAILCHIMP_API_KEY} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: lists, campaigns, automations, reports. Default: `lists,campaigns,automations,reports` |

## Example

```yaml
type: dagster_component_templates.MailchimpIngestionComponent
attributes:
  asset_name: mailchimp_ingestion
  api_key: "${MAILCHIMP_API_KEY}"
  resources: "lists,campaigns,automations,reports"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/mailchimp`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
