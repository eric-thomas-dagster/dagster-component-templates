# SurveyMonkey Ingestion

Ingest SurveyMonkey surveys and responses using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- SurveyMonkey has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | SurveyMonkey OAuth2 access token. Use ${SURVEYMONKEY_ACCESS_TOKEN} for env vars. |
| `survey_id` | optional | Survey ID to pull responses for (required if 'responses' is in resources). |
| `resources` | optional | Comma-separated list of resources to extract: surveys, responses (responses requires survey_id). Default: `surveys` |

## Example

```yaml
type: dagster_component_templates.SurveyMonkeyIngestionComponent
attributes:
  asset_name: surveymonkey_ingestion
  access_token: "${SURVEYMONKEY_ACCESS_TOKEN}"
  resources: "surveys"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/surveymonkey`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
