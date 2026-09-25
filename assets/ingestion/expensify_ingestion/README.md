# Expensify Ingestion

Ingest Expensify expense report data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Expensify's Integration Server API uses a non-standard request shape (credentials and job type inside a JSON body, not headers) -- the exact requestJobDescription fields here are a best-effort mapping from dltHub's reference; verify against Expensify's own Integration Server docs before production use.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `partner_user_id` | required | Expensify partnerUserID. Use ${EXPENSIFY_PARTNER_USER_ID} for env vars. |
| `partner_user_secret` | required | Expensify partnerUserSecret. Use ${EXPENSIFY_PARTNER_USER_SECRET} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: report_list, transaction_list, receipt_list, policy_list. Default: `report_list,transaction_list` |

## Example
```yaml
type: dagster_component_templates.ExpensifyIngestionComponent
attributes:
  asset_name: expensify_ingestion
  partner_user_id: "${EXPENSIFY_PARTNER_USER_ID}"
  partner_user_secret: "${EXPENSIFY_PARTNER_USER_SECRET}"
  resources: "report_list,transaction_list"
```
