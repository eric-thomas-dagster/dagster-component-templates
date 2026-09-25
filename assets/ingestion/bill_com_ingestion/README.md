# Bill.com Ingestion

Ingest Bill.com AP/AR data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Bill.com uses session-based auth, not a static API key: this connector logs in on every run (username/password/organizationId/devKey) to mint a short-lived sessionId (expires after ~35 min idle). The production base_url (gateway.bill.com) was inferred from Bill.com's confirmed sandbox host (gateway.stage.bill.com) and was not independently confirmed; the customers/bills endpoint paths follow Bill.com's naming pattern but were not directly fetched from live docs. Verify against a live account before production use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `username` | required | Bill.com login username. |
| `password` | required | Bill.com login password. |
| `organization_id` | required | Bill.com organization ID. |
| `dev_key` | required | Bill.com developer key issued to your integration. |
| `resources` | optional | Comma-separated list of resources to extract: invoices, vendors, customers, bills. Default: `invoices,vendors` |

## Example
```yaml
type: dagster_component_templates.BillComIngestionComponent
attributes:
  asset_name: bill_com_ingestion
  username: "${BILLCOM_USERNAME}"
  password: "${BILLCOM_PASSWORD}"
  organization_id: "${BILLCOM_ORG_ID}"
  dev_key: "${BILLCOM_DEV_KEY}"
  resources: "invoices,vendors"
```
